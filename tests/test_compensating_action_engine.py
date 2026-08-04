import asyncio
import json
import logging
import re
from concurrent.futures import ThreadPoolExecutor

import httpx
import pytest

from compensating_action_engine import (
    CompensatingActionEngine,
    RemediationConfigurationError,
    _HTTPX_REQUEST_URL_FILTER,
    _install_httpx_request_log_filter,
    build_remediation_allowlist_from_env,
    parse_remediation_allowlist_json,
)
from remediation_models import ActionableTarget, RemediationReceipt


TERMINAL_SUCCESS = {
    "receipt_version": 1,
    "status": "succeeded",
    "receipt_id": "receipt-001",
}


def _target(
    urn_suffix: str,
    *,
    action: str | None = "ISSUE_PO",
    webhook: str | None = "https://remediation.example/cancel",
) -> ActionableTarget:
    return ActionableTarget(
        urn=f"urn:li:dataJob:{urn_suffix}",
        entity_type="DATA_JOB",
        business_action=action,
        remediation_webhook=webhook,
    )


def _engine(
    client: httpx.AsyncClient,
    *allowed_endpoints: str,
    max_concurrency: int = 8,
    workflow_timeout_seconds: float = 30.0,
) -> CompensatingActionEngine:
    return CompensatingActionEngine(
        http_client=client,
        allowed_endpoints=allowed_endpoints,
        max_concurrency=max_concurrency,
        workflow_timeout_seconds=workflow_timeout_seconds,
    )


def _run_one(
    response_or_error,
    *,
    webhook: str = "https://remediation.example/cancel",
) -> RemediationReceipt:
    async def handler(request: httpx.Request) -> httpx.Response:
        if isinstance(response_or_error, BaseException):
            if isinstance(response_or_error, httpx.RequestError):
                response_or_error.request = request
            raise response_or_error
        return response_or_error

    async def scenario() -> RemediationReceipt:
        async with httpx.AsyncClient(
            transport=httpx.MockTransport(handler), timeout=10.0
        ) as client:
            return await _engine(client, webhook).execute_rollback(
                _target("one", webhook=webhook), "INC-ONE"
            )

    return asyncio.run(scenario())


def test_terminal_v1_success_is_the_only_response_classified_succeeded() -> None:
    receipt = _run_one(httpx.Response(200, json=TERMINAL_SUCCESS))

    assert receipt == RemediationReceipt(
        incident_id="INC-ONE",
        target_urn="urn:li:dataJob:one",
        entity_type="DATA_JOB",
        business_action="ISSUE_PO",
        endpoint="https://remediation.example/cancel",
        status="succeeded",
        http_status=200,
        external_receipt_id="receipt-001",
        error=None,
    )


@pytest.mark.parametrize("contract_status", ["accepted", "pending"])
def test_valid_v1_nonterminal_contract_is_accepted(contract_status: str) -> None:
    receipt = _run_one(
        httpx.Response(
            200,
            json={
                "receipt_version": 1,
                "status": contract_status,
                "receipt_id": "queue-42",
            },
        )
    )

    assert receipt.status == "accepted"
    assert receipt.external_receipt_id == "queue-42"
    assert receipt.error is None


def test_valid_v1_failed_contract_is_terminal_failed_without_body_leak() -> None:
    receipt = _run_one(
        httpx.Response(
            200,
            json={
                "receipt_version": 1,
                "status": "failed",
                "receipt_id": "failure-9",
                "detail": "PRIVATE downstream detail",
            },
        )
    )

    assert receipt.status == "failed"
    assert receipt.external_receipt_id == "failure-9"
    assert receipt.error == "remediation endpoint reported terminal failure"
    assert "PRIVATE" not in json.dumps(receipt.to_dict())


@pytest.mark.parametrize(
    ("response", "expected_receipt_id"),
    [
        (httpx.Response(202, text="not-json PRIVATE"), None),
        (httpx.Response(200, json={"accepted": True}), None),
        (
            httpx.Response(
                200, json={"accepted": True, "receipt_id": "legacy-accepted-3"}
            ),
            "legacy-accepted-3",
        ),
    ],
)
def test_explicit_or_http_202_acceptance_is_nonterminal(
    response: httpx.Response, expected_receipt_id: str | None
) -> None:
    receipt = _run_one(response)

    assert receipt.status == "accepted"
    assert receipt.external_receipt_id == expected_receipt_id
    assert receipt.error is None


@pytest.mark.parametrize(
    "response",
    [
        httpx.Response(200),
        httpx.Response(200, text="not-json PRIVATE"),
        httpx.Response(200, json={"status": "succeeded", "receipt_id": "old"}),
        httpx.Response(
            200,
            json={"receipt_version": 2, "status": "succeeded", "receipt_id": "x"},
        ),
        httpx.Response(
            200,
            json={"receipt_version": 1, "status": "succeeded", "receipt_id": " "},
        ),
        httpx.Response(
            200,
            json={
                "receipt_version": 1,
                "status": "succeeded",
                "receipt_id": "x" * 257,
            },
        ),
        httpx.Response(
            200,
            json={"receipt_version": 1, "status": "mystery", "receipt_id": "x"},
        ),
    ],
)
def test_ambiguous_successful_http_response_is_outcome_unknown(
    response: httpx.Response,
) -> None:
    receipt = _run_one(response)

    assert receipt.status == "outcome_unknown"
    assert receipt.external_receipt_id is None
    assert receipt.error == "remediation endpoint returned no valid terminal receipt"
    assert "PRIVATE" not in json.dumps(receipt.to_dict())


def test_unexpected_json_decoder_failure_is_outcome_unknown() -> None:
    class BrokenJSONResponse(httpx.Response):
        def json(self, **_):
            raise RuntimeError("PRIVATE decoder detail")

    receipt = _run_one(BrokenJSONResponse(200))

    assert receipt.status == "outcome_unknown"
    assert receipt.error == "remediation endpoint returned no valid terminal receipt"
    assert "PRIVATE" not in json.dumps(receipt.to_dict())


@pytest.mark.parametrize(
    "error",
    [
        httpx.ReadTimeout("PRIVATE timeout"),
        httpx.ConnectError("PRIVATE network"),
        RuntimeError("PRIVATE transport bug"),
    ],
)
def test_error_after_dispatch_is_outcome_unknown_and_secret_safe(error) -> None:
    receipt = _run_one(error)

    assert receipt.status == "outcome_unknown"
    assert receipt.http_status is None
    assert receipt.external_receipt_id is None
    assert receipt.error == "remediation outcome unknown after dispatch"
    assert "PRIVATE" not in json.dumps(receipt.to_dict())


def test_request_build_failure_is_failed_before_dispatch() -> None:
    requests: list[httpx.Request] = []
    endpoint = "https://remediation.example/cancel"

    async def scenario() -> RemediationReceipt:
        async with httpx.AsyncClient(
            transport=httpx.MockTransport(
                lambda request: requests.append(request)
                or httpx.Response(200, json=TERMINAL_SUCCESS)
            )
        ) as client:
            return await _engine(client, endpoint).execute_rollback(
                _target("build-failure", webhook=endpoint), "\ud800"
            )

    receipt = asyncio.run(scenario())

    assert requests == []
    assert receipt.status == "failed"
    assert receipt.http_status is None
    assert receipt.external_receipt_id is None
    assert receipt.error == "remediation request could not be prepared"


@pytest.mark.parametrize("http_status", [400, 404, 429])
def test_clear_http_client_rejection_is_failed(http_status: int) -> None:
    receipt = _run_one(
        httpx.Response(http_status, text="PRIVATE response body")
    )

    assert receipt.status == "failed"
    assert receipt.http_status == http_status
    assert receipt.error == f"remediation endpoint returned HTTP {http_status}"
    assert "PRIVATE" not in json.dumps(receipt.to_dict())


@pytest.mark.parametrize("http_status", [408, 500, 502, 503, 504])
def test_ambiguous_http_error_without_terminal_receipt_is_outcome_unknown(
    http_status: int,
) -> None:
    receipt = _run_one(
        httpx.Response(http_status, text="PRIVATE response body")
    )

    assert receipt.status == "outcome_unknown"
    assert receipt.http_status == http_status
    assert receipt.external_receipt_id is None
    assert receipt.error == "remediation outcome unknown after dispatch"
    assert "PRIVATE" not in json.dumps(receipt.to_dict())


@pytest.mark.parametrize(
    ("http_status", "contract_status", "expected_status", "expected_error"),
    [
        (408, "succeeded", "succeeded", None),
        (500, "failed", "failed", "remediation endpoint reported terminal failure"),
        (503, "succeeded", "succeeded", None),
    ],
)
def test_ambiguous_http_error_honors_valid_terminal_receipt(
    http_status: int,
    contract_status: str,
    expected_status: str,
    expected_error: str | None,
) -> None:
    receipt = _run_one(
        httpx.Response(
            http_status,
            json={
                "receipt_version": 1,
                "status": contract_status,
                "receipt_id": "receipt-ambiguous-1",
            },
        )
    )

    assert receipt.status == expected_status
    assert receipt.http_status == http_status
    assert receipt.external_receipt_id == "receipt-ambiguous-1"
    assert receipt.error == expected_error


def test_redirect_is_not_followed_and_is_reported_failed() -> None:
    requests: list[httpx.Request] = []

    async def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(
            302, headers={"Location": "https://unlisted.example/steal?secret=1"}
        )

    async def scenario() -> RemediationReceipt:
        async with httpx.AsyncClient(
            transport=httpx.MockTransport(handler), follow_redirects=True
        ) as client:
            return await _engine(
                client, "https://remediation.example/cancel"
            ).execute_rollback(_target("redirect"), "INC-REDIRECT")

    receipt = asyncio.run(scenario())

    assert len(requests) == 1
    assert receipt.status == "failed"
    assert receipt.http_status == 302
    assert receipt.error == "remediation endpoint returned HTTP 302"


def test_allowlist_parser_accepts_exact_governed_urls(monkeypatch) -> None:
    raw = json.dumps(
        [
            "https://controls.example/cancel?tenant=one",
            "http://127.0.0.1:8080/revert",
            "http://localhost:9000/pause",
            "http://[::1]:7000/control",
        ]
    )
    expected = frozenset(json.loads(raw))

    assert parse_remediation_allowlist_json(raw) == expected
    monkeypatch.setenv("AFTERSHOCK_REMEDIATION_ALLOWLIST_JSON", raw)
    assert build_remediation_allowlist_from_env() == expected


@pytest.mark.parametrize(
    "raw",
    [
        None,
        "",
        "not-json PRIVATE",
        "{}",
        "[]",
        '["https://controls.example/x", 7]',
        '[" https://controls.example/x"]',
        '["ftp://controls.example/x"]',
        '["https://user:pass@controls.example/x"]',
        '["https://controls.example/x#fragment"]',
        '["https://controls.example/x#"]',
        '["http://controls.example/x"]',
    ],
)
def test_allowlist_parser_rejects_missing_malformed_or_unsafe_config(
    raw: str | None,
) -> None:
    with pytest.raises(RemediationConfigurationError) as error:
        parse_remediation_allowlist_json(raw)

    assert str(error.value) == "invalid remediation endpoint allowlist configuration"
    assert "PRIVATE" not in str(error.value)


def test_engine_rejects_nonstring_allowlist_entries_with_fixed_error() -> None:
    with pytest.raises(RemediationConfigurationError) as error:
        CompensatingActionEngine(
            allowed_endpoints=[["https://controls.example/x"]]  # type: ignore[list-item]
        )

    assert str(error.value) == "invalid remediation endpoint allowlist configuration"


@pytest.mark.parametrize(
    "webhook",
    [
        "https://controls.example/control/child",
        "https://controls.example/control?tenant=two",
        "https://controls.example/control?tenant=one&extra=1",
        "https://user:password@controls.example/control?tenant=one",
        "https://controls.example/control?tenant=one#fragment",
        "http://controls.example/control?tenant=one",
    ],
)
def test_unlisted_or_unsafe_endpoint_never_dispatches(webhook: str) -> None:
    requests: list[httpx.Request] = []
    allowed = "https://controls.example/control?tenant=one"

    async def scenario() -> RemediationReceipt:
        async with httpx.AsyncClient(
            transport=httpx.MockTransport(
                lambda request: requests.append(request)
                or httpx.Response(200, json=TERMINAL_SUCCESS)
            )
        ) as client:
            return await _engine(client, allowed).execute_rollback(
                _target("denied", webhook=webhook), "INC-DENIED"
            )

    receipt = asyncio.run(scenario())

    assert requests == []
    assert receipt.status in {"failed", "skipped"}
    assert receipt.external_receipt_id is None
    serialized = json.dumps(receipt.to_dict())
    for secret in ("user", "password", "fragment", "extra"):
        assert secret not in serialized


def test_no_allowlist_denies_outbound_call() -> None:
    requests: list[httpx.Request] = []

    async def scenario() -> RemediationReceipt:
        async with httpx.AsyncClient(
            transport=httpx.MockTransport(
                lambda request: requests.append(request)
                or httpx.Response(200, json=TERMINAL_SUCCESS)
            )
        ) as client:
            return await CompensatingActionEngine(
                http_client=client
            ).execute_rollback(_target("denied"), "INC-NO-POLICY")

    receipt = asyncio.run(scenario())

    assert requests == []
    assert receipt.status == "skipped"
    assert receipt.error == "remediation endpoint is not allowlisted"


def test_exact_allowlisted_url_dispatches_unchanged_and_is_sanitized_in_receipt(
    caplog,
) -> None:
    endpoint = "https://controls.example/control?tenant=one&signature=PRIVATE"
    requests: list[httpx.Request] = []

    async def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(200, json=TERMINAL_SUCCESS)

    async def scenario() -> RemediationReceipt:
        async with httpx.AsyncClient(
            transport=httpx.MockTransport(handler)
        ) as client:
            return await _engine(client, endpoint).execute_rollback(
                _target("allowed", webhook=endpoint), "INC-ALLOWED"
            )

    caplog.set_level(logging.INFO, logger="httpx")
    receipt = asyncio.run(scenario())

    assert len(requests) == 1
    assert str(requests[0].url) == endpoint
    assert receipt.status == "succeeded"
    assert receipt.endpoint == "https://controls.example/control"
    combined = caplog.text + json.dumps(receipt.to_dict())
    assert "signature" not in combined
    assert "PRIVATE" not in combined


def test_missing_playbook_skips_before_endpoint_policy() -> None:
    async def scenario() -> list[RemediationReceipt]:
        async with httpx.AsyncClient(
            transport=httpx.MockTransport(
                lambda _: (_ for _ in ()).throw(AssertionError("must not send"))
            )
        ) as client:
            engine = _engine(client, "https://x.example/x")
            return [
                await engine.execute_rollback(
                    _target("no-webhook", webhook=None), "INC-MISSING"
                ),
                await engine.execute_rollback(
                    _target("no-action", action=None, webhook="https://x.example/x"),
                    "INC-MISSING",
                ),
            ]

    receipts = asyncio.run(scenario())
    assert [receipt.status for receipt in receipts] == ["skipped", "skipped"]
    assert [receipt.error for receipt in receipts] == [
        "missing remediation webhook",
        "missing business action",
    ]


def test_process_blast_radius_uses_bounded_workers_and_preserves_order() -> None:
    targets = [
        _target(str(index), webhook=f"https://controls.example/{index}")
        for index in range(5)
    ]
    allowed = [target.remediation_webhook for target in targets]

    async def scenario() -> tuple[list[RemediationReceipt], int, list[str]]:
        release = asyncio.Event()
        two_started = asyncio.Event()
        active = 0
        maximum_active = 0
        started: list[str] = []

        async def handler(request: httpx.Request) -> httpx.Response:
            nonlocal active, maximum_active
            active += 1
            maximum_active = max(maximum_active, active)
            started.append(request.url.path)
            if len(started) == 2:
                two_started.set()
            await release.wait()
            active -= 1
            return httpx.Response(
                200,
                json={
                    "receipt_version": 1,
                    "status": "succeeded",
                    "receipt_id": f"receipt-{request.url.path[1:]}",
                },
            )

        async with httpx.AsyncClient(
            transport=httpx.MockTransport(handler)
        ) as client:
            engine = _engine(client, *allowed, max_concurrency=2)
            task = asyncio.create_task(
                engine.process_blast_radius(targets, "INC-BOUNDED")
            )
            await two_started.wait()
            await asyncio.sleep(0)
            assert started == ["/0", "/1"]
            release.set()
            receipts = await task
        return receipts, maximum_active, started

    receipts, maximum_active, started = asyncio.run(scenario())

    assert maximum_active == 2
    assert started == ["/0", "/1", "/2", "/3", "/4"]
    assert [receipt.target_urn for receipt in receipts] == [
        f"urn:li:dataJob:{index}" for index in range(5)
    ]
    assert [receipt.external_receipt_id for receipt in receipts] == [
        f"receipt-{index}" for index in range(5)
    ]


def test_workflow_deadline_marks_inflight_unknown_and_never_started_skipped() -> None:
    targets = [
        _target(str(index), webhook=f"https://controls.example/{index}")
        for index in range(3)
    ]
    handler_cancelled = False

    async def scenario() -> list[RemediationReceipt]:
        async def handler(_: httpx.Request) -> httpx.Response:
            nonlocal handler_cancelled
            try:
                await asyncio.Event().wait()
            finally:
                handler_cancelled = True
            raise AssertionError("unreachable")

        async with httpx.AsyncClient(
            transport=httpx.MockTransport(handler)
        ) as client:
            engine = _engine(
                client,
                *(target.remediation_webhook for target in targets),
                max_concurrency=1,
                workflow_timeout_seconds=0.01,
            )
            return await engine.process_blast_radius(targets, "INC-DEADLINE")

    receipts = asyncio.run(scenario())

    assert handler_cancelled is True
    assert [receipt.status for receipt in receipts] == [
        "outcome_unknown",
        "skipped",
        "skipped",
    ]
    assert [receipt.error for receipt in receipts] == [
        "workflow deadline expired after dispatch",
        "workflow deadline expired before dispatch",
        "workflow deadline expired before dispatch",
    ]


def test_external_cancellation_propagates_and_cleans_up_workers() -> None:
    cancelled = asyncio.Event()
    started = asyncio.Event()

    async def scenario() -> None:
        async def handler(_: httpx.Request) -> httpx.Response:
            started.set()
            try:
                await asyncio.Event().wait()
            finally:
                cancelled.set()
            raise AssertionError("unreachable")

        endpoint = "https://controls.example/hang"
        async with httpx.AsyncClient(
            transport=httpx.MockTransport(handler)
        ) as client:
            task = asyncio.create_task(
                _engine(client, endpoint).process_blast_radius(
                    [_target("hang", webhook=endpoint)], "INC-CANCEL"
                )
            )
            await started.wait()
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task
            await cancelled.wait()

    asyncio.run(scenario())


@pytest.mark.parametrize(
    ("max_concurrency", "timeout"),
    [(0, 1.0), (-1, 1.0), (True, 1.0), (1, 0), (1, -1), (1, float("inf"))],
)
def test_invalid_execution_bounds_are_rejected(
    max_concurrency, timeout
) -> None:
    with pytest.raises(ValueError):
        CompensatingActionEngine(
            allowed_endpoints=(),
            max_concurrency=max_concurrency,
            workflow_timeout_seconds=timeout,
        )


def test_engine_closes_only_the_client_it_owns() -> None:
    async def scenario() -> tuple[bool, bool]:
        owned_engine = CompensatingActionEngine(allowed_endpoints=())
        await owned_engine.aclose()
        owned_closed = owned_engine.http_client.is_closed

        external_client = httpx.AsyncClient(
            transport=httpx.MockTransport(lambda _: httpx.Response(200))
        )
        external_engine = CompensatingActionEngine(
            http_client=external_client, allowed_endpoints=()
        )
        await external_engine.aclose()
        external_closed_by_engine = external_client.is_closed
        await external_client.aclose()
        return owned_closed, external_closed_by_engine

    assert asyncio.run(scenario()) == (True, False)


def test_downstream_idempotency_key_is_stable_and_scoped_without_deduping() -> None:
    requests: list[httpx.Request] = []
    endpoint = "https://remediation.example/cancel"

    async def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(200, json=TERMINAL_SUCCESS)

    async def scenario() -> None:
        async with httpx.AsyncClient(
            transport=httpx.MockTransport(handler), timeout=10.0
        ) as client:
            engine = _engine(client, endpoint)
            first_target = _target("stable", action="PAUSE_JOB")
            await engine.execute_rollback(first_target, "INC-RETRY")
            await engine.execute_rollback(first_target, "INC-RETRY")
            await engine.execute_rollback(first_target, "INC-DIFFERENT")
            await engine.execute_rollback(
                _target("different-target", action="PAUSE_JOB"), "INC-RETRY"
            )

    asyncio.run(scenario())

    assert len(requests) == 4
    keys = [request.headers["Idempotency-Key"] for request in requests]
    assert keys[0] == keys[1]
    assert keys[0] != keys[2]
    assert keys[0] != keys[3]
    assert len(set(keys)) == 3
    assert all(re.fullmatch(r"aftershock-[0-9a-f]{64}", key) for key in keys)
    assert requests[0].url == requests[1].url
    assert requests[0].content == requests[1].content


def test_execute_rollback_does_not_swallow_base_exceptions() -> None:
    class StopSignal(BaseException):
        pass

    async def handler(_: httpx.Request) -> httpx.Response:
        raise StopSignal()

    async def scenario() -> None:
        endpoint = "https://remediation.example/stop"
        async with httpx.AsyncClient(
            transport=httpx.MockTransport(handler), timeout=10.0
        ) as client:
            with pytest.raises(StopSignal):
                await _engine(client, endpoint).execute_rollback(
                    _target("cancelled", webhook=endpoint), "INC-CANCEL"
                )

    asyncio.run(scenario())


def test_httpx_request_log_filter_installation_is_thread_safe_and_idempotent() -> None:
    with ThreadPoolExecutor(max_workers=8) as executor:
        list(executor.map(lambda _: _install_httpx_request_log_filter(), range(64)))

    assert sum(
        installed_filter is _HTTPX_REQUEST_URL_FILTER
        for installed_filter in logging.getLogger("httpx").filters
    ) == 1
