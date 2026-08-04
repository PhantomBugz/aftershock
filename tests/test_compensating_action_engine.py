import asyncio
import json
import logging
from concurrent.futures import ThreadPoolExecutor

import httpx
import pytest

from compensating_action_engine import (
    CompensatingActionEngine,
    _HTTPX_REQUEST_URL_FILTER,
    _install_httpx_request_log_filter,
)
from remediation_models import ActionableTarget, RemediationReceipt


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


def test_processes_success_failure_and_skips_as_structured_receipts(caplog) -> None:
    requests: list[httpx.Request] = []

    async def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        if request.url.path == "/unavailable":
            return httpx.Response(
                503,
                text="PRIVATE response body that must not leak",
            )
        return httpx.Response(200, json={"status": "accepted"})

    targets = [
        _target(
            "success",
            webhook=(
                "https://api-user:api-password@remediation.example:8443/cancel"
                "?api_key=private-query#private-fragment"
            ),
        ),
        _target(
            "http-failure",
            action="PAUSE_JOB",
            webhook="https://remediation.example/unavailable?token=private-token",
        ),
        _target("no-webhook", webhook=None),
        _target(
            "no-action",
            action=None,
            webhook="https://remediation.example/not-called?token=private-skip",
        ),
    ]

    async def scenario() -> list[RemediationReceipt]:
        async with httpx.AsyncClient(
            transport=httpx.MockTransport(handler), timeout=10.0
        ) as client:
            engine = CompensatingActionEngine(http_client=client)
            return await engine.process_blast_radius(targets, "INC-9942")

    caplog.set_level(logging.INFO, logger="Aftershock-Engine")
    caplog.set_level(logging.INFO, logger="httpx")
    receipts = asyncio.run(scenario())

    assert [receipt.status for receipt in receipts] == [
        "succeeded",
        "failed",
        "skipped",
        "skipped",
    ]
    assert receipts[0] == RemediationReceipt(
        incident_id="INC-9942",
        target_urn="urn:li:dataJob:success",
        entity_type="DATA_JOB",
        business_action="ISSUE_PO",
        endpoint="https://remediation.example:8443/cancel",
        status="succeeded",
        http_status=200,
        error=None,
    )
    assert receipts[1].http_status == 503
    assert receipts[1].error == "remediation endpoint returned HTTP 503"
    assert receipts[1].endpoint == "https://remediation.example/unavailable"
    assert receipts[2].http_status is None
    assert receipts[2].error == "missing remediation webhook"
    assert receipts[2].endpoint is None
    assert receipts[3].http_status is None
    assert receipts[3].error == "missing business action"
    assert receipts[3].endpoint == "https://remediation.example/not-called"

    assert len(requests) == 2
    success_request = next(
        request for request in requests if request.url.path == "/cancel"
    )
    assert success_request.url.username == "api-user"
    assert success_request.url.password == "api-password"
    assert success_request.url.query == b"api_key=private-query"
    assert json.loads(success_request.content) == {
        "incident_id": "INC-9942",
        "target_urn": "urn:li:dataJob:success",
        "action": "REVERT_STATE",
        "business_action": "ISSUE_PO",
    }
    failed_request = next(
        request for request in requests if request.url.path == "/unavailable"
    )
    assert json.loads(failed_request.content)["business_action"] == "PAUSE_JOB"

    serialized = json.dumps([receipt.to_dict() for receipt in receipts])
    combined_output = serialized + caplog.text
    assert (
        'HTTP Request: POST https://remediation.example:8443/cancel '
        '"HTTP/1.1 200 OK"'
    ) in caplog.text
    assert (
        'HTTP Request: POST https://remediation.example/unavailable '
        '"HTTP/1.1 503 Service Unavailable"'
    ) in caplog.text
    for secret in (
        "api-user",
        "api-password",
        "api_key",
        "private-query",
        "private-fragment",
        "private-token",
        "private-skip",
        "PRIVATE response body",
    ):
        assert secret not in combined_output


def test_network_and_unexpected_errors_are_isolated_and_order_is_preserved() -> None:
    async def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/network":
            raise httpx.ConnectError(
                "private network diagnostic", request=request
            )
        if request.url.path == "/unexpected":
            raise RuntimeError("private handler detail")
        if request.url.path == "/slow-success":
            await asyncio.sleep(0.01)
        return httpx.Response(200, json={"status": "accepted"})

    targets = [
        _target("slow", webhook="https://remediation.example/slow-success"),
        _target("network", webhook="https://remediation.example/network"),
        _target("unexpected", webhook="https://remediation.example/unexpected"),
        _target("fast", webhook="https://remediation.example/fast-success"),
    ]

    async def scenario() -> list[RemediationReceipt]:
        async with httpx.AsyncClient(
            transport=httpx.MockTransport(handler), timeout=10.0
        ) as client:
            engine = CompensatingActionEngine(http_client=client)
            return await engine.process_blast_radius(targets, "INC-ORDER")

    receipts = asyncio.run(scenario())

    assert [receipt.target_urn for receipt in receipts] == [
        "urn:li:dataJob:slow",
        "urn:li:dataJob:network",
        "urn:li:dataJob:unexpected",
        "urn:li:dataJob:fast",
    ]
    assert [receipt.status for receipt in receipts] == [
        "succeeded",
        "failed",
        "failed",
        "succeeded",
    ]
    assert receipts[1].http_status is None
    assert receipts[1].error == "remediation request failed"
    assert receipts[2].http_status is None
    assert receipts[2].error == "unexpected remediation error"
    serialized = json.dumps([receipt.to_dict() for receipt in receipts])
    assert "private network diagnostic" not in serialized
    assert "private handler detail" not in serialized


@pytest.mark.parametrize(
    "webhook",
    [
        "https://user:password@remediation.example:not-a-port/control?secret=1#x",
        "not an absolute URL?secret=2#x",
        "ftp://user:password@remediation.example/control?secret=3#x",
    ],
)
def test_invalid_endpoint_fails_without_leaking_or_sending(webhook: str) -> None:
    requests: list[httpx.Request] = []

    async def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(200)

    async def scenario() -> RemediationReceipt:
        async with httpx.AsyncClient(
            transport=httpx.MockTransport(handler), timeout=10.0
        ) as client:
            engine = CompensatingActionEngine(http_client=client)
            return await engine.execute_rollback(
                _target("invalid", webhook=webhook), "INC-INVALID"
            )

    receipt = asyncio.run(scenario())

    assert receipt.status == "failed"
    assert receipt.endpoint is None
    assert receipt.http_status is None
    assert receipt.error == "invalid remediation endpoint"
    assert requests == []
    serialized = json.dumps(receipt.to_dict())
    assert "user" not in serialized
    assert "password" not in serialized
    assert "secret" not in serialized


def test_execute_rollback_does_not_swallow_base_exceptions() -> None:
    class StopSignal(BaseException):
        pass

    async def handler(_: httpx.Request) -> httpx.Response:
        raise StopSignal()

    async def scenario() -> None:
        async with httpx.AsyncClient(
            transport=httpx.MockTransport(handler), timeout=10.0
        ) as client:
            engine = CompensatingActionEngine(http_client=client)
            with pytest.raises(StopSignal):
                await engine.execute_rollback(
                    _target("cancelled", webhook="https://remediation.example/stop"),
                    "INC-CANCEL",
                )

    asyncio.run(scenario())


def test_engine_closes_only_the_client_it_owns() -> None:
    async def scenario() -> tuple[bool, bool]:
        owned_engine = CompensatingActionEngine()
        await owned_engine.aclose()
        owned_closed = owned_engine.http_client.is_closed

        external_client = httpx.AsyncClient(
            transport=httpx.MockTransport(lambda _: httpx.Response(200))
        )
        external_engine = CompensatingActionEngine(http_client=external_client)
        await external_engine.aclose()
        external_closed_by_engine = external_client.is_closed
        await external_client.aclose()
        return owned_closed, external_closed_by_engine

    assert asyncio.run(scenario()) == (True, False)


def test_httpx_request_log_filter_installation_is_thread_safe_and_idempotent() -> None:
    with ThreadPoolExecutor(max_workers=8) as executor:
        list(executor.map(lambda _: _install_httpx_request_log_filter(), range(64)))

    assert sum(
        installed_filter is _HTTPX_REQUEST_URL_FILTER
        for installed_filter in logging.getLogger("httpx").filters
    ) == 1
