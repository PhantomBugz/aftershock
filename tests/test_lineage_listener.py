import json
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Any

import httpx
import pytest
from fastapi.testclient import TestClient

import lineage_listener
from compensating_action_engine import CompensatingActionEngine, RemediationGrant
from datahub_context import FixtureDataHubContext, MCPDataHubContext
from incident_processor import AftershockIncidentProcessor
from lineage_listener import (
    app,
    get_critical_authenticator,
    get_processor_session_factory,
)
from mcp_test_server import (
    DATASET_URN,
    DATA_JOB_URN,
    MODEL_URN,
    MCPCallRecorder,
    make_client_factory,
)


FIXED_NOW = datetime(2026, 8, 4, 15, 16, 17, tzinfo=timezone.utc)
SAFE_FAILURE = {"detail": "Aftershock incident processing unavailable"}
SAFE_AUTH_CONFIG_FAILURE = {
    "detail": "Aftershock critical authentication unavailable"
}
SAFE_UNAUTHORIZED = {"detail": "Unauthorized critical incident request"}
TEST_WEBHOOK_TOKEN = "test-aftershock-webhook-token"
GENERATED_DOCUMENT_URN = "urn:li:document:aftershock-generated-9942"
CONTROL_ENDPOINTS = (
    "https://controls.example/cancel-po",
    "https://controls.example/revert-price",
)
FIXTURE_ENDPOINTS = (
    "https://api.internal.example/remediate/cancel_po",
    "https://api.internal.example/remediate/revert_pricing",
)


def _control_grants(endpoints: tuple[str, str]) -> tuple[RemediationGrant, ...]:
    return (
        RemediationGrant(
            target_urn=DATA_JOB_URN,
            entity_type="DATA_JOB",
            business_action="ISSUE_PO",
            endpoint=endpoints[0],
        ),
        RemediationGrant(
            target_urn=MODEL_URN,
            entity_type="ML_MODEL",
            business_action="ADJUST_PRICE",
            endpoint=endpoints[1],
        ),
    )


def _property(qualified_name: str, value: str) -> dict[str, Any]:
    return {
        "structuredProperty": {
            "urn": f"urn:li:structuredProperty:{qualified_name}",
            "definition": {"qualifiedName": qualified_name},
        },
        "values": [{"stringValue": value}],
    }


def _entity(
    urn: str, entity_type: str, action: str, endpoint: str
) -> dict[str, Any]:
    return {
        "urn": urn,
        "type": entity_type,
        "structuredProperties": {
            "properties": [
                _property("aftershock.businessAction", action),
                _property("aftershock.remediationWebhook", endpoint),
            ]
        },
    }


def _critical_recorder() -> MCPCallRecorder:
    return MCPCallRecorder(
        lineage_pages={
            0: {
                "downstreams": {
                    "searchResults": [
                        {"entity": {"urn": DATA_JOB_URN, "type": "DATA_JOB"}},
                        {"entity": {"urn": MODEL_URN, "type": "ML_MODEL"}},
                    ],
                    "total": 2,
                    "offset": 0,
                    "returned": 2,
                    "hasMore": False,
                }
            }
        },
        entities_payload=[
            _entity(
                DATA_JOB_URN,
                "DATA_JOB",
                "ISSUE_PO",
                "https://controls.example/cancel-po",
            ),
            _entity(
                MODEL_URN,
                "ML_MODEL",
                "ADJUST_PRICE",
                "https://controls.example/revert-price",
            ),
        ],
        save_payload={"success": True, "urn": GENERATED_DOCUMENT_URN},
        search_payload={
            "start": 0,
            "count": 50,
            "total": 0,
            "searchResults": [],
        },
    )


def _post(
    payload: dict[str, object], *, authorization: str | None = None
) -> httpx.Response:
    headers = (
        {"Authorization": authorization} if authorization is not None else None
    )
    with TestClient(app) as client:
        return client.post("/webhook/datahub", json=payload, headers=headers)


def test_critical_envelope_runs_real_mcp_processor_and_returns_report(
    monkeypatch,
) -> None:
    monkeypatch.setenv("AFTERSHOCK_WEBHOOK_TOKEN", TEST_WEBHOOK_TOKEN)
    recorder = _critical_recorder()
    requests: list[httpx.Request] = []

    async def remediation(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        recorder.events.append(f"http:{request.url.path}")
        return httpx.Response(
            200,
            json={
                "receipt_version": 1,
                "status": "succeeded",
                "receipt_id": f"terminal{request.url.path.replace('/', '-')}",
            },
        )

    @asynccontextmanager
    async def session():
        context = MCPDataHubContext(client_factory=make_client_factory(recorder))
        async with httpx.AsyncClient(
            transport=httpx.MockTransport(remediation), timeout=10.0
        ) as http_client:
            yield AftershockIncidentProcessor(
                context,
                CompensatingActionEngine(
                    http_client=http_client,
                    allowed_controls=_control_grants(CONTROL_ENDPOINTS),
                ),
                clock=lambda: FIXED_NOW,
            )

    app.dependency_overrides[get_processor_session_factory] = lambda: session
    try:
        response = _post(
            {
                "incident_id": "  INC-9942  ",
                "dataset_urn": f"  {DATASET_URN}  ",
                "severity": " critical ",
            },
            authorization=f"Bearer {TEST_WEBHOOK_TOKEN}",
        )
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 200
    body = response.json()
    assert body["status"] == "completed"
    assert body["incident_id"] == "INC-9942"
    assert body["dataset_urn"] == DATASET_URN
    assert body["context_mode"] == "mcp"
    assert body["execution_mode"] == "DATAHUB MCP MODE"
    assert body["counts"] == {
        "succeeded": 2,
        "accepted": 0,
        "failed": 0,
        "skipped": 0,
        "outcome_unknown": 0,
    }
    assert [receipt["status"] for receipt in body["receipts"]] == [
        "succeeded",
        "succeeded",
    ]
    assert [receipt["external_receipt_id"] for receipt in body["receipts"]] == [
        "terminal-cancel-po",
        "terminal-revert-price",
    ]
    assert body["writeback"] == {
        "status": "succeeded",
        "document_urn": GENERATED_DOCUMENT_URN,
        "error": None,
    }
    assert recorder.events[:2] == ["get_lineage", "get_entities"]
    assert recorder.events[-1] == "save_document"
    assert set(recorder.events[2:-1]) == {
        "http:/cancel-po",
        "http:/revert-price",
        "search_documents",
    }
    assert [name for name, _ in recorder.calls] == [
        "get_lineage",
        "get_entities",
        "search_documents",
        "save_document",
    ]
    assert recorder.calls[0][1]["upstream"] is False
    assert recorder.calls[-1][1]["urn"] is None
    assert {request.url.path for request in requests} == {
        "/cancel-po",
        "/revert-price",
    }
    assert {json.loads(request.content)["incident_id"] for request in requests} == {
        "INC-9942"
    }


def test_noncritical_envelope_is_ignored_without_context_or_authentication(
    monkeypatch,
) -> None:
    monkeypatch.delenv("AFTERSHOCK_WEBHOOK_TOKEN", raising=False)
    entered = False
    authenticated = False

    @asynccontextmanager
    async def forbidden_session():
        nonlocal entered
        entered = True
        raise AssertionError("noncritical envelopes must not build DataHub context")
        yield  # pragma: no cover

    app.dependency_overrides[get_processor_session_factory] = (
        lambda: forbidden_session
    )
    app.dependency_overrides[get_critical_authenticator] = lambda: (
        lambda _: _mark_authenticated()
    )

    def _mark_authenticated() -> None:
        nonlocal authenticated
        authenticated = True

    try:
        response = _post(
            {
                "incident_id": " INC-9943 ",
                "dataset_urn": DATASET_URN,
                "severity": "warning",
            }
        )
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 200
    assert response.json() == {
        "status": "ignored",
        "incident_id": "INC-9943",
        "message": "no action required",
    }
    assert entered is False
    assert authenticated is False


@pytest.mark.parametrize("raw_allowlist", [None, "not-json PRIVATE-ALLOWLIST"])
def test_default_critical_session_rejects_missing_or_malformed_allowlist_before_context(
    monkeypatch, raw_allowlist: str | None
) -> None:
    monkeypatch.setenv("AFTERSHOCK_WEBHOOK_TOKEN", TEST_WEBHOOK_TOKEN)
    if raw_allowlist is None:
        monkeypatch.delenv("AFTERSHOCK_REMEDIATION_ALLOWLIST_JSON", raising=False)
    else:
        monkeypatch.setenv(
            "AFTERSHOCK_REMEDIATION_ALLOWLIST_JSON", raw_allowlist
        )
    context_built = False

    def build_context() -> FixtureDataHubContext:
        nonlocal context_built
        context_built = True
        return FixtureDataHubContext()

    monkeypatch.setattr(
        lineage_listener, "build_datahub_context_from_env", build_context
    )

    response = _post(
        {
            "incident_id": "INC-POLICY-CONFIG",
            "dataset_urn": DATASET_URN,
            "severity": "CRITICAL",
        },
        authorization=f"Bearer {TEST_WEBHOOK_TOKEN}",
    )

    assert response.status_code == 503
    assert response.json() == SAFE_FAILURE
    assert "PRIVATE-ALLOWLIST" not in response.text
    assert context_built is False


def test_mcp_failure_returns_fixed_secret_safe_service_error(monkeypatch) -> None:
    monkeypatch.setenv("AFTERSHOCK_WEBHOOK_TOKEN", TEST_WEBHOOK_TOKEN)
    recorder = _critical_recorder()
    recorder.fail_tool = "get_lineage"
    recorder.failure_message = "server leaked TOP-SECRET-MCP-TOKEN"

    @asynccontextmanager
    async def session():
        context = MCPDataHubContext(client_factory=make_client_factory(recorder))
        async with httpx.AsyncClient(
            transport=httpx.MockTransport(lambda _: httpx.Response(200))
        ) as http_client:
            yield AftershockIncidentProcessor(
                context,
                CompensatingActionEngine(
                    http_client=http_client,
                    allowed_controls=_control_grants(CONTROL_ENDPOINTS),
                ),
                clock=lambda: FIXED_NOW,
            )

    app.dependency_overrides[get_processor_session_factory] = lambda: session
    try:
        response = _post(
            {
                "incident_id": "INC-FAIL",
                "dataset_urn": DATASET_URN,
                "severity": "CRITICAL",
            },
            authorization=f"Bearer {TEST_WEBHOOK_TOKEN}",
        )
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 503
    assert response.json() == SAFE_FAILURE
    assert "TOP-SECRET-MCP-TOKEN" not in response.text
    assert recorder.events == ["get_lineage"]


@pytest.mark.parametrize("configured_token", [None, "   "])
def test_critical_auth_configuration_failure_is_fixed_and_opens_no_session(
    monkeypatch, configured_token: str | None
) -> None:
    if configured_token is None:
        monkeypatch.delenv("AFTERSHOCK_WEBHOOK_TOKEN", raising=False)
    else:
        monkeypatch.setenv("AFTERSHOCK_WEBHOOK_TOKEN", configured_token)
    entered = False

    @asynccontextmanager
    async def forbidden_session():
        nonlocal entered
        entered = True
        raise AssertionError("authentication must precede processor setup")
        yield  # pragma: no cover

    app.dependency_overrides[get_processor_session_factory] = (
        lambda: forbidden_session
    )
    try:
        response = _post(
            {
                "incident_id": "INC-AUTH-CONFIG",
                "dataset_urn": DATASET_URN,
                "severity": "CRITICAL",
            },
            authorization="Bearer request-secret-that-must-not-leak",
        )
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 503
    assert response.json() == SAFE_AUTH_CONFIG_FAILURE
    assert "request-secret-that-must-not-leak" not in response.text
    assert entered is False


@pytest.mark.parametrize(
    "authorization",
    [
        None,
        "Bearer wrong-secret-that-must-not-leak",
        "Basic test-aftershock-webhook-token",
        "Bearer",
        "Bearer test-aftershock-webhook-token extra",
    ],
)
def test_missing_or_invalid_critical_auth_is_fixed_and_opens_no_session(
    monkeypatch, authorization: str | None
) -> None:
    monkeypatch.setenv("AFTERSHOCK_WEBHOOK_TOKEN", TEST_WEBHOOK_TOKEN)
    entered = False

    @asynccontextmanager
    async def forbidden_session():
        nonlocal entered
        entered = True
        raise AssertionError("authentication must precede processor setup")
        yield  # pragma: no cover

    app.dependency_overrides[get_processor_session_factory] = (
        lambda: forbidden_session
    )
    try:
        response = _post(
            {
                "incident_id": "INC-UNAUTHORIZED",
                "dataset_urn": DATASET_URN,
                "severity": "CRITICAL",
            },
            authorization=authorization,
        )
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 401
    assert response.json() == SAFE_UNAUTHORIZED
    assert "wrong-secret-that-must-not-leak" not in response.text
    assert TEST_WEBHOOK_TOKEN not in response.text
    assert entered is False


def test_critical_fixture_response_is_unmistakably_labeled(monkeypatch) -> None:
    monkeypatch.setenv("AFTERSHOCK_WEBHOOK_TOKEN", TEST_WEBHOOK_TOKEN)
    context = FixtureDataHubContext()

    @asynccontextmanager
    async def session():
        async with httpx.AsyncClient(
            transport=httpx.MockTransport(
                lambda request: httpx.Response(
                    200,
                    json={
                        "receipt_version": 1,
                        "status": "succeeded",
                        "receipt_id": f"fixture{request.url.path.replace('/', '-')}",
                    },
                )
            )
        ) as http_client:
            yield AftershockIncidentProcessor(
                context,
                CompensatingActionEngine(
                    http_client=http_client,
                    allowed_controls=_control_grants(FIXTURE_ENDPOINTS),
                ),
                clock=lambda: FIXED_NOW,
            )

    app.dependency_overrides[get_processor_session_factory] = lambda: session
    try:
        response = _post(
            {
                "incident_id": "INC-FIXTURE",
                "dataset_urn": DATASET_URN,
                "severity": "CRITICAL",
            },
            authorization=f"Bearer {TEST_WEBHOOK_TOKEN}",
        )
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 200
    assert response.json()["context_mode"] == "fixture"
    assert response.json()["execution_mode"] == "OFFLINE FIXTURE MODE"


@pytest.mark.parametrize(
    "payload",
    [
        {"incident_id": " ", "dataset_urn": DATASET_URN, "severity": "CRITICAL"},
        {"incident_id": "I" * 129, "dataset_urn": DATASET_URN, "severity": "CRITICAL"},
        {"incident_id": "INC-1", "dataset_urn": "urn:li:dataJob:not-a-dataset", "severity": "CRITICAL"},
        {"incident_id": "INC-1", "dataset_urn": "urn:li:dataset:", "severity": "CRITICAL"},
        {"incident_id": "INC-1", "dataset_urn": DATASET_URN, "severity": " "},
        {"incident_id": "INC-1", "dataset_urn": DATASET_URN, "severity": "X" * 33},
    ],
)
def test_normalized_incident_envelope_rejects_invalid_input(
    payload: dict[str, object],
) -> None:
    entered = False

    @asynccontextmanager
    async def forbidden_session():
        nonlocal entered
        entered = True
        raise AssertionError("invalid envelopes must not enter processor session")
        yield  # pragma: no cover

    app.dependency_overrides[get_processor_session_factory] = (
        lambda: forbidden_session
    )
    try:
        response = _post(payload)
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 422
    assert entered is False
