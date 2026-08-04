import json
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Any

import httpx
import pytest
from fastapi.testclient import TestClient

from compensating_action_engine import CompensatingActionEngine
from datahub_context import MCPDataHubContext
from incident_processor import AftershockIncidentProcessor, _incident_document_urn
from lineage_listener import app, get_processor_session_factory
from mcp_test_server import (
    DATASET_URN,
    DATA_JOB_URN,
    MODEL_URN,
    MCPCallRecorder,
    make_client_factory,
)


FIXED_NOW = datetime(2026, 8, 4, 15, 16, 17, tzinfo=timezone.utc)
SAFE_FAILURE = {"detail": "Aftershock incident processing unavailable"}


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
        save_payload={"success": True, "urn": "ignored-by-echo"},
        echo_saved_urn=True,
    )


def _post(payload: dict[str, object]) -> httpx.Response:
    with TestClient(app) as client:
        return client.post("/webhook/datahub", json=payload)


def test_critical_envelope_runs_real_mcp_processor_and_returns_report() -> None:
    recorder = _critical_recorder()
    requests: list[httpx.Request] = []

    async def remediation(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        recorder.events.append(f"http:{request.url.path}")
        return httpx.Response(200, json={"accepted": True})

    @asynccontextmanager
    async def session():
        context = MCPDataHubContext(client_factory=make_client_factory(recorder))
        async with httpx.AsyncClient(
            transport=httpx.MockTransport(remediation), timeout=10.0
        ) as http_client:
            yield AftershockIncidentProcessor(
                context,
                CompensatingActionEngine(http_client=http_client),
                clock=lambda: FIXED_NOW,
            )

    app.dependency_overrides[get_processor_session_factory] = lambda: session
    try:
        response = _post(
            {
                "incident_id": "  INC-9942  ",
                "dataset_urn": f"  {DATASET_URN}  ",
                "severity": " critical ",
            }
        )
    finally:
        app.dependency_overrides.clear()

    expected_document_urn = _incident_document_urn("INC-9942", DATASET_URN)
    assert response.status_code == 200
    body = response.json()
    assert body["status"] == "completed"
    assert body["incident_id"] == "INC-9942"
    assert body["dataset_urn"] == DATASET_URN
    assert body["context_mode"] == "mcp"
    assert body["counts"] == {"succeeded": 2, "failed": 0, "skipped": 0}
    assert [receipt["status"] for receipt in body["receipts"]] == [
        "succeeded",
        "succeeded",
    ]
    assert body["writeback"] == {
        "status": "succeeded",
        "document_urn": expected_document_urn,
        "error": None,
    }
    assert recorder.events[:2] == ["get_lineage", "get_entities"]
    assert recorder.events[-1] == "save_document"
    assert set(recorder.events[2:-1]) == {
        "http:/cancel-po",
        "http:/revert-price",
    }
    assert [name for name, _ in recorder.calls] == [
        "get_lineage",
        "get_entities",
        "save_document",
    ]
    assert recorder.calls[0][1]["upstream"] is False
    assert recorder.calls[-1][1]["urn"] == expected_document_urn
    assert {request.url.path for request in requests} == {
        "/cancel-po",
        "/revert-price",
    }
    assert {json.loads(request.content)["incident_id"] for request in requests} == {
        "INC-9942"
    }


def test_noncritical_envelope_is_ignored_without_entering_processor_session() -> None:
    entered = False

    @asynccontextmanager
    async def forbidden_session():
        nonlocal entered
        entered = True
        raise AssertionError("noncritical envelopes must not build DataHub context")
        yield  # pragma: no cover

    app.dependency_overrides[get_processor_session_factory] = (
        lambda: forbidden_session
    )
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


def test_mcp_failure_returns_fixed_secret_safe_service_error() -> None:
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
                CompensatingActionEngine(http_client=http_client),
                clock=lambda: FIXED_NOW,
            )

    app.dependency_overrides[get_processor_session_factory] = lambda: session
    try:
        response = _post(
            {
                "incident_id": "INC-FAIL",
                "dataset_urn": DATASET_URN,
                "severity": "CRITICAL",
            }
        )
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 503
    assert response.json() == SAFE_FAILURE
    assert "TOP-SECRET-MCP-TOKEN" not in response.text
    assert recorder.events == ["get_lineage"]


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
