import json
from collections.abc import AsyncIterator, Callable

import httpx
from fastapi.testclient import TestClient

from compensating_action_engine import CompensatingActionEngine
from lineage_listener import app, get_compensating_action_engine


DATASET_URN = "urn:li:dataset:(urn:li:dataPlatform:postgres,inventory_pricing,PROD)"


def build_engine_override(
    requests: list[httpx.Request],
) -> Callable[[], AsyncIterator[CompensatingActionEngine]]:
    async def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(200, json={"status": "accepted"})

    async def override() -> AsyncIterator[CompensatingActionEngine]:
        async with httpx.AsyncClient(
            transport=httpx.MockTransport(handler), timeout=10.0
        ) as client:
            yield CompensatingActionEngine(http_client=client)

    return override


def test_critical_incident_triggers_downstream_remediations(monkeypatch) -> None:
    monkeypatch.setenv("AFTERSHOCK_DATAHUB_MODE", "fixture")
    requests: list[httpx.Request] = []
    app.dependency_overrides[get_compensating_action_engine] = build_engine_override(
        requests
    )

    try:
        with TestClient(app) as client:
            response = client.post(
                "/webhook/datahub",
                json={
                    "incident_id": "INC-9942",
                    "dataset_urn": DATASET_URN,
                    "severity": "CRITICAL",
                },
            )
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 202
    assert response.json() == {
        "status": "accepted",
        "incident_id": "INC-9942",
        "targets_found": 2,
        "remediations_triggered": 2,
        "results": [
            {
                "incident_id": "INC-9942",
                "target_urn": (
                    "urn:li:dataJob:(urn:li:dataFlow:(airflow,aftershock_demo,PROD),"
                    "purchase_order_generator)"
                ),
                "entity_type": "DATA_JOB",
                "business_action": "ISSUE_PO",
                "endpoint": "https://api.internal.example/remediate/cancel_po",
                "status": "succeeded",
                "http_status": 200,
                "error": None,
            },
            {
                "incident_id": "INC-9942",
                "target_urn": (
                    "urn:li:mlModel:(urn:li:dataPlatform:sagemaker,"
                    "dynamic_pricing_model,PROD)"
                ),
                "entity_type": "ML_MODEL",
                "business_action": "ADJUST_PRICE",
                "endpoint": "https://api.internal.example/remediate/revert_pricing",
                "status": "succeeded",
                "http_status": 200,
                "error": None,
            },
        ],
    }
    assert {request.url.path for request in requests} == {
        "/remediate/cancel_po",
        "/remediate/revert_pricing",
    }
    assert {json.loads(request.content)["incident_id"] for request in requests} == {
        "INC-9942"
    }


def test_noncritical_incident_returns_no_action_required(monkeypatch) -> None:
    monkeypatch.setenv("AFTERSHOCK_DATAHUB_MODE", "fixture")
    requests: list[httpx.Request] = []
    app.dependency_overrides[get_compensating_action_engine] = build_engine_override(
        requests
    )

    try:
        with TestClient(app) as client:
            response = client.post(
                "/webhook/datahub",
                json={
                    "incident_id": "INC-9943",
                    "dataset_urn": DATASET_URN,
                    "severity": "WARNING",
                },
            )
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 200
    assert response.json() == {
        "status": "ignored",
        "incident_id": "INC-9943",
        "message": "no action required",
    }
    assert requests == []
