from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor

import pytest
from fastapi.testclient import TestClient

from demo_contract import (
    DEMO_ACTION,
    DEMO_BUSINESS_ACTION,
    DEMO_DATASET_URN,
    DEMO_INCIDENT_ID,
    DEMO_JOB_URN,
    DEMO_PURCHASE_ORDER_ID,
    DEMO_REMEDIATION_ENDPOINT,
)
from demo_remediation_receiver import create_app


VALID_KEY = "aftershock-" + "a" * 64
OTHER_VALID_KEY = "aftershock-" + "b" * 64


def _payload(*, incident_id: str = DEMO_INCIDENT_ID) -> dict[str, str]:
    return {
        "incident_id": incident_id,
        "target_urn": DEMO_JOB_URN,
        "action": DEMO_ACTION,
        "business_action": DEMO_BUSINESS_ACTION,
    }


def _client(*, host: str = "127.0.0.1") -> TestClient:
    return TestClient(create_app(), client=(host, 54321))


def _assert_v1_receipt(body: object, status: str) -> str:
    assert isinstance(body, dict)
    assert body.keys() == {"receipt_version", "status", "receipt_id"}
    assert body["receipt_version"] == 1
    assert body["status"] == status
    receipt_id = body["receipt_id"]
    assert isinstance(receipt_id, str)
    assert receipt_id == receipt_id.strip()
    assert receipt_id
    assert len(receipt_id) <= 256
    return receipt_id


def test_contract_matches_the_seeded_live_datahub_demo() -> None:
    assert DEMO_DATASET_URN == (
        "urn:li:dataset:(urn:li:dataPlatform:postgres,"
        "aftershock_demo.inventory_pricing,DEV)"
    )
    assert DEMO_JOB_URN == (
        "urn:li:dataJob:(urn:li:dataFlow:(airflow,aftershock_demo,DEV),"
        "purchase_order_generator)"
    )
    assert DEMO_INCIDENT_ID == "INC-LIVE-001"
    assert DEMO_ACTION == "REVERT_STATE"
    assert DEMO_BUSINESS_ACTION == "ISSUE_PO"
    assert DEMO_PURCHASE_ORDER_ID == "PO-AFTERSHOCK-001"
    assert DEMO_REMEDIATION_ENDPOINT == (
        "http://127.0.0.1:8765/remediate/cancel_po"
    )


def test_reset_then_remediation_exposes_a_real_state_transition() -> None:
    with _client() as client:
        reset = client.post("/demo/reset")
        before = client.get("/demo/state")
        response = client.post(
            "/remediate/cancel_po",
            headers={"Idempotency-Key": VALID_KEY},
            json=_payload(),
        )
        after = client.get("/demo/state")

    expected_initial = {
        "dataset_urn": DEMO_DATASET_URN,
        "target_urn": DEMO_JOB_URN,
        "business_action": DEMO_BUSINESS_ACTION,
        "purchase_order_id": DEMO_PURCHASE_ORDER_ID,
        "purchase_order_status": "issued",
        "issue_po_enabled": True,
        "apply_count": 0,
        "last_incident_id": None,
        "last_receipt_id": None,
    }
    assert reset.status_code == 200
    assert reset.json() == expected_initial
    assert before.json() == expected_initial
    assert response.status_code == 200
    receipt_id = _assert_v1_receipt(response.json(), "succeeded")
    assert DEMO_PURCHASE_ORDER_ID in receipt_id
    assert after.json() == {
        **expected_initial,
        "purchase_order_status": "canceled",
        "issue_po_enabled": False,
        "apply_count": 1,
        "last_incident_id": DEMO_INCIDENT_ID,
        "last_receipt_id": receipt_id,
    }


def test_same_key_and_payload_replays_the_same_receipt_without_reapplying() -> None:
    with _client() as client:
        first = client.post(
            "/remediate/cancel_po",
            headers={"Idempotency-Key": VALID_KEY},
            json=_payload(),
        )
        second = client.post(
            "/remediate/cancel_po",
            headers={"Idempotency-Key": VALID_KEY},
            json=_payload(),
        )
        state = client.get("/demo/state")

    assert first.status_code == second.status_code == 200
    assert first.json() == second.json()
    receipt_id = _assert_v1_receipt(second.json(), "succeeded")
    assert DEMO_PURCHASE_ORDER_ID in receipt_id
    assert state.json()["apply_count"] == 1
    assert state.json()["purchase_order_id"] == DEMO_PURCHASE_ORDER_ID
    assert state.json()["purchase_order_status"] == "canceled"


def test_distinct_key_for_the_same_canceled_order_returns_canonical_receipt() -> None:
    with _client() as client:
        first = client.post(
            "/remediate/cancel_po",
            headers={"Idempotency-Key": VALID_KEY},
            json=_payload(),
        )
        second = client.post(
            "/remediate/cancel_po",
            headers={"Idempotency-Key": OTHER_VALID_KEY},
            json=_payload(),
        )
        state = client.get("/demo/state").json()

    assert first.status_code == second.status_code == 200
    assert first.json() == second.json()
    assert state["apply_count"] == 1
    assert state["purchase_order_status"] == "canceled"
    assert state["last_receipt_id"] == first.json()["receipt_id"]


def test_same_key_with_a_different_payload_is_a_terminal_conflict() -> None:
    with _client() as client:
        applied = client.post(
            "/remediate/cancel_po",
            headers={"Idempotency-Key": VALID_KEY},
            json=_payload(),
        )
        before_conflict = client.get("/demo/state").json()
        conflict = client.post(
            "/remediate/cancel_po",
            headers={"Idempotency-Key": VALID_KEY},
            json=_payload(incident_id="INC-LIVE-002"),
        )
        after_conflict = client.get("/demo/state").json()

    assert applied.status_code == 200
    assert conflict.status_code == 409
    _assert_v1_receipt(conflict.json(), "failed")
    assert after_conflict == before_conflict


def test_distinct_key_cannot_retarget_an_already_canceled_order() -> None:
    with _client() as client:
        applied = client.post(
            "/remediate/cancel_po",
            headers={"Idempotency-Key": VALID_KEY},
            json=_payload(),
        )
        before_conflict = client.get("/demo/state").json()
        conflict = client.post(
            "/remediate/cancel_po",
            headers={"Idempotency-Key": OTHER_VALID_KEY},
            json=_payload(incident_id="INC-LIVE-002"),
        )
        after_conflict = client.get("/demo/state").json()

    assert applied.status_code == 200
    assert conflict.status_code == 409
    _assert_v1_receipt(conflict.json(), "failed")
    assert after_conflict == before_conflict


@pytest.mark.parametrize(
    ("headers", "payload"),
    [
        ({}, _payload()),
        ({"Idempotency-Key": "not-valid"}, _payload()),
        (
            {"Idempotency-Key": VALID_KEY},
            {**_payload(), "target_urn": "urn:li:dataJob:wrong"},
        ),
        (
            {"Idempotency-Key": VALID_KEY},
            {**_payload(), "action": "PAUSE_PIPELINE"},
        ),
        (
            {"Idempotency-Key": VALID_KEY},
            {**_payload(), "business_action": "ADJUST_PRICE"},
        ),
        (
            {"Idempotency-Key": VALID_KEY},
            {**_payload(), "unexpected": "field"},
        ),
        (
            {"Idempotency-Key": VALID_KEY},
            {**_payload(), "incident_id": " INC-LIVE-001 "},
        ),
    ],
)
def test_invalid_control_requests_fail_closed_without_mutation(
    headers: dict[str, str], payload: dict[str, str]
) -> None:
    with _client() as client:
        response = client.post(
            "/remediate/cancel_po", headers=headers, json=payload
        )
        state = client.get("/demo/state")

    assert response.status_code == 422
    _assert_v1_receipt(response.json(), "failed")
    assert state.json()["issue_po_enabled"] is True
    assert state.json()["apply_count"] == 0
    assert state.json()["purchase_order_id"] == DEMO_PURCHASE_ORDER_ID
    assert state.json()["purchase_order_status"] == "issued"


@pytest.mark.parametrize("path,method", [("/demo/state", "get"), ("/demo/reset", "post"), ("/remediate/cancel_po", "post")])
def test_every_endpoint_rejects_non_loopback_peers(path: str, method: str) -> None:
    app = create_app()
    with TestClient(app, client=("192.0.2.25", 54321)) as client:
        kwargs = (
            {
                "headers": {"Idempotency-Key": VALID_KEY},
                "json": _payload(),
            }
            if path.startswith("/remediate/")
            else {}
        )
        response = getattr(client, method)(path, **kwargs)

    assert response.status_code == 403
    assert response.json() == {"detail": "demo receiver accepts loopback clients only"}


def test_concurrent_retries_apply_once_and_return_one_receipt() -> None:
    app = create_app()

    def call_receiver(_: int) -> tuple[int, dict[str, object]]:
        with TestClient(app, client=("127.0.0.1", 54321)) as client:
            response = client.post(
                "/remediate/cancel_po",
                headers={"Idempotency-Key": VALID_KEY},
                json=_payload(),
            )
            return response.status_code, response.json()

    with ThreadPoolExecutor(max_workers=12) as pool:
        results = list(pool.map(call_receiver, range(24)))

    with TestClient(app, client=("127.0.0.1", 54321)) as client:
        state = client.get("/demo/state").json()

    assert {status for status, _ in results} == {200}
    assert len({receipt["receipt_id"] for _, receipt in results}) == 1
    assert all(
        DEMO_PURCHASE_ORDER_ID in str(receipt["receipt_id"])
        for _, receipt in results
    )
    assert state["issue_po_enabled"] is False
    assert state["apply_count"] == 1
    assert state["purchase_order_id"] == DEMO_PURCHASE_ORDER_ID
    assert state["purchase_order_status"] == "canceled"


def test_concurrent_distinct_keys_cancel_once_and_return_canonical_receipt() -> None:
    app = create_app()

    def call_receiver(index: int) -> tuple[int, dict[str, object]]:
        key = f"aftershock-{index:064x}"
        with TestClient(app, client=("127.0.0.1", 54321)) as client:
            response = client.post(
                "/remediate/cancel_po",
                headers={"Idempotency-Key": key},
                json=_payload(),
            )
            return response.status_code, response.json()

    with ThreadPoolExecutor(max_workers=12) as pool:
        results = list(pool.map(call_receiver, range(1, 25)))

    with TestClient(app, client=("127.0.0.1", 54321)) as client:
        state = client.get("/demo/state").json()

    assert {status for status, _ in results} == {200}
    assert len({receipt["receipt_id"] for _, receipt in results}) == 1
    assert state["apply_count"] == 1
    assert state["purchase_order_status"] == "canceled"
    assert state["last_receipt_id"] == results[0][1]["receipt_id"]


def test_reset_clears_state_and_idempotency_history_for_a_fresh_demo() -> None:
    with _client() as client:
        client.post(
            "/remediate/cancel_po",
            headers={"Idempotency-Key": VALID_KEY},
            json=_payload(),
        )
        reset = client.post("/demo/reset")
        replay = client.post(
            "/remediate/cancel_po",
            headers={"Idempotency-Key": VALID_KEY},
            json=_payload(),
        )
        state = client.get("/demo/state")

    assert reset.json()["issue_po_enabled"] is True
    assert reset.json()["apply_count"] == 0
    assert reset.json()["purchase_order_id"] == DEMO_PURCHASE_ORDER_ID
    assert reset.json()["purchase_order_status"] == "issued"
    assert replay.status_code == 200
    _assert_v1_receipt(replay.json(), "succeeded")
    assert state.json()["issue_po_enabled"] is False
    assert state.json()["apply_count"] == 1
    assert state.json()["purchase_order_status"] == "canceled"
