"""Resettable loopback receiver that makes the live demo action observable."""

from __future__ import annotations

import hashlib
import ipaddress
import threading
import unicodedata
from dataclasses import dataclass
from typing import Annotated, Literal

from fastapi import FastAPI, Header, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from pydantic import BaseModel, ConfigDict, Field, field_validator

from demo_contract import (
    DEMO_ACTION,
    DEMO_BUSINESS_ACTION,
    DEMO_DATASET_URN,
    DEMO_JOB_URN,
    DEMO_PURCHASE_ORDER_ID,
    DEMO_RECEIVER_HOST,
    DEMO_RECEIVER_PORT,
    DEMO_REMEDIATION_PATH,
)


_IDEMPOTENCY_KEY_PATTERN = r"^aftershock-[0-9a-f]{64}$"
_INVALID_REQUEST_RECEIPT_ID = "aftershock-demo-invalid-request"


class DemoControlRequest(BaseModel):
    """The one compensating control supported by the seeded demo."""

    model_config = ConfigDict(extra="forbid", strict=True)

    incident_id: str = Field(min_length=1, max_length=128)
    target_urn: Literal[DEMO_JOB_URN]
    action: Literal[DEMO_ACTION]
    business_action: Literal[DEMO_BUSINESS_ACTION]

    @field_validator("incident_id")
    @classmethod
    def require_safe_incident_id(cls, value: str) -> str:
        if value != value.strip() or any(
            unicodedata.category(character).startswith("C") for character in value
        ):
            raise ValueError("incident_id must be a trimmed printable value")
        return value


IdempotencyKey = Annotated[
    str,
    Header(
        alias="Idempotency-Key",
        min_length=75,
        max_length=75,
        pattern=_IDEMPOTENCY_KEY_PATTERN,
    ),
]


@dataclass(frozen=True)
class _StoredRequest:
    payload: tuple[str, str, str, str]
    receipt: dict[str, object]


class _DemoState:
    """Guard demo state and idempotency records as one atomic unit."""

    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._history: dict[str, _StoredRequest] = {}
        self._reset_unlocked()

    def reset(self) -> dict[str, object]:
        with self._lock:
            self._history.clear()
            self._reset_unlocked()
            return self._snapshot_unlocked()

    def snapshot(self) -> dict[str, object]:
        with self._lock:
            return self._snapshot_unlocked()

    def apply(
        self, request: DemoControlRequest, idempotency_key: str
    ) -> tuple[int, dict[str, object]]:
        payload = (
            request.incident_id,
            request.target_urn,
            request.action,
            request.business_action,
        )
        with self._lock:
            previous = self._history.get(idempotency_key)
            if previous is not None:
                if previous.payload == payload:
                    return 200, dict(previous.receipt)
                return 409, _receipt(
                    "failed", _receipt_id("conflict", idempotency_key)
                )

            if self._canonical_success is not None:
                if self._canonical_success.payload == payload:
                    self._history[idempotency_key] = self._canonical_success
                    return 200, dict(self._canonical_success.receipt)
                return 409, _receipt(
                    "failed", _receipt_id("conflict", idempotency_key)
                )

            receipt = _receipt(
                "succeeded", _receipt_id("succeeded", idempotency_key)
            )
            stored = _StoredRequest(payload, receipt)
            self._history[idempotency_key] = stored
            self._canonical_success = stored
            if self._issue_po_enabled:
                self._issue_po_enabled = False
                self._purchase_order_status = "canceled"
                self._apply_count += 1
                self._last_incident_id = request.incident_id
                self._last_receipt_id = str(receipt["receipt_id"])
            return 200, dict(receipt)

    def _reset_unlocked(self) -> None:
        self._canonical_success: _StoredRequest | None = None
        self._issue_po_enabled = True
        self._purchase_order_status = "issued"
        self._apply_count = 0
        self._last_incident_id: str | None = None
        self._last_receipt_id: str | None = None

    def _snapshot_unlocked(self) -> dict[str, object]:
        return {
            "dataset_urn": DEMO_DATASET_URN,
            "target_urn": DEMO_JOB_URN,
            "business_action": DEMO_BUSINESS_ACTION,
            "purchase_order_id": DEMO_PURCHASE_ORDER_ID,
            "purchase_order_status": self._purchase_order_status,
            "issue_po_enabled": self._issue_po_enabled,
            "apply_count": self._apply_count,
            "last_incident_id": self._last_incident_id,
            "last_receipt_id": self._last_receipt_id,
        }


def _receipt(status: str, receipt_id: str) -> dict[str, object]:
    return {
        "receipt_version": 1,
        "status": status,
        "receipt_id": receipt_id,
    }


def _receipt_id(outcome: str, idempotency_key: str) -> str:
    digest = hashlib.sha256(idempotency_key.encode("ascii")).hexdigest()[:24]
    return (
        f"aftershock-demo-{outcome}-{DEMO_PURCHASE_ORDER_ID}-{digest}"
    )


def _is_loopback_peer(request: Request) -> bool:
    if request.client is None:
        return False
    host = request.client.host
    if host.casefold() == "localhost":
        return True
    try:
        return ipaddress.ip_address(host).is_loopback
    except ValueError:
        return False


def create_app() -> FastAPI:
    """Create an isolated receiver instance for one demo or test run."""

    state = _DemoState()
    receiver = FastAPI(
        title="Aftershock Demo Remediation Receiver",
        version="1.0.0",
    )

    @receiver.middleware("http")
    async def require_loopback(request: Request, call_next):
        if not _is_loopback_peer(request):
            return JSONResponse(
                status_code=403,
                content={
                    "detail": "demo receiver accepts loopback clients only"
                },
            )
        return await call_next(request)

    @receiver.exception_handler(RequestValidationError)
    async def invalid_request(
        _request: Request, _error: RequestValidationError
    ) -> JSONResponse:
        return JSONResponse(
            status_code=422,
            content=_receipt("failed", _INVALID_REQUEST_RECEIPT_ID),
        )

    @receiver.post("/demo/reset")
    def reset_demo() -> dict[str, object]:
        return state.reset()

    @receiver.get("/demo/state")
    def get_demo_state() -> dict[str, object]:
        return state.snapshot()

    @receiver.post(DEMO_REMEDIATION_PATH)
    def cancel_purchase_orders(
        payload: DemoControlRequest, idempotency_key: IdempotencyKey
    ) -> JSONResponse:
        status_code, receipt = state.apply(payload, idempotency_key)
        return JSONResponse(status_code=status_code, content=receipt)

    return receiver


app = create_app()


def main() -> None:
    """Run the demo receiver on its fixed loopback-only endpoint."""

    import uvicorn

    uvicorn.run(app, host=DEMO_RECEIVER_HOST, port=DEMO_RECEIVER_PORT)


if __name__ == "__main__":
    main()
