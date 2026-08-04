"""Immutable domain models shared by the Aftershock remediation workflow."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Literal


RemediationStatus = Literal["succeeded", "failed", "skipped"]
WriteBackStatus = Literal["succeeded", "failed"]


@dataclass(frozen=True)
class ActionableTarget:
    """One downstream DataHub entity and its optional control playbook."""

    urn: str
    entity_type: str
    business_action: str | None
    remediation_webhook: str | None

    def to_dict(self) -> dict[str, str | None]:
        """Return a JSON-serializable representation of the target."""

        return asdict(self)


@dataclass(frozen=True)
class RemediationReceipt:
    """Immutable evidence for one attempted compensating control."""

    incident_id: str
    target_urn: str
    entity_type: str
    business_action: str | None
    endpoint: str | None
    status: RemediationStatus
    http_status: int | None
    error: str | None

    def to_dict(self) -> dict[str, object]:
        """Return a JSON-serializable representation of the receipt."""

        return asdict(self)


@dataclass(frozen=True)
class WriteBackReceipt:
    """Outcome of persisting one incident summary to DataHub."""

    status: WriteBackStatus
    document_urn: str | None
    error: str | None

    def to_dict(self) -> dict[str, str | None]:
        """Return a JSON-serializable representation of the write-back."""

        return {
            "status": self.status,
            "document_urn": self.document_urn,
            "error": self.error,
        }


@dataclass(frozen=True)
class IncidentReport:
    """Immutable evidence from one complete Aftershock incident run."""

    incident_id: str
    dataset_urn: str
    context_mode: str
    timestamp: str
    receipts: tuple[RemediationReceipt, ...]
    writeback: WriteBackReceipt

    def to_dict(self) -> dict[str, object]:
        """Serialize the report with explicit receipt-status counts."""

        counts = {
            status: sum(receipt.status == status for receipt in self.receipts)
            for status in ("succeeded", "failed", "skipped")
        }
        return {
            "incident_id": self.incident_id,
            "dataset_urn": self.dataset_urn,
            "context_mode": self.context_mode,
            "timestamp": self.timestamp,
            "counts": counts,
            "receipts": [receipt.to_dict() for receipt in self.receipts],
            "writeback": self.writeback.to_dict(),
        }
