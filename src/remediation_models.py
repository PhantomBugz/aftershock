"""Immutable domain models shared by the Aftershock remediation workflow."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Literal


RemediationStatus = Literal["succeeded", "failed", "skipped"]


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
