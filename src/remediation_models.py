"""Immutable domain models shared by the Aftershock remediation workflow."""

from __future__ import annotations

from dataclasses import asdict, dataclass


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
