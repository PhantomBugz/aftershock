"""Execute compensating controls and emit immutable remediation receipts."""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Sequence
from urllib.parse import urlsplit, urlunsplit

import httpx

from remediation_models import (
    ActionableTarget,
    RemediationReceipt,
    RemediationStatus,
)


logger = logging.getLogger("Aftershock-Engine")


class CompensatingActionEngine:
    """Execute independent downstream controls without coupling their failures."""

    def __init__(self, http_client: httpx.AsyncClient | None = None) -> None:
        self.http_client = http_client or httpx.AsyncClient(timeout=10.0)
        self._owns_client = http_client is None

    async def aclose(self) -> None:
        """Close the HTTP client only when the engine created it."""

        if self._owns_client:
            await self.http_client.aclose()

    async def execute_rollback(
        self, target: ActionableTarget, incident_id: str
    ) -> RemediationReceipt:
        """Attempt one control and return a secret-safe, structured receipt."""

        endpoint = _sanitize_endpoint(target.remediation_webhook)
        missing = _missing_playbook_error(target)
        if missing is not None:
            logger.warning(
                "Skipping remediation control for target %s: incomplete playbook",
                target.urn,
            )
            return _receipt(
                target,
                incident_id,
                endpoint=endpoint,
                status="skipped",
                error=missing,
            )

        if endpoint is None:
            logger.error(
                "Remediation control for target %s has an invalid endpoint",
                target.urn,
            )
            return _receipt(
                target,
                incident_id,
                endpoint=None,
                status="failed",
                error="invalid remediation endpoint",
            )

        payload = {
            "incident_id": incident_id,
            "target_urn": target.urn,
            "action": "REVERT_STATE",
            "business_action": target.business_action,
        }

        try:
            # Execute the configured endpoint unchanged so authenticated or
            # query-routed webhooks retain their semantics. This module never
            # logs it and persists only the sanitized endpoint above.
            response = await self.http_client.post(
                target.remediation_webhook, json=payload
            )
            response.raise_for_status()
        except httpx.HTTPStatusError as exc:
            http_status = exc.response.status_code
            logger.error(
                "Remediation control failed for target %s with HTTP %d",
                target.urn,
                http_status,
            )
            return _receipt(
                target,
                incident_id,
                endpoint=endpoint,
                status="failed",
                http_status=http_status,
                error=f"remediation endpoint returned HTTP {http_status}",
            )
        except httpx.RequestError:
            logger.error(
                "Remediation request failed for target %s",
                target.urn,
            )
            return _receipt(
                target,
                incident_id,
                endpoint=endpoint,
                status="failed",
                error="remediation request failed",
            )
        except Exception:
            # Exception details can include response bodies, credentials, or
            # URLs. Preserve isolation while returning only a fixed summary.
            logger.error(
                "Unexpected remediation error for target %s",
                target.urn,
            )
            return _receipt(
                target,
                incident_id,
                endpoint=endpoint,
                status="failed",
                error="unexpected remediation error",
            )

        logger.info(
            "Remediation control succeeded for target %s with HTTP %d",
            target.urn,
            response.status_code,
        )
        return _receipt(
            target,
            incident_id,
            endpoint=endpoint,
            status="succeeded",
            http_status=response.status_code,
        )

    async def process_blast_radius(
        self, targets: Sequence[ActionableTarget], incident_id: str
    ) -> list[RemediationReceipt]:
        """Execute every target concurrently and retain input ordering."""

        logger.info(
            "Processing %d downstream remediation controls for incident %s",
            len(targets),
            incident_id,
        )
        receipts = list(
            await asyncio.gather(
                *(self.execute_rollback(target, incident_id) for target in targets)
            )
        )
        logger.info(
            "Remediation controls settled for incident %s: %d succeeded, "
            "%d failed, %d skipped",
            incident_id,
            sum(receipt.status == "succeeded" for receipt in receipts),
            sum(receipt.status == "failed" for receipt in receipts),
            sum(receipt.status == "skipped" for receipt in receipts),
        )
        return receipts


def _missing_playbook_error(target: ActionableTarget) -> str | None:
    missing_action = not target.business_action
    missing_webhook = not target.remediation_webhook
    if missing_action and missing_webhook:
        return "missing business action and remediation webhook"
    if missing_action:
        return "missing business action"
    if missing_webhook:
        return "missing remediation webhook"
    return None


def _sanitize_endpoint(raw_url: str | None) -> str | None:
    """Return only a valid HTTP(S) endpoint's scheme, host, port, and path."""

    if not isinstance(raw_url, str) or not raw_url:
        return None
    try:
        parts = urlsplit(raw_url)
        scheme = parts.scheme.lower()
        hostname = parts.hostname
        port = parts.port
    except (UnicodeError, ValueError):
        return None

    if scheme not in {"http", "https"} or not hostname:
        return None
    if any(character.isspace() for character in hostname):
        return None

    safe_host = f"[{hostname}]" if ":" in hostname else hostname
    netloc = f"{safe_host}:{port}" if port is not None else safe_host
    return urlunsplit((scheme, netloc, parts.path, "", ""))


def _receipt(
    target: ActionableTarget,
    incident_id: str,
    *,
    endpoint: str | None,
    status: RemediationStatus,
    http_status: int | None = None,
    error: str | None = None,
) -> RemediationReceipt:
    return RemediationReceipt(
        incident_id=incident_id,
        target_urn=target.urn,
        entity_type=target.entity_type,
        business_action=target.business_action,
        endpoint=endpoint,
        status=status,
        http_status=http_status,
        error=error,
    )
