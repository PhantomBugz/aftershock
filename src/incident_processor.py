"""Coordinate DataHub discovery, controls, and incident-summary write-back."""

from __future__ import annotations

import hashlib
import logging
import re
import unicodedata
from collections.abc import Callable, Mapping
from datetime import datetime, timezone

from blast_radius_mapper import BlastRadiusMapper
from compensating_action_engine import CompensatingActionEngine
from datahub_context import DataHubContextPort
from remediation_models import (
    IncidentReport,
    RemediationReceipt,
    WriteBackReceipt,
)


logger = logging.getLogger("Aftershock-Processor")
Clock = Callable[[], datetime]
_WRITEBACK_ERROR = "DataHub remediation record persistence failed"
_DOCUMENT_URN_PATTERN = re.compile(r"urn:li:document:[^\s\x00-\x1f\x7f]+\Z")


class AftershockIncidentProcessor:
    """Run one incident from downstream discovery through DataHub evidence."""

    def __init__(
        self,
        context: DataHubContextPort,
        engine: CompensatingActionEngine,
        *,
        clock: Clock | None = None,
    ) -> None:
        self.context = context
        self.engine = engine
        self.mapper = BlastRadiusMapper(context)
        self._clock = clock or (lambda: datetime.now(timezone.utc))

    async def process(
        self, incident_id: str, dataset_urn: str
    ) -> IncidentReport:
        """Discover targets, settle controls, then persist exactly one summary.

        Discovery errors intentionally propagate before a report is produced.
        A write-back error is instead represented separately so it cannot alter
        the already-observed compensating-control receipts.
        """

        timestamp = _utc_timestamp(self._clock())
        targets = await self.mapper.get_targets(dataset_urn)
        receipts = tuple(
            await self.engine.process_blast_radius(targets, incident_id)
        )

        document_urn = _incident_document_urn(incident_id, dataset_urn)
        related_assets = list(
            dict.fromkeys([dataset_urn, *(target.urn for target in targets)])
        )
        content = _render_summary(
            incident_id=incident_id,
            dataset_urn=dataset_urn,
            context_mode=self.context.mode,
            timestamp=timestamp,
            receipts=receipts,
        )

        try:
            result = await self.context.save_document(
                document_type="Summary",
                title=f"Aftershock incident {_single_line(incident_id)}",
                content=content,
                urn=document_urn,
                related_assets=related_assets,
            )
            saved_urn = _saved_document_urn(
                result, expected_urn=document_urn
            )
        except Exception:
            # MCP/server details can contain credentials or response content.
            # Keep the external receipt fixed and log no exception text.
            logger.error("DataHub incident-summary write-back failed")
            writeback = WriteBackReceipt(
                status="failed",
                document_urn=None,
                error=_WRITEBACK_ERROR,
            )
        else:
            writeback = WriteBackReceipt(
                status="succeeded",
                document_urn=saved_urn,
                error=None,
            )

        return IncidentReport(
            incident_id=incident_id,
            dataset_urn=dataset_urn,
            context_mode=self.context.mode,
            timestamp=timestamp,
            receipts=receipts,
            writeback=writeback,
        )


def _utc_timestamp(observed: datetime) -> str:
    if not isinstance(observed, datetime):
        raise TypeError("incident clock must return a datetime")
    if observed.tzinfo is None or observed.utcoffset() is None:
        raise ValueError("incident clock must return a timezone-aware datetime")
    return (
        observed.astimezone(timezone.utc)
        .isoformat(timespec="seconds")
        .replace("+00:00", "Z")
    )


def _incident_document_urn(incident_id: str, dataset_urn: str) -> str:
    """Create a stable, injection-safe ID namespaced by source dataset."""

    normalized = _single_line(incident_id).lower()
    slug = re.sub(r"[^a-z0-9]+", "-", normalized).strip("-")[:48]
    if not slug:
        slug = "incident"
    digest = hashlib.sha256(
        f"{incident_id}\0{dataset_urn}".encode("utf-8", errors="surrogatepass")
    ).hexdigest()[:32]
    return f"urn:li:document:aftershock-incident-{slug}-{digest}"


def _saved_document_urn(result: object, *, expected_urn: str) -> str:
    if not isinstance(result, Mapping) or result.get("success") is not True:
        raise ValueError("invalid DataHub save result")
    urn = result.get("urn")
    if not isinstance(urn, str) or not _DOCUMENT_URN_PATTERN.fullmatch(urn):
        raise ValueError("invalid DataHub document URN")
    if urn != expected_urn:
        raise ValueError("DataHub returned a different document URN")
    return urn


def _single_line(value: object) -> str:
    """Collapse newlines and controls for titles without interpreting markup."""

    text = str(value)
    return " ".join(text.split()) or "unnamed"


def _markdown_value(value: object | None, *, table: bool = False) -> str:
    if value is None:
        return "—"
    text = str(value).replace("\r\n", "\n").replace("\r", "\n")
    text = "".join(
        character
        if character == "\n" or not unicodedata.category(character).startswith("C")
        else " "
        for character in text
    )
    text = text.replace("\\", "\\\\")
    text = (
        text.replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace("`", "&#96;")
    )
    for character in ("*", "[", "]", "!"):
        text = text.replace(character, f"\\{character}")
    if table:
        text = text.replace("|", "\\|")
    else:
        for character in ("#",):
            text = text.replace(character, f"\\{character}")
    return text.replace("\n", "<br>")


def _render_summary(
    *,
    incident_id: str,
    dataset_urn: str,
    context_mode: str,
    timestamp: str,
    receipts: tuple[RemediationReceipt, ...],
) -> str:
    lines = [
        "# Aftershock remediation summary",
        "",
        f"- Incident ID: {_markdown_value(incident_id)}",
        f"- Source dataset: {_markdown_value(dataset_urn)}",
        f"- Context mode: {_markdown_value(context_mode)}",
        f"- Timestamp (UTC): {_markdown_value(timestamp)}",
        "",
        "## Compensating-control receipts",
        "",
        "| Target URN | Entity type | Business action | Endpoint | Status | HTTP status | Error |",
        "| --- | --- | --- | --- | --- | --- | --- |",
    ]
    if receipts:
        for receipt in receipts:
            values = (
                receipt.target_urn,
                receipt.entity_type,
                receipt.business_action,
                receipt.endpoint,
                receipt.status,
                receipt.http_status,
                receipt.error,
            )
            lines.append(
                "| "
                + " | ".join(
                    _markdown_value(value, table=True) for value in values
                )
                + " |"
            )
    else:
        lines.extend(["", "No downstream remediation targets were found."])
    return "\n".join(lines) + "\n"
