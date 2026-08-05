"""Coordinate DataHub discovery, controls, and incident-summary write-back."""

from __future__ import annotations

import hashlib
import logging
import re
import unicodedata
from collections.abc import Callable, Mapping
from datetime import datetime, timezone
from typing import Literal

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
WorkflowPhase = Literal["observe", "decide", "act", "persist"]
MilestoneObserver = Callable[[WorkflowPhase, str], None]
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
        milestone_observer: MilestoneObserver | None = None,
    ) -> None:
        self.context = context
        self.engine = engine
        self.mapper = BlastRadiusMapper(context)
        self._clock = clock or (lambda: datetime.now(timezone.utc))
        self._milestone_observer = milestone_observer

    def _announce(self, phase: WorkflowPhase, detail: str) -> None:
        """Notify presentation observers without changing workflow behavior."""

        if self._milestone_observer is None:
            return
        try:
            self._milestone_observer(phase, detail)
        except Exception:
            logger.warning("Aftershock workflow milestone observer failed")

    async def process(
        self, incident_id: str, dataset_urn: str
    ) -> IncidentReport:
        """Discover targets, settle controls, then persist one summary write.

        Discovery errors intentionally propagate before a report is produced.
        A write-back error is instead represented separately so it cannot alter
        the already-observed compensating-control receipts.
        """

        timestamp = _utc_timestamp(self._clock())
        self._announce(
            "observe", "reading DataHub lineage and entity metadata"
        )
        targets = await self.mapper.get_targets(dataset_urn)
        self._announce(
            "decide",
            f"resolved {len(targets)} metadata-backed remediation targets",
        )
        self._announce(
            "act",
            f"evaluating {len(targets)} targets against exact remediation grants",
        )
        receipts = tuple(
            await self.engine.process_blast_radius(targets, incident_id)
        )

        related_assets = list(
            dict.fromkeys([dataset_urn, *(target.urn for target in targets)])
        )
        record_key = _incident_document_urn(incident_id, dataset_urn)
        content = _render_summary(
            incident_id=incident_id,
            dataset_urn=dataset_urn,
            record_key=record_key,
            context_mode=self.context.mode,
            timestamp=timestamp,
            receipts=receipts,
        )
        title = f"Aftershock incident {_single_line(incident_id)}"

        self._announce("persist", "saving receipt evidence to DataHub")
        try:
            existing_urn = await _existing_incident_document_urn(
                self.context,
                title=title,
                dataset_urn=dataset_urn,
                record_key=record_key,
            )
            result = await self.context.save_document(
                document_type="Summary",
                title=title,
                content=content,
                urn=existing_urn,
                related_assets=related_assets,
            )
            saved_urn = _saved_document_urn(
                result,
                expected_urn=existing_urn,
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


async def _existing_incident_document_urn(
    context: DataHubContextPort,
    *,
    title: str,
    dataset_urn: str,
    record_key: str,
) -> str | None:
    """Find one stable prior write-back for this incident and dataset."""

    payload = await context.search_documents(
        query=title,
        num_results=50,
        offset=0,
    )
    search_results = payload.get("searchResults")
    if not isinstance(search_results, list):
        raise ValueError("invalid DataHub document search result")
    total = payload.get("total")
    if (
        isinstance(total, int)
        and not isinstance(total, bool)
        and total > len(search_results)
    ):
        raise ValueError("incomplete DataHub document search result")

    exact_urns: set[str] = set()
    for result in search_results:
        if not isinstance(result, Mapping):
            raise ValueError("invalid DataHub document search result")
        entity = result.get("entity")
        if not isinstance(entity, Mapping):
            raise ValueError("invalid DataHub document search result")
        info = entity.get("info")
        result_title = info.get("title") if isinstance(info, Mapping) else None
        if result_title != title:
            continue
        urn = entity.get("urn")
        if not isinstance(urn, str) or not _DOCUMENT_URN_PATTERN.fullmatch(urn):
            raise ValueError("invalid DataHub document search result")
        exact_urns.add(urn)

    if not exact_urns:
        return None

    ordered_urns = sorted(exact_urns)
    dataset_marker = f"- Source dataset: {_markdown_value(dataset_urn)}"
    record_key_marker = f"- Record key: {record_key}"
    grep_payload = await context.grep_documents(
        urns=ordered_urns,
        pattern=(
            rf"(?m)^(?:{re.escape('- Record key: ')}[^\r\n]+|"
            rf"{re.escape(dataset_marker)})$"
        ),
        context_chars=0,
        max_matches_per_doc=2,
        start_offset=0,
    )
    grep_results = grep_payload.get("results")
    if not isinstance(grep_results, list):
        raise ValueError("invalid DataHub document content result")

    requested_key_urns: set[str] = set()
    foreign_key_urns: set[str] = set()
    dataset_urns: set[str] = set()
    for result in grep_results:
        if not isinstance(result, Mapping):
            raise ValueError("invalid DataHub document content result")
        urn = result.get("urn")
        matches = result.get("matches")
        if urn not in exact_urns or not isinstance(matches, list):
            raise ValueError("invalid DataHub document content result")
        document_total = result.get("total_matches")
        if (
            not isinstance(document_total, int)
            or isinstance(document_total, bool)
            or document_total != len(matches)
        ):
            raise ValueError("truncated DataHub document content result")
        for match in matches:
            if not isinstance(match, Mapping):
                raise ValueError("invalid DataHub document content result")
            excerpt = match.get("excerpt")
            if not isinstance(excerpt, str):
                raise ValueError("invalid DataHub document content result")
            if record_key_marker in excerpt:
                requested_key_urns.add(urn)
            elif "- Record key:" in excerpt:
                foreign_key_urns.add(urn)
            elif dataset_marker in excerpt:
                dataset_urns.add(urn)
            else:
                raise ValueError("invalid DataHub document content result")

    keyed_urns = requested_key_urns - foreign_key_urns
    if keyed_urns:
        return min(keyed_urns)
    legacy_urns = dataset_urns - requested_key_urns - foreign_key_urns
    return min(legacy_urns) if legacy_urns else None


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


def _saved_document_urn(
    result: object, *, expected_urn: str | None = None
) -> str:
    if not isinstance(result, Mapping) or result.get("success") is not True:
        raise ValueError("invalid DataHub save result")
    urn = result.get("urn")
    if not isinstance(urn, str) or not _DOCUMENT_URN_PATTERN.fullmatch(urn):
        raise ValueError("invalid DataHub document URN")
    if expected_urn is not None and urn != expected_urn:
        raise ValueError("DataHub returned a different document URN")
    return urn


def _single_line(value: object) -> str:
    """Collapse newlines and controls for titles without interpreting markup."""

    text = "".join(
        " " if unicodedata.category(character).startswith("C") else character
        for character in str(value)
    )
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
    record_key: str,
    context_mode: str,
    timestamp: str,
    receipts: tuple[RemediationReceipt, ...],
) -> str:
    lines = [
        "# Aftershock remediation summary",
        "",
        f"- Incident ID: {_markdown_value(incident_id)}",
        f"- Source dataset: {_markdown_value(dataset_urn)}",
        f"- Record key: {_markdown_value(record_key)}",
        f"- Context mode: {_markdown_value(context_mode)}",
        f"- Timestamp (UTC): {_markdown_value(timestamp)}",
        "",
        "## Compensating-control receipts",
        "",
        "| Target URN | Entity type | Business action | Endpoint | Status | "
        "HTTP status | External receipt ID | Error |",
        "| --- | --- | --- | --- | --- | --- | --- | --- |",
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
                receipt.external_receipt_id,
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
