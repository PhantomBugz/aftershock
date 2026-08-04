"""Live, receipt-driven Aftershock demonstration against DataHub MCP."""

from __future__ import annotations

import asyncio
import re
import time
from collections.abc import Awaitable, Callable, Mapping, Sequence
from dataclasses import dataclass
from typing import Any, TypeVar

import httpx
from rich.console import Console
from rich.panel import Panel
from rich.table import Table

from compensating_action_engine import RemediationGrant
from datahub_context import (
    DataHubContextPort,
    DataHubMCPError,
    build_datahub_context_from_env,
)
from demo_contract import (
    DEMO_BUSINESS_ACTION,
    DEMO_DATASET_URN,
    DEMO_INCIDENT_ID,
    DEMO_JOB_URN,
    DEMO_PURCHASE_ORDER_ID,
    DEMO_RECEIVER_HOST,
    DEMO_RECEIVER_PORT,
    DEMO_REMEDIATION_ENDPOINT,
)
from demo_dashboard import run_demo
from incident_processor import _single_line
from remediation_models import IncidentReport


_RECEIVER_ORIGIN = (
    f"http://{DEMO_RECEIVER_HOST}:{DEMO_RECEIVER_PORT}"
)
_RESET_URL = f"{_RECEIVER_ORIGIN}/demo/reset"
_STATE_URL = f"{_RECEIVER_ORIGIN}/demo/state"
_READBACK_TIMEOUT_SECONDS = 30.0
_READBACK_POLL_INTERVAL_SECONDS = 1.0
_HTTP_TIMEOUT_SECONDS = 10.0
_SUCCESS_RECEIPT_PATTERN = re.compile(
    rf"aftershock-demo-succeeded-{re.escape(DEMO_PURCHASE_ORDER_ID)}-"
    r"[0-9a-f]{24}\Z"
)

T = TypeVar("T")


class LiveDemoError(RuntimeError):
    """A controlled failure of the live demonstration proof gate."""


@dataclass(frozen=True, slots=True)
class DocumentReadbackProof:
    """Independent evidence that DataHub indexed the saved incident record."""

    document_urn: str
    title: str
    markers: tuple[str, ...]
    related_asset_urns: tuple[str, ...]


def _is_po_bound_success_receipt(value: object) -> bool:
    return isinstance(value, str) and bool(
        _SUCCESS_RECEIPT_PATTERN.fullmatch(value)
    )


def _readback_timeout(timeout_seconds: float) -> LiveDemoError:
    return LiveDemoError(
        f"timed out after {timeout_seconds:g}s waiting for independent "
        "DataHub document readback proof"
    )


async def _call_before_deadline(
    operation: Callable[[], Awaitable[T]],
    *,
    deadline: float,
    monotonic: Callable[[], float],
    timeout_seconds: float,
) -> T:
    remaining = deadline - monotonic()
    if remaining <= 0:
        raise _readback_timeout(timeout_seconds)
    try:
        return await asyncio.wait_for(operation(), timeout=remaining)
    except TimeoutError:
        raise _readback_timeout(timeout_seconds) from None


def _search_contains_document(
    payload: object, *, document_urn: str, title: str
) -> bool:
    if not isinstance(payload, Mapping):
        return False
    results = payload.get("searchResults")
    if not isinstance(results, Sequence) or isinstance(results, (str, bytes)):
        return False
    for result in results:
        if not isinstance(result, Mapping):
            continue
        entity = result.get("entity")
        if not isinstance(entity, Mapping):
            continue
        info = entity.get("info")
        if (
            entity.get("urn") == document_urn
            and isinstance(info, Mapping)
            and info.get("title") == title
        ):
            return True
    return False


def _grep_contains_markers(
    payload: object,
    *,
    document_urn: str,
    title: str,
    markers: Sequence[str],
) -> bool:
    if not isinstance(payload, Mapping):
        return False
    results = payload.get("results")
    if not isinstance(results, Sequence) or isinstance(results, (str, bytes)):
        return False
    excerpts: list[str] = []
    for result in results:
        if not isinstance(result, Mapping):
            continue
        if result.get("urn") != document_urn or result.get("title") != title:
            continue
        matches = result.get("matches")
        if not isinstance(matches, Sequence) or isinstance(matches, (str, bytes)):
            continue
        for match in matches:
            if isinstance(match, Mapping) and isinstance(
                match.get("excerpt"), str
            ):
                excerpts.append(match["excerpt"])
    evidence = "\n".join(excerpts)
    return all(marker in evidence for marker in markers)


def _entities_link_document(
    payload: object,
    *,
    asset_urns: Sequence[str],
    document_urn: str,
) -> bool:
    if not isinstance(payload, Sequence) or isinstance(payload, (str, bytes)):
        return False
    entities = {
        entity.get("urn"): entity
        for entity in payload
        if isinstance(entity, Mapping) and isinstance(entity.get("urn"), str)
    }
    for asset_urn in asset_urns:
        entity = entities.get(asset_urn)
        if not isinstance(entity, Mapping):
            return False
        related = entity.get("relatedDocuments")
        documents = (
            related.get("documents") if isinstance(related, Mapping) else None
        )
        if not isinstance(documents, Sequence) or isinstance(
            documents, (str, bytes)
        ):
            return False
        if not any(
            isinstance(document, Mapping)
            and document.get("urn") == document_urn
            for document in documents
        ):
            return False
    return True


async def wait_for_document_readback(
    context: Any,
    *,
    document_urn: str,
    expected_title: str,
    incident_id: str,
    receipt_ids: Sequence[str],
    related_asset_urns: Sequence[str],
    timeout_seconds: float = _READBACK_TIMEOUT_SECONDS,
    poll_interval_seconds: float = _READBACK_POLL_INTERVAL_SECONDS,
    monotonic: Callable[[], float] = time.monotonic,
    sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
) -> DocumentReadbackProof:
    """Poll three independent MCP reads until DataHub proves persistence."""

    if timeout_seconds <= 0:
        raise ValueError("timeout_seconds must be positive")
    if poll_interval_seconds <= 0:
        raise ValueError("poll_interval_seconds must be positive")
    markers = (incident_id, *tuple(receipt_ids))
    assets = tuple(related_asset_urns)
    if (
        not document_urn
        or not expected_title
        or not incident_id
        or not receipt_ids
        or not assets
        or any(not isinstance(value, str) or not value for value in markers)
        or any(not isinstance(value, str) or not value for value in assets)
    ):
        raise ValueError("readback expectations must be nonempty strings")

    pattern = "|".join(re.escape(marker) for marker in markers)
    deadline = monotonic() + timeout_seconds
    while True:
        try:
            search_payload = await _call_before_deadline(
                lambda: context.search_documents(
                    query=expected_title,
                    num_results=50,
                    offset=0,
                ),
                deadline=deadline,
                monotonic=monotonic,
                timeout_seconds=timeout_seconds,
            )
            if _search_contains_document(
                search_payload,
                document_urn=document_urn,
                title=expected_title,
            ):
                grep_payload = await _call_before_deadline(
                    lambda: context.grep_documents(
                        urns=[document_urn],
                        pattern=pattern,
                        context_chars=2048,
                        max_matches_per_doc=max(5, len(markers) * 2),
                        start_offset=0,
                    ),
                    deadline=deadline,
                    monotonic=monotonic,
                    timeout_seconds=timeout_seconds,
                )
                if _grep_contains_markers(
                    grep_payload,
                    document_urn=document_urn,
                    title=expected_title,
                    markers=markers,
                ):
                    entity_payload = await _call_before_deadline(
                        lambda: context.get_entities(list(assets)),
                        deadline=deadline,
                        monotonic=monotonic,
                        timeout_seconds=timeout_seconds,
                    )
                    if _entities_link_document(
                        entity_payload,
                        asset_urns=assets,
                        document_urn=document_urn,
                    ):
                        return DocumentReadbackProof(
                            document_urn=document_urn,
                            title=expected_title,
                            markers=markers,
                            related_asset_urns=assets,
                        )
        except DataHubMCPError:
            # Search indexing and related-document projections can become
            # available at slightly different times. Retry only until the
            # same strict overall deadline.
            pass

        remaining = deadline - monotonic()
        if remaining <= 0:
            raise _readback_timeout(timeout_seconds)
        await sleep(min(poll_interval_seconds, remaining))


def _receiver_state(payload: object) -> dict[str, object]:
    if not isinstance(payload, dict):
        raise LiveDemoError("demo receiver returned an invalid state")
    if (
        payload.get("dataset_urn") != DEMO_DATASET_URN
        or payload.get("target_urn") != DEMO_JOB_URN
        or payload.get("business_action") != DEMO_BUSINESS_ACTION
        or payload.get("purchase_order_id") != DEMO_PURCHASE_ORDER_ID
        or payload.get("purchase_order_status") not in {"issued", "canceled"}
        or type(payload.get("issue_po_enabled")) is not bool
        or type(payload.get("apply_count")) is not int
        or (
            payload.get("last_incident_id") is not None
            and not isinstance(payload.get("last_incident_id"), str)
        )
        or (
            payload.get("last_receipt_id") is not None
            and not isinstance(payload.get("last_receipt_id"), str)
        )
    ):
        raise LiveDemoError("demo receiver returned an invalid state")
    return payload


async def _request_receiver_state(
    client: httpx.AsyncClient, *, reset: bool
) -> dict[str, object]:
    try:
        response = (
            await client.post(_RESET_URL)
            if reset
            else await client.get(_STATE_URL)
        )
    except httpx.RequestError:
        raise LiveDemoError("demo receiver is unavailable") from None
    if response.status_code != 200:
        raise LiveDemoError("demo receiver rejected the state request")
    try:
        payload = response.json()
    except ValueError:
        raise LiveDemoError("demo receiver returned an invalid state") from None
    return _receiver_state(payload)


def _state_table(title: str, state: Mapping[str, object]) -> Table:
    table = Table(title=title, show_header=True, show_lines=False)
    table.add_column("Signal")
    table.add_column("Value")
    for key in (
        "purchase_order_id",
        "purchase_order_status",
        "issue_po_enabled",
        "apply_count",
        "last_incident_id",
        "last_receipt_id",
    ):
        value = state.get(key)
        table.add_row(key, "—" if value is None else str(value))
    return table


def _validate_before_state(state: Mapping[str, object]) -> None:
    if (
        state.get("purchase_order_id") != DEMO_PURCHASE_ORDER_ID
        or state.get("purchase_order_status") != "issued"
        or state.get("issue_po_enabled") is not True
        or state.get("apply_count") != 0
        or state.get("last_incident_id") is not None
        or state.get("last_receipt_id") is not None
    ):
        raise LiveDemoError("demo receiver did not reset to its baseline")


def _validate_after_state(
    state: Mapping[str, object], *, receipt_id: str
) -> None:
    if (
        state.get("purchase_order_id") != DEMO_PURCHASE_ORDER_ID
        or state.get("purchase_order_status") != "canceled"
        or state.get("issue_po_enabled") is not False
        or state.get("apply_count") != 1
        or state.get("last_incident_id") != DEMO_INCIDENT_ID
        or state.get("last_receipt_id") != receipt_id
    ):
        raise LiveDemoError("demo receiver state does not prove the control")


def _readback_panel(proof: DocumentReadbackProof) -> Panel:
    assets = "\n".join(proof.related_asset_urns)
    markers = "\n".join(proof.markers)
    return Panel.fit(
        f"Document: {proof.document_urn}\n"
        f"Title: {proof.title}\n"
        f"Content markers:\n{markers}\n"
        f"Related-asset backlinks:\n{assets}",
        title="LIVE DATAHUB READBACK VERIFIED",
        border_style="bright_green",
    )


async def _run_live_demo_with_client(
    *,
    console: Console,
    context: DataHubContextPort,
    client: httpx.AsyncClient,
    delay: float,
    readback_timeout_seconds: float,
) -> IncidentReport:
    before = await _request_receiver_state(client, reset=True)
    _validate_before_state(before)
    console.print(_state_table("RECEIVER BEFORE", before))

    grant = RemediationGrant(
        target_urn=DEMO_JOB_URN,
        entity_type="DATA_JOB",
        business_action=DEMO_BUSINESS_ACTION,
        endpoint=DEMO_REMEDIATION_ENDPOINT,
    )
    report = await run_demo(
        console=console,
        context=context,
        http_client=client,
        remediation_grants=(grant,),
        incident_id=DEMO_INCIDENT_ID,
        dataset_urn=DEMO_DATASET_URN,
        delay=delay,
    )

    if (
        len(report.receipts) != 1
        or report.receipts[0].target_urn != DEMO_JOB_URN
        or report.receipts[0].business_action != DEMO_BUSINESS_ACTION
        or report.receipts[0].status != "succeeded"
        or not _is_po_bound_success_receipt(
            report.receipts[0].external_receipt_id
        )
    ):
        raise LiveDemoError(
            "live remediation did not produce PO-bound terminal proof"
        )
    if report.writeback.status != "succeeded" or not report.writeback.document_urn:
        raise LiveDemoError("live DataHub write-back did not succeed")

    receipt_id = report.receipts[0].external_receipt_id
    after = await _request_receiver_state(client, reset=False)
    _validate_after_state(after, receipt_id=receipt_id)
    console.print(_state_table("RECEIVER AFTER", after))

    proof = await wait_for_document_readback(
        context,
        document_urn=report.writeback.document_urn,
        expected_title=f"Aftershock incident {_single_line(DEMO_INCIDENT_ID)}",
        incident_id=DEMO_INCIDENT_ID,
        receipt_ids=(receipt_id,),
        related_asset_urns=(DEMO_DATASET_URN, DEMO_JOB_URN),
        timeout_seconds=readback_timeout_seconds,
    )
    console.print(_readback_panel(proof))
    return report


async def run_live_demo(
    *,
    console: Console | None = None,
    context: DataHubContextPort | None = None,
    http_client: httpx.AsyncClient | None = None,
    delay: float = 1.0,
    readback_timeout_seconds: float = _READBACK_TIMEOUT_SECONDS,
) -> IncidentReport:
    """Run the exact seeded live workflow and require independent proof."""

    active_context = context or build_datahub_context_from_env()
    if active_context.mode != "mcp":
        raise LiveDemoError(
            "live demo requires AFTERSHOCK_DATAHUB_MODE=mcp"
        )
    active_console = console or Console()
    if http_client is not None:
        return await _run_live_demo_with_client(
            console=active_console,
            context=active_context,
            client=http_client,
            delay=delay,
            readback_timeout_seconds=readback_timeout_seconds,
        )

    async with httpx.AsyncClient(timeout=_HTTP_TIMEOUT_SECONDS) as client:
        return await _run_live_demo_with_client(
            console=active_console,
            context=active_context,
            client=client,
            delay=delay,
            readback_timeout_seconds=readback_timeout_seconds,
        )


def main() -> None:
    """Run the live proof gate as a command-line demonstration."""

    try:
        asyncio.run(run_live_demo())
    except LiveDemoError as error:
        Console(stderr=True).print(f"[bold red]LIVE DEMO FAILED:[/] {error}")
        raise SystemExit(1) from None


if __name__ == "__main__":
    main()
