"""Truthful Rich dashboard for the complete Aftershock incident workflow."""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from datetime import datetime

import httpx
from rich.console import Console
from rich.markup import escape
from rich.panel import Panel
from rich.progress import BarColumn, Progress, SpinnerColumn, TextColumn
from rich.table import Table
from rich.tree import Tree

from compensating_action_engine import CompensatingActionEngine
from datahub_context import DataHubContextPort, FixtureDataHubContext
from incident_processor import AftershockIncidentProcessor
from remediation_models import IncidentReport


DEMO_DATASET_URN = (
    "urn:li:dataset:(urn:li:dataPlatform:postgres,inventory_pricing,PROD)"
)
DEMO_INCIDENT_ID = "INC-9942"
Clock = Callable[[], datetime]


def _safe(value: object | None) -> str:
    return escape("—" if value is None else str(value))


def _build_blast_radius_tree(report: IncidentReport) -> Tree:
    tree = Tree(
        f"[bold red]{_safe(report.dataset_urn)}[/] [red](CORRUPTED SOURCE)[/]",
        guide_style="bold red",
    )
    for receipt in report.receipts:
        branch = tree.add(
            f"[bold yellow]{_safe(receipt.entity_type)}[/] "
            f"[white]{_safe(receipt.target_urn)}[/]"
        )
        branch.add(f"Business action: [magenta]{_safe(receipt.business_action)}[/]")
    return tree


def _receipt_table(report: IncidentReport) -> Table:
    table = Table(title="Compensating-control receipts", show_lines=True)
    table.add_column("Type")
    table.add_column("Target / action")
    table.add_column("Status")
    table.add_column("HTTP")
    table.add_column("Endpoint")
    for receipt in report.receipts:
        table.add_row(
            _safe(receipt.entity_type),
            f"{_safe(receipt.target_urn)}\n{_safe(receipt.business_action)}",
            _safe(receipt.status),
            _safe(receipt.http_status),
            _safe(receipt.endpoint),
        )
    return table


def _is_complete(report: IncidentReport) -> bool:
    return (
        all(receipt.status == "succeeded" for receipt in report.receipts)
        and report.writeback.status == "succeeded"
    )


async def _pause(delay: float) -> None:
    if delay > 0:
        await asyncio.sleep(delay)


async def run_demo(
    *,
    console: Console | None = None,
    context: DataHubContextPort | None = None,
    http_client: httpx.AsyncClient | None = None,
    clock: Clock | None = None,
    delay: float = 1.5,
) -> IncidentReport:
    """Render a receipt-driven workflow and return the exact displayed report."""

    active_console = console or Console()
    active_context = context or FixtureDataHubContext()
    fixture_mode = active_context.mode == "fixture"
    if not fixture_mode and http_client is None:
        raise ValueError("http_client is required for non-fixture context")

    if fixture_mode:
        active_console.print(
            Panel.fit(
                "[bold white]OFFLINE FIXTURE MODE[/]\n"
                "Lineage responses, remediation endpoints, and document "
                "write-back use deterministic local test doubles.",
                border_style="bright_yellow",
            )
        )
    else:
        active_console.print(
            Panel.fit(
                "[bold white]MCP MODE[/]\n"
                "Results shown below are the receipts returned by the "
                "configured services.",
                border_style="bright_cyan",
            )
        )

    active_console.print(
        Panel.fit(
            "[bold white]CRITICAL INCIDENT DETECTED[/]\n\n"
            '"[bold bright_red]Pricing Decimal Shift[/]" identified in '
            "[bold cyan]inventory_pricing[/].\n"
            "[yellow]Aftershock normalized incident envelope received.[/]",
            title="[bold red]ACT 1  //  THE FAULT[/]",
            border_style="bright_red",
            padding=(1, 3),
        )
    )
    await _pause(delay)

    qualifier = " (fixture response)" if fixture_mode else " (configured MCP)"
    active_console.print(
        "[bold yellow]ACT 2  //  DATAHUB CONTEXT AND BLAST RADIUS[/]\n"
        f"Context read contract: [bold]get_lineage(upstream=false)[/]"
        f"{qualifier}"
    )

    async def process(client: httpx.AsyncClient) -> IncidentReport:
        processor = AftershockIncidentProcessor(
            active_context,
            CompensatingActionEngine(http_client=client),
            clock=clock,
        )
        with Progress(
            SpinnerColumn(style="bold bright_cyan"),
            TextColumn("[bold bright_cyan]{task.description}"),
            BarColumn(bar_width=36, complete_style="bright_green"),
            console=active_console,
            transient=False,
        ) as progress:
            task_id = progress.add_task("Executing incident processor...", total=1)
            report = await processor.process(DEMO_INCIDENT_ID, DEMO_DATASET_URN)
            progress.update(task_id, completed=1)
        return report

    if http_client is not None:
        report = await process(http_client)
    else:

        async def fixture_remediation(_: httpx.Request) -> httpx.Response:
            await asyncio.sleep(0.05)
            return httpx.Response(200, json={"accepted": True})

        async with httpx.AsyncClient(
            transport=httpx.MockTransport(fixture_remediation), timeout=10.0
        ) as owned_client:
            report = await process(owned_client)

    active_console.print(
        Panel(
            _build_blast_radius_tree(report),
            subtitle="[dim]Typed downstream entities returned by the workflow[/]",
            border_style="yellow",
        )
    )
    await _pause(delay)

    active_console.print("[bold bright_cyan]ACT 3  //  CONTROL RECEIPTS[/]")
    active_console.print(_receipt_table(report))
    for index, receipt in enumerate(report.receipts, start=1):
        active_console.print(
            f"Receipt {index} endpoint: {_safe(receipt.endpoint)}"
        )
    await _pause(delay)

    writeback_note = (
        " (in-memory fixture recorder)" if fixture_mode else " (MCP save_document receipt)"
    )
    active_console.print(
        f"Write-back status: [bold]{_safe(report.writeback.status)}[/]"
        f"{writeback_note}\n"
        f"Saved document URN: {_safe(report.writeback.document_urn)}"
    )

    if _is_complete(report):
        resolution = (
            "[bold white]COMPLETED: all discovered controls succeeded and the "
            "incident record was saved.[/]"
        )
        border = "bright_green"
    else:
        resolution = (
            "[bold white]COMPLETED WITH ISSUES: inspect the control and "
            "write-back receipts above.[/]"
        )
        border = "bright_yellow"
    active_console.print(
        Panel.fit(
            resolution,
            title="[bold]ACT 4  //  RESOLUTION[/]",
            border_style=border,
            padding=(1, 3),
        )
    )
    return report


if __name__ == "__main__":
    asyncio.run(run_demo())
