"""Rich terminal showpiece for the complete Aftershock incident lifecycle."""

from __future__ import annotations

import asyncio
from collections.abc import Mapping, Sequence
from typing import Any

import httpx
from rich.console import Console
from rich.panel import Panel
from rich.progress import BarColumn, Progress, SpinnerColumn, TextColumn
from rich.tree import Tree

from blast_radius_mapper import BlastRadiusMapper
from compensating_action_engine import CompensatingActionEngine


DEMO_DATASET_URN = (
    "urn:li:dataset:(urn:li:dataPlatform:postgres,inventory_pricing,PROD)"
)
DEMO_INCIDENT_ID = "INC-9942"


def _build_blast_radius_tree(
    downstream_entities: Sequence[Mapping[str, Any]],
) -> Tree:
    tree = Tree(
        "[bold red]inventory_pricing[/] [red](CORRUPTED SOURCE)[/]",
        guide_style="bold red",
    )

    labels = {
        "DATA_JOB": ("Airflow Job", "purchase_order_generator"),
        "ML_MODEL": ("SageMaker Model", "dynamic_pricing_model"),
    }
    for wrapped_entity in downstream_entities:
        entity = wrapped_entity.get("entity", {})
        if not isinstance(entity, Mapping):
            continue
        entity_type = str(entity.get("type", "UNKNOWN"))
        system_label, fallback_name = labels.get(
            entity_type, (entity_type.replace("_", " ").title(), "downstream_system")
        )
        custom_properties = entity.get("customProperties", {})
        action = (
            custom_properties.get("business_action", "UNDEFINED")
            if isinstance(custom_properties, Mapping)
            else "UNDEFINED"
        )
        tree.add(
            f"[bold yellow]{system_label}[/] [white]{fallback_name}[/]\n"
            f"[dim]Action:[/] [bold bright_magenta]{action}[/]"
        )

    return tree


async def run_demo(
    *,
    console: Console | None = None,
    http_client: httpx.AsyncClient | None = None,
    delay: float = 1.5,
) -> list[bool]:
    """Render four acts while executing the real compensating-action engine."""

    active_console = console or Console()
    mapper = BlastRadiusMapper(gms_url="")
    downstream_entities = await mapper.get_actionable_targets(DEMO_DATASET_URN)

    active_console.print(
        Panel.fit(
            "[bold white]CRITICAL INCIDENT DETECTED[/]\n\n"
            '"[bold bright_red]Pricing Decimal Shift[/]" identified in '
            "[bold cyan]inventory_pricing[/].\n"
            "[yellow]Webhook intercepted.[/]",
            title="[bold red]ACT 1  //  THE FAULT[/]",
            border_style="bright_red",
            padding=(1, 3),
        )
    )
    await asyncio.sleep(delay)

    active_console.print(
        Panel(
            _build_blast_radius_tree(downstream_entities),
            title="[bold yellow]ACT 2  //  BLAST RADIUS[/]",
            subtitle="[dim]DataHub downstream lineage resolved[/]",
            border_style="yellow",
            padding=(1, 2),
        )
    )
    await asyncio.sleep(delay)

    async def execute_controls(client: httpx.AsyncClient) -> list[bool]:
        engine = CompensatingActionEngine(http_client=client)
        with Progress(
            SpinnerColumn(style="bold bright_cyan"),
            TextColumn("[bold bright_cyan]{task.description}"),
            BarColumn(bar_width=36, complete_style="bright_green"),
            console=active_console,
            transient=False,
        ) as progress:
            task_id = progress.add_task(
                "Executing Compensating Controls...", total=1
            )
            results = await engine.process_blast_radius(
                downstream_entities, incident_id=DEMO_INCIDENT_ID
            )
            progress.update(task_id, completed=1)
        return results

    active_console.print(
        "\n[bold bright_cyan]ACT 3  //  THE AFTERSHOCK[/]",
        justify="left",
    )
    if http_client is not None:
        results = await execute_controls(http_client)
    else:

        async def remediation_service(request: httpx.Request) -> httpx.Response:
            await asyncio.sleep(0.6)
            active_console.print(
                f"[green]HTTP 200[/]  POST [bold]{request.url.path}[/]"
            )
            return httpx.Response(200, json={"status": "accepted"})

        transport = httpx.MockTransport(remediation_service)
        async with httpx.AsyncClient(transport=transport, timeout=10.0) as client:
            results = await execute_controls(client)

    await asyncio.sleep(delay)
    active_console.print(
        Panel.fit(
            "[bold white]SUCCESS: Action Debt Neutralized.[/]\n\n"
            "[bold bright_green]$120,000 in erroneous orders reversed.[/]\n"
            "[white]Enterprise State Restored.[/]",
            title="[bold bright_green]ACT 4  //  RESOLUTION[/]",
            border_style="bright_green",
            padding=(1, 4),
        )
    )
    return results


if __name__ == "__main__":
    asyncio.run(run_demo())
