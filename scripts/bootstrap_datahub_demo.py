"""Create the deterministic DataHub assets used by Aftershock's live MCP demo."""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import warnings
from collections.abc import Callable, Mapping, Sequence
from pathlib import Path
from typing import Any, Literal

from datahub.errors import ExperimentalWarning

with warnings.catch_warnings():
    warnings.simplefilter("ignore", ExperimentalWarning)
    from datahub.sdk import DataFlow, DataHubClient, DataJob, Dataset


Mode = Literal["dry-run", "apply"]
ClientFactory = Callable[..., Any]
DefinitionsRunner = Callable[[tuple[str, ...], dict[str, str]], None]

ROOT = Path(__file__).resolve().parents[1]
PROPERTY_FILE = ROOT / "config" / "aftershock_structured_properties.yaml"
PROPERTY_FILE_DISPLAY = "config/aftershock_structured_properties.yaml"

DATASET_URN = "urn:li:dataset:(urn:li:dataPlatform:postgres,inventory_pricing,PROD)"
FLOW_URN = "urn:li:dataFlow:(airflow,aftershock_demo,PROD)"
JOB_URN = (
    "urn:li:dataJob:(urn:li:dataFlow:(airflow,aftershock_demo,PROD),"
    "purchase_order_generator)"
)
BUSINESS_ACTION_PROPERTY_URN = (
    "urn:li:structuredProperty:aftershock.businessAction"
)
REMEDIATION_WEBHOOK_PROPERTY_URN = (
    "urn:li:structuredProperty:aftershock.remediationWebhook"
)

# This loopback URL is deliberately safe for setup. It becomes callable only
# if the operator separately starts a local demo remediation receiver.
DEMO_REMEDIATION_URL = "http://127.0.0.1:8765/remediate/cancel_po"


def build_demo_assets() -> tuple[Dataset, DataFlow, DataJob]:
    """Build the three deterministic assets without making a network call."""

    dataset = Dataset(
        platform="postgres",
        name="inventory_pricing",
        env="PROD",
        display_name="Inventory Pricing",
        description="Synthetic upstream pricing data for the Aftershock demo.",
    )
    flow = DataFlow(
        platform="airflow",
        name="aftershock_demo",
        env="PROD",
        display_name="Aftershock Demo",
        description="Synthetic flow containing the Aftershock demo DataJob.",
    )
    job = DataJob(
        name="purchase_order_generator",
        flow=flow,
        display_name="Purchase Order Generator",
        description="Synthetic downstream job used by the Aftershock demo.",
        structured_properties={
            BUSINESS_ACTION_PROPERTY_URN: ["ISSUE_PO"],
            REMEDIATION_WEBHOOK_PROPERTY_URN: [DEMO_REMEDIATION_URL],
        },
    )

    actual_urns = (str(dataset.urn), str(flow.urn), str(job.urn))
    expected_urns = (DATASET_URN, FLOW_URN, JOB_URN)
    if actual_urns != expected_urns:
        raise RuntimeError("DataHub SDK generated unexpected demo asset URNs")
    return dataset, flow, job


def _definition_command(*, absolute_path: bool) -> tuple[str, ...]:
    path = str(PROPERTY_FILE) if absolute_path else PROPERTY_FILE_DISPLAY
    return ("datahub", "properties", "upsert", "-f", path)


def _plan(mode: Mode) -> dict[str, object]:
    return {
        "mode": mode,
        "definitions_command": list(_definition_command(absolute_path=False)),
        "assets": [DATASET_URN, FLOW_URN, JOB_URN],
        "lineage": {"upstream": DATASET_URN, "downstream": JOB_URN},
        "remediation_endpoint": DEMO_REMEDIATION_URL,
    }


def _run_definitions(
    command: tuple[str, ...], environ: dict[str, str]
) -> None:
    """Apply definitions with the official CLI while suppressing secret-bearing output."""

    try:
        completed = subprocess.run(
            list(command),
            check=False,
            capture_output=True,
            text=True,
            env=environ,
        )
    except OSError:
        raise RuntimeError(
            "structured-property definition command could not be started"
        ) from None
    if completed.returncode != 0:
        raise RuntimeError(
            "structured-property definition upsert failed"
        )


def execute_bootstrap(
    *,
    mode: Mode,
    environ: Mapping[str, str] | None = None,
    client_factory: ClientFactory = DataHubClient,
    definitions_runner: DefinitionsRunner = _run_definitions,
) -> dict[str, object]:
    """Plan or apply the reproducible assets without ever returning secrets."""

    if mode not in {"dry-run", "apply"}:
        raise ValueError("mode must be 'dry-run' or 'apply'")

    source = dict(os.environ if environ is None else environ)
    if mode == "dry-run":
        # Constructing SDK entity objects is local and deterministic, but is
        # intentionally unnecessary here: dry-run performs no client work.
        return _plan(mode)

    gms_url = source.get("DATAHUB_GMS_URL", "").strip()
    if not gms_url:
        raise ValueError("DATAHUB_GMS_URL must be set for --apply")

    definitions_runner(
        _definition_command(absolute_path=True),
        source,
    )
    client = client_factory(
        server=gms_url,
        token=source.get("DATAHUB_GMS_TOKEN"),
    )
    dataset, flow, job = build_demo_assets()
    for entity in (dataset, flow, job):
        client.entities.upsert(entity)
    client.lineage.add_lineage(
        upstream=DATASET_URN,
        downstream=JOB_URN,
    )
    return _plan(mode)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Plan or apply the deterministic DataHub assets for the "
            "Aftershock MCP demo."
        )
    )
    modes = parser.add_mutually_exclusive_group(required=True)
    modes.add_argument(
        "--dry-run",
        action="store_true",
        help="print the deterministic plan without creating a client",
    )
    modes.add_argument(
        "--apply",
        action="store_true",
        help="apply definitions, assets, and Dataset-to-DataJob lineage",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    mode: Mode = "apply" if args.apply else "dry-run"
    try:
        result = execute_bootstrap(mode=mode)
    except (RuntimeError, ValueError) as exc:
        parser.error(str(exc))
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
