"""Create the deterministic DataHub assets used by Aftershock's live MCP demo."""

from __future__ import annotations

import argparse
import ipaddress
import json
import os
import subprocess
import warnings
from collections.abc import Callable, Mapping, Sequence
from pathlib import Path
from typing import Any, Literal
from urllib.parse import urlsplit

from datahub.errors import ExperimentalWarning, ItemNotFoundError
from datahub.ingestion.graph.config import DatahubClientConfig

with warnings.catch_warnings():
    warnings.simplefilter("ignore", ExperimentalWarning)
    from datahub.sdk import DataFlow, DataHubClient, DataJob, Dataset


Mode = Literal["dry-run", "apply"]
ClientFactory = Callable[..., Any]
DefinitionsRunner = Callable[[tuple[str, ...], dict[str, str]], None]

DEFINITION_CLI_TIMEOUT_SECONDS = 30.0
SDK_REQUEST_TIMEOUT_SECONDS = 10.0
SDK_RETRY_MAX_TIMES = 1
SDK_RETRY_STATUS_CODES = [429, 500, 502, 503, 504]

ROOT = Path(__file__).resolve().parents[1]
PROPERTY_FILE = ROOT / "config" / "aftershock_structured_properties.yaml"
PROPERTY_FILE_DISPLAY = "config/aftershock_structured_properties.yaml"

DATASET_URN = (
    "urn:li:dataset:(urn:li:dataPlatform:postgres,"
    "aftershock_demo.inventory_pricing,DEV)"
)
FLOW_URN = "urn:li:dataFlow:(airflow,aftershock_demo,DEV)"
JOB_URN = (
    "urn:li:dataJob:(urn:li:dataFlow:(airflow,aftershock_demo,DEV),"
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


class BootstrapError(RuntimeError):
    """A controlled bootstrap failure that never contains third-party detail."""


def build_demo_assets() -> tuple[Dataset, DataFlow, DataJob]:
    """Build the three deterministic assets without making a network call."""

    dataset = Dataset(
        platform="postgres",
        name="aftershock_demo.inventory_pricing",
        env="DEV",
        display_name="Inventory Pricing",
        description="Synthetic upstream pricing data for the Aftershock demo.",
    )
    flow = DataFlow(
        platform="airflow",
        name="aftershock_demo",
        env="DEV",
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


def _canonical_target(raw_url: str) -> tuple[str, str]:
    """Return a secret-free canonical origin and normalized host."""

    try:
        parsed = urlsplit(raw_url)
        scheme = parsed.scheme.lower()
        if (
            scheme not in {"http", "https"}
            or not parsed.hostname
            or parsed.username is not None
            or parsed.password is not None
            or "#" in raw_url
        ):
            raise ValueError
        host = parsed.hostname.lower()
        port = parsed.port
    except (TypeError, ValueError):
        raise ValueError("DATAHUB_GMS_URL is invalid or unsafe") from None

    if any(character.isspace() for character in host):
        raise ValueError("DATAHUB_GMS_URL is invalid or unsafe")
    if port is None:
        port = 80 if scheme == "http" else 443
    rendered_host = f"[{host}]" if ":" in host else host
    return f"{scheme}://{rendered_host}:{port}", host


def _is_loopback_host(host: str) -> bool:
    if host == "localhost":
        return True
    try:
        return ipaddress.ip_address(host).is_loopback
    except ValueError:
        return False


def _dry_run_target_origin(source: Mapping[str, str]) -> str:
    configured = source.get("DATAHUB_GMS_URL", "").strip()
    if not configured:
        return "not configured"
    try:
        origin, _ = _canonical_target(configured)
    except ValueError:
        return "invalid or unsafe configuration"
    return origin


def _plan(mode: Mode, *, target_origin: str) -> dict[str, object]:
    return {
        "mode": mode,
        "target_origin": target_origin,
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
            timeout=DEFINITION_CLI_TIMEOUT_SECONDS,
        )
    except (OSError, subprocess.TimeoutExpired):
        raise BootstrapError(
            "structured-property definition command could not be started"
        ) from None
    if completed.returncode != 0:
        raise BootstrapError(
            "structured-property definition upsert failed"
        )


def execute_bootstrap(
    *,
    mode: Mode,
    environ: Mapping[str, str] | None = None,
    confirm_target: str | None = None,
    allow_remote_target: bool = False,
    allow_existing_demo_assets: bool = False,
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
        return _plan(mode, target_origin=_dry_run_target_origin(source))

    gms_url = source.get("DATAHUB_GMS_URL", "").strip()
    if not gms_url:
        raise ValueError("DATAHUB_GMS_URL must be set for --apply")

    try:
        target_origin, target_host = _canonical_target(gms_url)
    except ValueError:
        raise ValueError("DATAHUB_GMS_URL is invalid or unsafe") from None
    if confirm_target != target_origin:
        raise ValueError("target confirmation does not match DATAHUB_GMS_URL origin")
    target_is_loopback = _is_loopback_host(target_host)
    if not target_is_loopback and target_origin.startswith("http://"):
        raise ValueError("remote DataHub target requires HTTPS")
    if not target_is_loopback and not allow_remote_target:
        raise ValueError(
            "remote DataHub target requires --allow-remote-target"
        )

    try:
        client_config = DatahubClientConfig(
            server=gms_url,
            token=source.get("DATAHUB_GMS_TOKEN"),
            timeout_sec=SDK_REQUEST_TIMEOUT_SECONDS,
            retry_max_times=SDK_RETRY_MAX_TIMES,
            retry_status_codes=list(SDK_RETRY_STATUS_CODES),
        )
        client = client_factory(config=client_config)
    except Exception:
        raise BootstrapError("DataHub client creation failed") from None

    collision_found = False
    for urn in (DATASET_URN, FLOW_URN, JOB_URN):
        try:
            client.entities.get(urn)
        except ItemNotFoundError:
            continue
        except Exception:
            raise BootstrapError(
                "DataHub demo asset collision preflight failed"
            ) from None
        collision_found = True
    if collision_found and not allow_existing_demo_assets:
        raise BootstrapError("DataHub demo asset collision detected")

    try:
        definitions_runner(
            _definition_command(absolute_path=True),
            source,
        )
    except Exception:
        raise BootstrapError(
            "structured-property definition upsert failed"
        ) from None

    try:
        dataset, flow, job = build_demo_assets()
    except Exception:
        raise BootstrapError("DataHub demo asset construction failed") from None

    for label, entity in (
        ("Dataset", dataset),
        ("DataFlow", flow),
        ("DataJob", job),
    ):
        try:
            client.entities.upsert(entity)
        except Exception:
            raise BootstrapError(f"DataHub {label} upsert failed") from None

    try:
        client.lineage.add_lineage(
            upstream=DATASET_URN,
            downstream=JOB_URN,
        )
    except Exception:
        raise BootstrapError("DataHub lineage creation failed") from None
    return _plan(mode, target_origin=target_origin)


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
    parser.add_argument(
        "--confirm-target",
        help=(
            "for --apply, the exact canonical origin printed by --dry-run "
            "(including port)"
        ),
    )
    parser.add_argument(
        "--allow-remote-target",
        action="store_true",
        help="allow --apply to a confirmed non-loopback DataHub origin",
    )
    parser.add_argument(
        "--allow-existing-demo-assets",
        action="store_true",
        help="allow a deliberate idempotent rerun over the exact demo URNs",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    mode: Mode = "apply" if args.apply else "dry-run"
    try:
        result = execute_bootstrap(
            mode=mode,
            confirm_target=args.confirm_target,
            allow_remote_target=args.allow_remote_target,
            allow_existing_demo_assets=args.allow_existing_demo_assets,
        )
    except (BootstrapError, ValueError) as exc:
        parser.error(str(exc))
    except Exception:
        parser.error("DataHub bootstrap failed")
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
