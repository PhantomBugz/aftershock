"""Contracts for the deterministic DataHub demo bootstrap."""

from __future__ import annotations

import importlib.util
import json
import os
from pathlib import Path
import subprocess
import sys
from typing import Any

import pytest

from datahub.api.entities.structuredproperties.structuredproperties import (
    StructuredProperties,
)
from datahub.sdk import DataFlow, DataJob, Dataset


ROOT = Path(__file__).resolve().parents[1]
PROPERTY_FILE = ROOT / "config" / "aftershock_structured_properties.yaml"
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
EXPECTED_ENTITY_TYPES = [
    "urn:li:entityType:datahub.dataJob",
    "urn:li:entityType:datahub.mlModel",
]


def _bootstrap_module():
    module_path = ROOT / "scripts" / "bootstrap_datahub_demo.py"
    if not module_path.is_file():
        pytest.fail("scripts.bootstrap_datahub_demo has not been implemented")
    spec = importlib.util.spec_from_file_location(
        "aftershock_bootstrap_datahub_demo", module_path
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _assignments(job: DataJob) -> dict[str, list[str]]:
    return {
        assignment.propertyUrn: list(assignment.values)
        for assignment in job.structured_properties or []
    }


def test_structured_property_yaml_validates_with_exact_normalized_contract() -> None:
    assert PROPERTY_FILE.is_file(), "structured-property definition file is missing"

    definitions = StructuredProperties.from_yaml(str(PROPERTY_FILE))

    assert [definition.fqn for definition in definitions] == [
        "aftershock.businessAction",
        "aftershock.remediationWebhook",
    ]
    assert [definition.type for definition in definitions] == [
        "urn:li:dataType:datahub.string",
        "urn:li:dataType:datahub.string",
    ]
    assert [definition.cardinality for definition in definitions] == [
        "SINGLE",
        "SINGLE",
    ]
    assert [definition.entity_types for definition in definitions] == [
        EXPECTED_ENTITY_TYPES,
        EXPECTED_ENTITY_TYPES,
    ]
    assert all(
        "urn:li:entityType:datahub.dataset" not in (definition.entity_types or [])
        for definition in definitions
    )


def test_demo_assets_have_deterministic_urns_and_job_playbook_values() -> None:
    bootstrap = _bootstrap_module()

    dataset, flow, job = bootstrap.build_demo_assets()

    assert isinstance(dataset, Dataset)
    assert isinstance(flow, DataFlow)
    assert isinstance(job, DataJob)
    assert str(dataset.urn) == DATASET_URN
    assert str(flow.urn) == FLOW_URN
    assert str(job.urn) == JOB_URN
    assert _assignments(job) == {
        BUSINESS_ACTION_PROPERTY_URN: ["ISSUE_PO"],
        REMEDIATION_WEBHOOK_PROPERTY_URN: [
            "http://127.0.0.1:8765/remediate/cancel_po"
        ],
    }
    assert "mlModel" not in " ".join(
        [str(dataset.urn), str(flow.urn), str(job.urn)]
    )


def test_dry_run_is_deterministic_and_never_constructs_a_client_or_leaks_token() -> None:
    bootstrap = _bootstrap_module()
    calls: list[dict[str, Any]] = []

    def forbidden_factory(**kwargs: Any) -> object:
        calls.append(kwargs)
        raise AssertionError("dry-run constructed a DataHub client")

    first = bootstrap.execute_bootstrap(
        mode="dry-run",
        environ={"DATAHUB_GMS_TOKEN": "TOP-SECRET-TOKEN"},
        client_factory=forbidden_factory,
    )
    second = bootstrap.execute_bootstrap(
        mode="dry-run",
        environ={"DATAHUB_GMS_TOKEN": "A-DIFFERENT-SECRET"},
        client_factory=forbidden_factory,
    )

    assert first == second
    assert first == {
        "mode": "dry-run",
        "definitions_command": [
            "datahub",
            "properties",
            "upsert",
            "-f",
            "config/aftershock_structured_properties.yaml",
        ],
        "assets": [DATASET_URN, FLOW_URN, JOB_URN],
        "lineage": {"upstream": DATASET_URN, "downstream": JOB_URN},
        "remediation_endpoint": (
            "http://127.0.0.1:8765/remediate/cancel_po"
        ),
    }
    serialized = json.dumps(first, sort_keys=True)
    assert "TOP-SECRET-TOKEN" not in serialized
    assert "A-DIFFERENT-SECRET" not in serialized
    assert calls == []


def test_apply_requires_nonblank_gms_url_before_definitions_or_client() -> None:
    bootstrap = _bootstrap_module()
    events: list[str] = []

    def client_factory(**kwargs: Any) -> object:
        events.append("client")
        return object()

    def definitions_runner(*args: Any, **kwargs: Any) -> None:
        events.append("definitions")

    with pytest.raises(ValueError, match="DATAHUB_GMS_URL"):
        bootstrap.execute_bootstrap(
            mode="apply",
            environ={"DATAHUB_GMS_URL": "   "},
            client_factory=client_factory,
            definitions_runner=definitions_runner,
        )

    assert events == []


def test_apply_defines_properties_then_upserts_three_assets_and_one_exact_edge() -> None:
    bootstrap = _bootstrap_module()
    events: list[tuple[str, Any]] = []

    class Entities:
        def upsert(self, entity: object) -> None:
            events.append(("upsert", entity))

    class Lineage:
        def add_lineage(self, **kwargs: object) -> None:
            events.append(("lineage", kwargs))

    class Client:
        entities = Entities()
        lineage = Lineage()

    def client_factory(**kwargs: Any) -> Client:
        events.append(("client", kwargs))
        return Client()

    def definitions_runner(
        command: tuple[str, ...], environ: dict[str, str]
    ) -> None:
        events.append(
            (
                "definitions",
                {"command": command, "url": environ.get("DATAHUB_GMS_URL")},
            )
        )

    result = bootstrap.execute_bootstrap(
        mode="apply",
        environ={
            "DATAHUB_GMS_URL": "http://localhost:8080",
            "DATAHUB_GMS_TOKEN": "DO-NOT-PRINT-ME",
        },
        client_factory=client_factory,
        definitions_runner=definitions_runner,
    )

    assert [event[0] for event in events] == [
        "definitions",
        "client",
        "upsert",
        "upsert",
        "upsert",
        "lineage",
    ]
    assert events[0][1] == {
        "command": (
            "datahub",
            "properties",
            "upsert",
            "-f",
            str(PROPERTY_FILE),
        ),
        "url": "http://localhost:8080",
    }
    assert events[1][1] == {
        "server": "http://localhost:8080",
        "token": "DO-NOT-PRINT-ME",
    }

    upserted = [event[1] for event in events if event[0] == "upsert"]
    assert [type(entity) for entity in upserted] == [Dataset, DataFlow, DataJob]
    assert [str(entity.urn) for entity in upserted] == [
        DATASET_URN,
        FLOW_URN,
        JOB_URN,
    ]
    assert _assignments(upserted[2]) == {
        BUSINESS_ACTION_PROPERTY_URN: ["ISSUE_PO"],
        REMEDIATION_WEBHOOK_PROPERTY_URN: [
            "http://127.0.0.1:8765/remediate/cancel_po"
        ],
    }
    assert events[-1][1] == {
        "upstream": DATASET_URN,
        "downstream": JOB_URN,
    }
    serialized = json.dumps(result, sort_keys=True)
    assert "DO-NOT-PRINT-ME" not in serialized
    assert "mlModel" not in serialized


def test_cli_requires_exactly_one_mode() -> None:
    bootstrap = _bootstrap_module()
    parser = bootstrap.build_parser()

    with pytest.raises(SystemExit):
        parser.parse_args([])
    with pytest.raises(SystemExit):
        parser.parse_args(["--dry-run", "--apply"])

    assert parser.parse_args(["--dry-run"]).dry_run is True
    assert parser.parse_args(["--apply"]).apply is True


def test_cli_dry_run_emits_only_deterministic_json() -> None:
    completed = subprocess.run(
        [
            sys.executable,
            str(ROOT / "scripts" / "bootstrap_datahub_demo.py"),
            "--dry-run",
        ],
        check=False,
        capture_output=True,
        text=True,
        env={**os.environ, "DATAHUB_GMS_TOKEN": "CLI-SECRET"},
    )

    assert completed.returncode == 0
    assert completed.stderr == ""
    payload = json.loads(completed.stdout)
    assert payload["mode"] == "dry-run"
    assert payload["assets"] == [DATASET_URN, FLOW_URN, JOB_URN]
    assert "CLI-SECRET" not in completed.stdout


def test_dependency_pins_and_live_marker_are_reproducible() -> None:
    pyproject = (ROOT / "pyproject.toml").read_text(encoding="utf-8")
    requirements = (ROOT / "requirements.txt").read_text(encoding="utf-8")

    for content in (pyproject, requirements):
        assert "acryl-datahub==1.6.0.17" in content
        assert "mcp-server-datahub==0.6.0" in content
        assert "fastmcp==3.4.5" in content
    assert "live_datahub" in pyproject
