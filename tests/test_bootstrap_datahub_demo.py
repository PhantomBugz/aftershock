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
from datahub.errors import ItemNotFoundError
from datahub.ingestion.graph.config import DatahubClientConfig
from datahub.sdk import DataFlow, DataJob, Dataset


ROOT = Path(__file__).resolve().parents[1]
PROPERTY_FILE = ROOT / "config" / "aftershock_structured_properties.yaml"
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
        "target_origin": "not configured",
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


@pytest.mark.parametrize(
    ("configured_url", "expected_origin"),
    [
        ("http://LOCALHOST/path?token=QUERY-SECRET", "http://localhost:80"),
        ("https://Example.COM:8443/api/graphql?token=QUERY-SECRET", "https://example.com:8443"),
        ("http://[::1]/api", "http://[::1]:80"),
    ],
)
def test_dry_run_reports_only_canonical_target_origin(
    configured_url: str,
    expected_origin: str,
) -> None:
    bootstrap = _bootstrap_module()

    result = bootstrap.execute_bootstrap(
        mode="dry-run",
        environ={
            "DATAHUB_GMS_URL": configured_url,
            "DATAHUB_GMS_TOKEN": "HEADER-SECRET",
        },
        client_factory=lambda **kwargs: pytest.fail(
            "dry-run must not construct a client"
        ),
    )

    assert result["target_origin"] == expected_origin
    serialized = json.dumps(result, sort_keys=True)
    assert "QUERY-SECRET" not in serialized
    assert "HEADER-SECRET" not in serialized
    assert "/path" not in serialized
    assert "/api/graphql" not in serialized


def test_dry_run_never_echoes_unsafe_userinfo_or_fragment() -> None:
    bootstrap = _bootstrap_module()

    for configured_url, secret in (
        ("https://username:USERINFO-SECRET@example.com:8443/path", "USERINFO-SECRET"),
        ("https://example.com:8443/path#FRAGMENT-SECRET", "FRAGMENT-SECRET"),
    ):
        result = bootstrap.execute_bootstrap(
            mode="dry-run",
            environ={"DATAHUB_GMS_URL": configured_url},
            client_factory=lambda **kwargs: pytest.fail(
                "dry-run must not construct a client"
            ),
        )
        assert result["target_origin"] == "invalid or unsafe configuration"
        assert secret not in json.dumps(result, sort_keys=True)


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
            confirm_target="http://localhost:8080",
            client_factory=client_factory,
            definitions_runner=definitions_runner,
        )

    assert events == []


@pytest.mark.parametrize(
    "configured_url",
    [
        "ftp://localhost:8080",
        "http://operator:secret@localhost:8080",
        "http://localhost:8080/#secret-fragment",
        "http://localhost:8080/path#",
        "http:///missing-host",
    ],
)
def test_apply_rejects_unsafe_target_before_definitions_or_client(
    configured_url: str,
) -> None:
    bootstrap = _bootstrap_module()
    events: list[str] = []

    with pytest.raises(ValueError, match="DATAHUB_GMS_URL is invalid or unsafe"):
        bootstrap.execute_bootstrap(
            mode="apply",
            environ={"DATAHUB_GMS_URL": configured_url},
            confirm_target="http://localhost:8080",
            client_factory=lambda **kwargs: events.append("client"),
            definitions_runner=lambda *args, **kwargs: events.append("definitions"),
        )

    assert events == []


def test_apply_requires_exact_canonical_target_confirmation_before_any_work() -> None:
    bootstrap = _bootstrap_module()
    events: list[str] = []

    for confirmation in (None, "http://localhost:80", "http://LOCALHOST:8080"):
        with pytest.raises(ValueError, match="target confirmation does not match"):
            bootstrap.execute_bootstrap(
                mode="apply",
                environ={"DATAHUB_GMS_URL": "http://localhost:8080/api?secret=x"},
                confirm_target=confirmation,
                client_factory=lambda **kwargs: events.append("client"),
                definitions_runner=lambda *args, **kwargs: events.append("definitions"),
            )

    assert events == []


def test_apply_refuses_remote_target_without_explicit_override() -> None:
    bootstrap = _bootstrap_module()
    events: list[str] = []

    with pytest.raises(ValueError, match="remote DataHub target requires"):
        bootstrap.execute_bootstrap(
            mode="apply",
            environ={"DATAHUB_GMS_URL": "https://datahub.example:443/path"},
            confirm_target="https://datahub.example:443",
            client_factory=lambda **kwargs: events.append("client"),
            definitions_runner=lambda *args, **kwargs: events.append("definitions"),
        )

    assert events == []


@pytest.mark.parametrize("allow_remote_target", [False, True])
@pytest.mark.parametrize("token", [None, "REMOTE-GMS-TOKEN"])
def test_apply_rejects_remote_http_before_any_work(
    allow_remote_target: bool,
    token: str | None,
) -> None:
    bootstrap = _bootstrap_module()
    events: list[str] = []
    environ = {"DATAHUB_GMS_URL": "http://datahub.example:8080/api"}
    if token is not None:
        environ["DATAHUB_GMS_TOKEN"] = token

    with pytest.raises(ValueError, match="remote DataHub target requires HTTPS"):
        bootstrap.execute_bootstrap(
            mode="apply",
            environ=environ,
            confirm_target="http://datahub.example:8080",
            allow_remote_target=allow_remote_target,
            client_factory=lambda **kwargs: events.append("client"),
            definitions_runner=lambda *args, **kwargs: events.append("definitions"),
        )

    assert events == []


def test_apply_defines_properties_then_upserts_three_assets_and_one_exact_edge() -> None:
    bootstrap = _bootstrap_module()
    events: list[tuple[str, Any]] = []

    class Entities:
        def get(self, urn: str) -> object:
            events.append(("preflight", urn))
            raise ItemNotFoundError("not found")

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
        confirm_target="http://localhost:8080",
        client_factory=client_factory,
        definitions_runner=definitions_runner,
    )

    assert [event[0] for event in events] == [
        "client",
        "preflight",
        "preflight",
        "preflight",
        "definitions",
        "upsert",
        "upsert",
        "upsert",
        "lineage",
    ]
    assert [event[1] for event in events[1:4]] == [
        DATASET_URN,
        FLOW_URN,
        JOB_URN,
    ]
    assert events[4][1] == {
        "command": (
            "datahub",
            "properties",
            "upsert",
            "-f",
            str(PROPERTY_FILE),
        ),
        "url": "http://localhost:8080",
    }
    client_kwargs = events[0][1]
    assert set(client_kwargs) == {"config"}
    client_config = client_kwargs["config"]
    assert isinstance(client_config, DatahubClientConfig)
    assert client_config.server == "http://localhost:8080"
    assert client_config.token == "DO-NOT-PRINT-ME"
    assert client_config.timeout_sec == bootstrap.SDK_REQUEST_TIMEOUT_SECONDS
    assert client_config.retry_max_times == bootstrap.SDK_RETRY_MAX_TIMES
    assert client_config.retry_status_codes == bootstrap.SDK_RETRY_STATUS_CODES

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


@pytest.mark.parametrize("token", [None, "REMOTE-GMS-TOKEN"])
def test_apply_remote_override_allows_confirmed_https_target(
    token: str | None,
) -> None:
    bootstrap = _bootstrap_module()
    captured: dict[str, Any] = {}

    class Entities:
        def get(self, urn: str) -> object:
            raise ItemNotFoundError("not found")

        def upsert(self, entity: object) -> None:
            return None

    class Lineage:
        def add_lineage(self, **kwargs: object) -> None:
            return None

    class Client:
        entities = Entities()
        lineage = Lineage()

    environ = {
        "DATAHUB_GMS_URL": "https://datahub.example/api?token=secret"
    }
    if token is not None:
        environ["DATAHUB_GMS_TOKEN"] = token

    def client_factory(**kwargs: Any) -> Client:
        captured.update(kwargs)
        return Client()

    result = bootstrap.execute_bootstrap(
        mode="apply",
        environ=environ,
        confirm_target="https://datahub.example:443",
        allow_remote_target=True,
        client_factory=client_factory,
        definitions_runner=lambda *args, **kwargs: None,
    )

    assert result["target_origin"] == "https://datahub.example:443"
    assert captured["config"].token == token


def test_apply_refuses_existing_assets_before_any_mutation() -> None:
    bootstrap = _bootstrap_module()
    events: list[tuple[str, Any]] = []

    class Entities:
        def get(self, urn: str) -> object:
            events.append(("preflight", urn))
            if urn == FLOW_URN:
                return object()
            raise ItemNotFoundError("not found")

        def upsert(self, entity: object) -> None:
            events.append(("upsert", entity))

    class Client:
        entities = Entities()

    with pytest.raises(
        RuntimeError,
        match="^DataHub demo asset collision detected$",
    ):
        bootstrap.execute_bootstrap(
            mode="apply",
            environ={"DATAHUB_GMS_URL": "http://localhost:8080"},
            confirm_target="http://localhost:8080",
            client_factory=lambda **kwargs: Client(),
            definitions_runner=lambda *args, **kwargs: events.append(
                ("definitions", None)
            ),
        )

    assert events == [
        ("preflight", DATASET_URN),
        ("preflight", FLOW_URN),
        ("preflight", JOB_URN),
    ]


def test_apply_existing_asset_override_is_explicit_idempotent_rerun() -> None:
    bootstrap = _bootstrap_module()
    events: list[tuple[str, Any]] = []

    class Entities:
        def get(self, urn: str) -> object:
            events.append(("preflight", urn))
            return object()

        def upsert(self, entity: object) -> None:
            events.append(("upsert", entity))

    class Lineage:
        def add_lineage(self, **kwargs: object) -> None:
            events.append(("lineage", kwargs))

    class Client:
        entities = Entities()
        lineage = Lineage()

    bootstrap.execute_bootstrap(
        mode="apply",
        environ={"DATAHUB_GMS_URL": "http://127.0.0.1:8080"},
        confirm_target="http://127.0.0.1:8080",
        allow_existing_demo_assets=True,
        client_factory=lambda **kwargs: Client(),
        definitions_runner=lambda *args, **kwargs: events.append(
            ("definitions", None)
        ),
    )

    assert [event[0] for event in events] == [
        "preflight",
        "preflight",
        "preflight",
        "definitions",
        "upsert",
        "upsert",
        "upsert",
        "lineage",
    ]


def test_apply_wraps_collision_preflight_error_without_secret_detail(
    capsys: pytest.CaptureFixture[str],
) -> None:
    bootstrap = _bootstrap_module()
    sentinel = "SECRET-PREFLIGHT-FAILURE"
    events: list[str] = []

    class Entities:
        def get(self, urn: str) -> object:
            raise RuntimeError(sentinel)

    class Client:
        entities = Entities()

    with pytest.raises(
        RuntimeError,
        match="^DataHub demo asset collision preflight failed$",
    ) as exc_info:
        bootstrap.execute_bootstrap(
            mode="apply",
            environ={"DATAHUB_GMS_URL": "http://localhost:8080"},
            confirm_target="http://localhost:8080",
            client_factory=lambda **kwargs: Client(),
            definitions_runner=lambda *args, **kwargs: events.append("definitions"),
        )

    assert sentinel not in str(exc_info.value)
    assert events == []
    captured = capsys.readouterr()
    assert sentinel not in captured.out
    assert sentinel not in captured.err


def test_cli_requires_exactly_one_mode() -> None:
    bootstrap = _bootstrap_module()
    parser = bootstrap.build_parser()

    with pytest.raises(SystemExit):
        parser.parse_args([])
    with pytest.raises(SystemExit):
        parser.parse_args(["--dry-run", "--apply"])

    assert parser.parse_args(["--dry-run"]).dry_run is True
    parsed_apply = parser.parse_args(
        ["--apply", "--confirm-target", "http://localhost:8080"]
    )
    assert parsed_apply.apply is True
    assert parsed_apply.confirm_target == "http://localhost:8080"
    assert parsed_apply.allow_remote_target is False
    assert parsed_apply.allow_existing_demo_assets is False


def test_cli_accepts_explicit_remote_and_existing_asset_overrides() -> None:
    bootstrap = _bootstrap_module()
    parser = bootstrap.build_parser()

    args = parser.parse_args(
        [
            "--apply",
            "--confirm-target",
            "https://datahub.example:443",
            "--allow-remote-target",
            "--allow-existing-demo-assets",
        ]
    )

    assert args.allow_remote_target is True
    assert args.allow_existing_demo_assets is True


def test_definition_cli_has_a_hard_timeout_and_hides_timeout_output(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    bootstrap = _bootstrap_module()
    sentinel = "SECRET-FROM-TIMED-OUT-CLI"
    observed: dict[str, Any] = {}

    def timed_out_run(*args: Any, **kwargs: Any) -> object:
        observed.update(kwargs)
        raise subprocess.TimeoutExpired(
            cmd=args[0],
            timeout=kwargs["timeout"],
            output=sentinel,
            stderr=sentinel,
        )

    monkeypatch.setattr(bootstrap.subprocess, "run", timed_out_run)

    class Entities:
        def get(self, urn: str) -> object:
            raise ItemNotFoundError("not found")

    class Client:
        entities = Entities()

    with pytest.raises(RuntimeError) as exc_info:
        bootstrap.execute_bootstrap(
            mode="apply",
            environ={
                "DATAHUB_GMS_URL": "http://localhost:8080",
                "DATAHUB_GMS_TOKEN": sentinel,
            },
            confirm_target="http://localhost:8080",
            client_factory=lambda **kwargs: Client(),
        )

    assert observed["timeout"] == bootstrap.DEFINITION_CLI_TIMEOUT_SECONDS
    assert str(exc_info.value) == "structured-property definition upsert failed"
    assert sentinel not in str(exc_info.value)
    captured = capsys.readouterr()
    assert sentinel not in captured.out
    assert sentinel not in captured.err


@pytest.mark.parametrize(
    ("failure_stage", "expected_message"),
    [
        ("definitions", "structured-property definition upsert failed"),
        ("client", "DataHub client creation failed"),
        ("dataset", "DataHub Dataset upsert failed"),
        ("flow", "DataHub DataFlow upsert failed"),
        ("job", "DataHub DataJob upsert failed"),
        ("lineage", "DataHub lineage creation failed"),
    ],
)
def test_apply_wraps_each_third_party_failure_without_leaking_details(
    failure_stage: str,
    expected_message: str,
    capsys: pytest.CaptureFixture[str],
) -> None:
    bootstrap = _bootstrap_module()
    sentinel = f"SECRET-FAILURE-{failure_stage}"

    class Entities:
        def get(self, urn: str) -> object:
            raise ItemNotFoundError("not found")

        def upsert(self, entity: object) -> None:
            stage_by_type = {
                Dataset: "dataset",
                DataFlow: "flow",
                DataJob: "job",
            }
            if failure_stage == stage_by_type[type(entity)]:
                raise RuntimeError(sentinel)

    class Lineage:
        def add_lineage(self, **kwargs: object) -> None:
            if failure_stage == "lineage":
                raise RuntimeError(sentinel)

    class Client:
        entities = Entities()
        lineage = Lineage()

    def client_factory(**kwargs: Any) -> Client:
        if failure_stage == "client":
            raise RuntimeError(sentinel)
        return Client()

    def definitions_runner(*args: Any, **kwargs: Any) -> None:
        if failure_stage == "definitions":
            raise RuntimeError(sentinel)

    with pytest.raises(RuntimeError) as exc_info:
        bootstrap.execute_bootstrap(
            mode="apply",
            environ={
                "DATAHUB_GMS_URL": "http://localhost:8080",
                "DATAHUB_GMS_TOKEN": sentinel,
            },
            confirm_target="http://localhost:8080",
            client_factory=client_factory,
            definitions_runner=definitions_runner,
        )

    assert str(exc_info.value) == expected_message
    assert sentinel not in str(exc_info.value)
    captured = capsys.readouterr()
    assert sentinel not in captured.out
    assert sentinel not in captured.err


@pytest.mark.parametrize(
    "fatal",
    [KeyboardInterrupt(), SystemExit(23)],
    ids=["keyboard-interrupt", "system-exit"],
)
def test_apply_does_not_wrap_process_control_base_exceptions(
    fatal: BaseException,
) -> None:
    bootstrap = _bootstrap_module()

    def client_factory(**kwargs: Any) -> object:
        raise fatal

    with pytest.raises(type(fatal)):
        bootstrap.execute_bootstrap(
            mode="apply",
            environ={"DATAHUB_GMS_URL": "http://localhost:8080"},
            confirm_target="http://localhost:8080",
            client_factory=client_factory,
            definitions_runner=lambda *args, **kwargs: None,
        )


def test_cli_suppresses_unexpected_exception_details_and_tracebacks(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    bootstrap = _bootstrap_module()
    sentinel = "SECRET-UNEXPECTED-CLI-FAILURE"

    def fail(**kwargs: Any) -> dict[str, object]:
        raise RuntimeError(sentinel)

    monkeypatch.setattr(bootstrap, "execute_bootstrap", fail)

    with pytest.raises(SystemExit) as exc_info:
        bootstrap.main(
            ["--apply", "--confirm-target", "http://localhost:8080"]
        )

    assert exc_info.value.code == 2
    captured = capsys.readouterr()
    assert sentinel not in captured.out
    assert sentinel not in captured.err
    assert "Traceback" not in captured.err
    assert "DataHub bootstrap failed" in captured.err


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
