import asyncio
import json
from datetime import datetime, timezone
from io import StringIO
from pathlib import Path
from typing import Any

import httpx
import pytest
from rich.console import Console

from compensating_action_engine import RemediationGrant
from datahub_context import FixtureDataHubContext
from demo_dashboard import DEMO_DATASET_URN, run_demo
from remediation_models import IncidentReport


FIXED_NOW = datetime(2026, 8, 4, 15, 16, 17, tzinfo=timezone.utc)
FIXTURE_GRANTS = (
    RemediationGrant(
        target_urn=(
            "urn:li:dataJob:(urn:li:dataFlow:(airflow,aftershock_demo,PROD),"
            "purchase_order_generator)"
        ),
        entity_type="DATA_JOB",
        business_action="ISSUE_PO",
        endpoint="https://api.internal.example/remediate/cancel_po",
    ),
    RemediationGrant(
        target_urn=(
            "urn:li:mlModel:(urn:li:dataPlatform:sagemaker,"
            "dynamic_pricing_model,PROD)"
        ),
        entity_type="ML_MODEL",
        business_action="ADJUST_PRICE",
        endpoint="https://api.internal.example/remediate/revert_pricing",
    ),
)
DYNAMIC_TARGET_URN = "urn:li:dataJob:dynamic-test-target"


def _console(output: StringIO) -> Console:
    return Console(
        file=output,
        force_terminal=False,
        color_system=None,
        width=140,
    )


def _run(
    context: FixtureDataHubContext,
    handler,
    *,
    allowed_controls: tuple[RemediationGrant, ...] = FIXTURE_GRANTS,
    incident_id: str = "INC-9942",
    dataset_urn: str = DEMO_DATASET_URN,
) -> tuple[IncidentReport, str]:
    output = StringIO()

    async def scenario() -> IncidentReport:
        async with httpx.AsyncClient(
            transport=httpx.MockTransport(handler), timeout=10.0
        ) as client:
            return await run_demo(
                console=_console(output),
                context=context,
                http_client=client,
                remediation_grants=allowed_controls,
                incident_id=incident_id,
                dataset_urn=dataset_urn,
                clock=lambda: FIXED_NOW,
                delay=0,
            )

    return asyncio.run(scenario()), output.getvalue()


class _UncalledMCPContext:
    mode = "mcp"

    async def get_lineage(self, _: str) -> dict[str, Any]:
        raise AssertionError("validation must occur before an MCP read")

    async def get_entities(self, _: list[str]) -> list[dict[str, Any]]:
        raise AssertionError("validation must occur before an MCP read")

    async def save_document(self, **_: Any) -> dict[str, Any]:
        raise AssertionError("validation must occur before an MCP write")


def test_dashboard_never_uses_mock_controls_for_an_mcp_context() -> None:
    with pytest.raises(
        ValueError, match="http_client is required for non-fixture context"
    ):
        asyncio.run(
            run_demo(
                console=_console(StringIO()),
                context=_UncalledMCPContext(),
                delay=0,
            )
        )


def test_dashboard_runs_processor_records_fixture_document_and_displays_report() -> None:
    context = FixtureDataHubContext()
    requests: list[tuple[str, dict[str, Any]]] = []

    async def remediation(request: httpx.Request) -> httpx.Response:
        requests.append((request.url.path, json.loads(request.content)))
        return httpx.Response(
            200,
            json={
                "receipt_version": 1,
                "status": "succeeded",
                "receipt_id": f"fixture{request.url.path.replace('/', '-')}",
            },
        )

    report, rendered = _run(context, remediation)

    assert isinstance(report, IncidentReport)
    assert report.context_mode == "fixture"
    assert [receipt.status for receipt in report.receipts] == [
        "succeeded",
        "succeeded",
    ]
    assert report.writeback.status == "succeeded"
    assert len(context.saved_documents) == 1
    assert context.saved_documents[0]["urn"] is None
    assert report.writeback.document_urn == "urn:li:document:aftershock-fixture"
    assert "MIXED DEMO MODE" in rendered
    assert "caller-supplied HTTP transport" in rendered
    assert "OFFLINE FIXTURE MODE" not in rendered
    assert "Aftershock normalized incident envelope received" in rendered
    assert "get_lineage(upstream=false)" in rendered
    assert "fixture response" in rendered
    assert "DATA_JOB" in rendered
    assert "ML_MODEL" in rendered
    assert "ISSUE_PO" in rendered
    assert "ADJUST_PRICE" in rendered
    assert "OBSERVE  //  reading DataHub lineage and entity metadata" in rendered
    assert "DECIDE  //  resolved 2 metadata-backed remediation targets" in rendered
    assert "ACT  //  evaluating 2 targets against exact remediation grants" in rendered
    assert "PERSIST  //  saving receipt evidence to DataHub" in rendered
    assert rendered.index("OBSERVE  //") < rendered.index("DECIDE  //")
    assert rendered.index("DECIDE  //") < rendered.index("ACT  //")
    assert rendered.index("ACT  //") < rendered.index("PERSIST  //")
    assert "RECORDED BLAST-RADIUS VIEW" in rendered
    assert "ACT 3  //  RECORDED CONTROL RECEIPTS" in rendered
    assert (
        "Control processing occurred during the full workflow above; this is a "
        "retrospective receipt view."
    ) in rendered
    assert "Executing incident processor" not in rendered
    assert "Executing Compensating Controls" not in rendered
    for receipt in report.receipts:
        assert receipt.status in rendered
        assert str(receipt.http_status) in rendered
        assert receipt.endpoint in rendered
        assert receipt.external_receipt_id in rendered
    assert [receipt.external_receipt_id for receipt in report.receipts] == [
        "fixture-remediate-cancel_po",
        "fixture-remediate-revert_pricing",
    ]
    assert "Recorded write-back status: succeeded" in rendered
    assert report.writeback.document_urn in rendered
    assert "in-memory fixture recorder" in rendered
    assert "COMPLETED: all discovered controls succeeded and the incident record was saved" in rendered
    assert "$120,000" not in rendered
    assert "transactions reversed" not in rendered
    assert "Enterprise State Restored" not in rendered
    assert "native webhook" not in rendered
    assert {path for path, _ in requests} == {
        "/remediate/cancel_po",
        "/remediate/revert_pricing",
    }
    assert {payload["incident_id"] for _, payload in requests} == {"INC-9942"}


def test_default_fixture_transport_returns_explicit_terminal_receipts() -> None:
    context = FixtureDataHubContext()
    output = StringIO()

    report = asyncio.run(
        run_demo(
            console=_console(output),
            context=context,
            clock=lambda: FIXED_NOW,
            delay=0,
        )
    )

    assert [receipt.status for receipt in report.receipts] == [
        "succeeded",
        "succeeded",
    ]
    assert [receipt.external_receipt_id for receipt in report.receipts] == [
        "fixture-receipt-cancel_po",
        "fixture-receipt-revert_pricing",
    ]
    assert "deterministic local test doubles" in output.getvalue()
    assert "OFFLINE FIXTURE MODE" in output.getvalue()
    assert "MIXED DEMO MODE" not in output.getvalue()
    assert "fixture-receipt-cancel_po" in output.getvalue()


def _fixture_with_target(
    tmp_path: Path,
    *,
    action: str = "ISSUE_PO",
    webhook: str | None = "https://controls.example/cancel",
    dataset_urn: str = DEMO_DATASET_URN,
) -> FixtureDataHubContext:
    properties = [
        {
            "structuredProperty": {
                "urn": "urn:li:structuredProperty:aftershock.businessAction",
                "definition": {"qualifiedName": "aftershock.businessAction"},
            },
            "values": [{"stringValue": action}],
        }
    ]
    if webhook is not None:
        properties.append(
            {
                "structuredProperty": {
                    "urn": "urn:li:structuredProperty:aftershock.remediationWebhook",
                    "definition": {
                        "qualifiedName": "aftershock.remediationWebhook"
                    },
                },
                "values": [{"stringValue": webhook}],
            }
        )
    payload = {
        "data": {
            "dataset": {
                "urn": dataset_urn,
                "downstreamLineage": {
                    "entities": [
                        {
                            "entity": {
                                "urn": DYNAMIC_TARGET_URN,
                                "type": "DATA_JOB",
                                "structuredProperties": {"properties": properties},
                            }
                        }
                    ]
                },
            }
        }
    }
    fixture_path = tmp_path / "lineage.json"
    fixture_path.write_text(json.dumps(payload), encoding="utf-8")
    return FixtureDataHubContext(fixture_path)


def _dynamic_grant(action: str, endpoint: str) -> RemediationGrant:
    return RemediationGrant(
        target_urn=DYNAMIC_TARGET_URN,
        entity_type="DATA_JOB",
        business_action=action,
        endpoint=endpoint,
    )


def test_dashboard_uses_the_supplied_incident_and_dataset_without_prod_copy(
    tmp_path: Path,
) -> None:
    dataset_urn = (
        "urn:li:dataset:(urn:li:dataPlatform:postgres,"
        "aftershock_demo.inventory_pricing,DEV)"
    )
    endpoint = "https://controls.example/live"
    context = _fixture_with_target(
        tmp_path,
        webhook=endpoint,
        dataset_urn=dataset_urn,
    )
    requests: list[dict[str, Any]] = []

    def terminal(request: httpx.Request) -> httpx.Response:
        requests.append(json.loads(request.content))
        return httpx.Response(
            200,
            json={
                "receipt_version": 1,
                "status": "succeeded",
                "receipt_id": "parameterized-demo-receipt",
            },
        )

    report, rendered = _run(
        context,
        terminal,
        allowed_controls=(_dynamic_grant("ISSUE_PO", endpoint),),
        incident_id="INC-LIVE-777",
        dataset_urn=dataset_urn,
    )

    assert report.incident_id == "INC-LIVE-777"
    assert report.dataset_urn == dataset_urn
    assert requests[0]["incident_id"] == "INC-LIVE-777"
    assert dataset_urn in rendered
    assert "inventory_pricing,PROD" not in rendered


def test_dashboard_escapes_dynamic_values_and_marks_unknown_control_as_issues(
    tmp_path: Path,
) -> None:
    context = _fixture_with_target(
        tmp_path,
        action="[bold_red]UNTRUSTED[/]",
        webhook="https://controls.example/fail",
    )

    report, rendered = _run(
        context,
        lambda _: httpx.Response(503, text="private failure body must not render"),
        allowed_controls=(
            _dynamic_grant(
                "[bold_red]UNTRUSTED[/]",
                "https://controls.example/fail",
            ),
        ),
    )

    assert report.receipts[0].status == "outcome_unknown"
    assert report.receipts[0].error == "remediation outcome unknown after dispatch"
    assert "remediation outcome unknown after dispatch" in rendered
    assert "private failure body must not render" not in rendered
    assert "[bold_red]UNTRUSTED[/]" in rendered
    assert "COMPLETED WITH ISSUES" in rendered
    assert "all discovered controls succeeded" not in rendered


def test_dashboard_marks_skipped_control_as_completed_with_issues(
    tmp_path: Path,
) -> None:
    context = _fixture_with_target(tmp_path, webhook=None)

    async def forbidden(_: httpx.Request) -> httpx.Response:
        raise AssertionError("skipped targets must not issue HTTP requests")

    report, rendered = _run(context, forbidden)

    assert report.receipts[0].status == "skipped"
    assert report.receipts[0].error == "missing remediation webhook"
    assert "missing remediation webhook" in rendered
    assert "COMPLETED WITH ISSUES" in rendered
    assert "all discovered controls succeeded" not in rendered


@pytest.mark.parametrize(
    ("response", "expected_status"),
    [
        (httpx.Response(202, json={"accepted": True}), "accepted"),
        (httpx.Response(200, json={"status": "succeeded"}), "outcome_unknown"),
    ],
)
def test_dashboard_never_calls_nonterminal_receipts_complete(
    tmp_path: Path,
    response: httpx.Response,
    expected_status: str,
) -> None:
    endpoint = "https://controls.example/nonterminal"
    context = _fixture_with_target(tmp_path, webhook=endpoint)

    report, rendered = _run(
        context,
        lambda _: response,
        allowed_controls=(_dynamic_grant("ISSUE_PO", endpoint),),
    )

    assert report.receipts[0].status == expected_status
    assert expected_status in rendered
    assert "COMPLETED WITH ISSUES" in rendered
    assert "all discovered controls succeeded" not in rendered


class _FailingWritebackFixture(FixtureDataHubContext):
    async def save_document(self, **_: Any) -> dict[str, Any]:
        raise RuntimeError("writeback secret must not be rendered")


def test_dashboard_marks_writeback_failure_as_completed_with_issues() -> None:
    context = _FailingWritebackFixture()

    def terminal(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            json={
                "receipt_version": 1,
                "status": "succeeded",
                "receipt_id": f"fixture{request.url.path.replace('/', '-')}",
            },
        )

    report, rendered = _run(context, terminal)

    assert all(receipt.status == "succeeded" for receipt in report.receipts)
    assert report.writeback.status == "failed"
    assert "Recorded write-back status: failed" in rendered
    assert "COMPLETED WITH ISSUES" in rendered
    assert "writeback secret" not in rendered
    assert "all discovered controls succeeded" not in rendered
