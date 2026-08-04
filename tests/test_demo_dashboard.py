import asyncio
import json
from datetime import datetime, timezone
from io import StringIO
from pathlib import Path
from typing import Any

import httpx
import pytest
from rich.console import Console

from datahub_context import FixtureDataHubContext
from demo_dashboard import DEMO_DATASET_URN, run_demo
from remediation_models import IncidentReport


FIXED_NOW = datetime(2026, 8, 4, 15, 16, 17, tzinfo=timezone.utc)


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
        return httpx.Response(200, json={"accepted": True})

    report, rendered = _run(context, remediation)

    assert isinstance(report, IncidentReport)
    assert report.context_mode == "fixture"
    assert [receipt.status for receipt in report.receipts] == [
        "succeeded",
        "succeeded",
    ]
    assert report.writeback.status == "succeeded"
    assert len(context.saved_documents) == 1
    assert context.saved_documents[0]["urn"] == report.writeback.document_urn
    assert "OFFLINE FIXTURE MODE" in rendered
    assert "Aftershock normalized incident envelope received" in rendered
    assert "get_lineage(upstream=false)" in rendered
    assert "fixture response" in rendered
    assert "DATA_JOB" in rendered
    assert "ML_MODEL" in rendered
    assert "ISSUE_PO" in rendered
    assert "ADJUST_PRICE" in rendered
    assert "Executing full workflow: lineage + controls + save_document" in rendered
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


def _fixture_with_target(
    tmp_path: Path,
    *,
    action: str = "ISSUE_PO",
    webhook: str | None = "https://controls.example/cancel",
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
                "urn": DEMO_DATASET_URN,
                "downstreamLineage": {
                    "entities": [
                        {
                            "entity": {
                                "urn": "urn:li:dataJob:dynamic-test-target",
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


def test_dashboard_escapes_dynamic_values_and_marks_failed_control_as_issues(
    tmp_path: Path,
) -> None:
    context = _fixture_with_target(
        tmp_path,
        action="[bold red]UNTRUSTED[/]",
        webhook="https://controls.example/fail",
    )

    report, rendered = _run(
        context,
        lambda _: httpx.Response(503, text="private failure body must not render"),
    )

    assert report.receipts[0].status == "failed"
    assert report.receipts[0].error == "remediation endpoint returned HTTP 503"
    assert "remediation endpoint returned HTTP 503" in rendered
    assert "private failure body must not render" not in rendered
    assert "[bold red]UNTRUSTED[/]" in rendered
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


class _FailingWritebackFixture(FixtureDataHubContext):
    async def save_document(self, **_: Any) -> dict[str, Any]:
        raise RuntimeError("writeback secret must not be rendered")


def test_dashboard_marks_writeback_failure_as_completed_with_issues() -> None:
    context = _FailingWritebackFixture()

    report, rendered = _run(context, lambda _: httpx.Response(200))

    assert all(receipt.status == "succeeded" for receipt in report.receipts)
    assert report.writeback.status == "failed"
    assert "Recorded write-back status: failed" in rendered
    assert "COMPLETED WITH ISSUES" in rendered
    assert "writeback secret" not in rendered
    assert "all discovered controls succeeded" not in rendered
