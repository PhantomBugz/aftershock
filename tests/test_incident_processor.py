import asyncio
import json
import unicodedata
from dataclasses import FrozenInstanceError
from datetime import datetime, timezone
from typing import Any

import httpx
import pytest

from compensating_action_engine import CompensatingActionEngine, RemediationGrant
from datahub_context import DataHubMCPError, MCPDataHubContext
from incident_processor import AftershockIncidentProcessor, _single_line
from remediation_models import IncidentReport, WriteBackReceipt
from mcp_test_server import (
    DATASET_URN,
    DATA_JOB_URN,
    MODEL_URN,
    MCPCallRecorder,
    make_client_factory,
)


SKIPPED_URN = (
    "urn:li:dataJob:(urn:li:dataFlow:(airflow,aftershock_demo,PROD),audit_sink)"
)
FIXED_NOW = datetime(2026, 8, 4, 15, 16, 17, tzinfo=timezone.utc)
FIXED_TIMESTAMP = "2026-08-04T15:16:17Z"
DOCUMENT_RESULT_URN = "urn:li:document:aftershock-writeback-result"
WRITEBACK_ERROR = "DataHub remediation record persistence failed"


def _property(qualified_name: str, value: str) -> dict[str, Any]:
    return {
        "structuredProperty": {
            "urn": f"urn:li:structuredProperty:{qualified_name}",
            "definition": {"qualifiedName": qualified_name},
        },
        "values": [{"stringValue": value}],
    }


def _entity(
    urn: str,
    entity_type: str,
    *,
    action: str | None = None,
    webhook: str | None = None,
) -> dict[str, Any]:
    properties = []
    if action is not None:
        properties.append(_property("aftershock.businessAction", action))
    if webhook is not None:
        properties.append(_property("aftershock.remediationWebhook", webhook))
    return {
        "urn": urn,
        "type": entity_type,
        "structuredProperties": {"properties": properties},
    }


def _recorder_with_mixed_targets() -> MCPCallRecorder:
    return MCPCallRecorder(
        lineage_pages={
            0: {
                "downstreams": {
                    "searchResults": [
                        {"entity": {"urn": DATA_JOB_URN, "type": "DATA_JOB"}},
                        {"entity": {"urn": MODEL_URN, "type": "ML_MODEL"}},
                        {"entity": {"urn": SKIPPED_URN, "type": "DATA_JOB"}},
                        # Repeated lineage is a repeated receipt, but must not
                        # duplicate the related-assets list.
                        {"entity": {"urn": DATA_JOB_URN, "type": "DATA_JOB"}},
                    ],
                    "total": 4,
                    "offset": 0,
                    "returned": 4,
                    "hasMore": False,
                }
            }
        },
        entities_payload=[
            _entity(
                DATA_JOB_URN,
                "DATA_JOB",
                action="ISSUE_PO",
                webhook="https://controls.example/succeed?secret=not-in-receipt",
            ),
            _entity(
                MODEL_URN,
                "ML_MODEL",
                action=(
                    "ADJUST|PRICE<script>`code`[link](x)*bold*"
                ),
                webhook="https://controls.example/fail?token=private-fragment",
            ),
            _entity(SKIPPED_URN, "DATA_JOB", action="AUDIT_ONLY"),
        ],
        save_payload={
            "success": True,
            "urn": DOCUMENT_RESULT_URN,
            "message": "saved",
            "author": "urn:li:corpuser:test",
        },
    )


def _run_processor(
    recorder: MCPCallRecorder,
    handler,
    *,
    incident_id: str = "INC-9942",
    dataset_urn: str = DATASET_URN,
    milestone_observer=None,
) -> IncidentReport:
    allowed_controls: list[RemediationGrant] = []
    for entity in recorder.entities_payload or []:
        values_by_name: dict[str, str] = {}
        properties = entity.get("structuredProperties", {}).get("properties", [])
        for property_value in properties:
            definition = property_value.get("structuredProperty", {}).get(
                "definition", {}
            )
            qualified_name = definition.get("qualifiedName")
            values = property_value.get("values", [])
            if (
                isinstance(qualified_name, str)
                and values
                and isinstance(values[0].get("stringValue"), str)
            ):
                values_by_name[qualified_name] = values[0]["stringValue"]
        business_action = values_by_name.get("aftershock.businessAction")
        endpoint = values_by_name.get("aftershock.remediationWebhook")
        if business_action is not None and endpoint is not None:
            allowed_controls.append(
                RemediationGrant(
                    target_urn=entity["urn"],
                    entity_type=entity["type"],
                    business_action=business_action,
                    endpoint=endpoint,
                )
            )

    async def scenario() -> IncidentReport:
        context = MCPDataHubContext(client_factory=make_client_factory(recorder))
        async with httpx.AsyncClient(
            transport=httpx.MockTransport(handler), timeout=10.0
        ) as client:
            processor = AftershockIncidentProcessor(
                context,
                CompensatingActionEngine(
                    http_client=client,
                    allowed_controls=allowed_controls,
                ),
                clock=lambda: FIXED_NOW,
                milestone_observer=milestone_observer,
            )
            return await processor.process(incident_id, dataset_urn)

    return asyncio.run(scenario())


def test_processor_announces_truthful_workflow_milestones_in_order() -> None:
    recorder = _recorder_with_mixed_targets()
    milestones: list[tuple[str, str]] = []

    def terminal(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            json={
                "receipt_version": 1,
                "status": "succeeded",
                "receipt_id": f"milestone{request.url.path.replace('/', '-')}",
            },
        )

    report = _run_processor(
        recorder,
        terminal,
        milestone_observer=lambda phase, detail: milestones.append(
            (phase, detail)
        ),
    )

    assert [phase for phase, _ in milestones] == [
        "observe",
        "decide",
        "act",
        "persist",
    ]
    assert milestones == [
        ("observe", "reading DataHub lineage and entity metadata"),
        ("decide", "resolved 3 metadata-backed remediation targets"),
        ("act", "evaluating 3 targets against exact remediation grants"),
        ("persist", "saving receipt evidence to DataHub"),
    ]
    assert report.writeback.status == "succeeded"


def test_milestone_observer_failure_never_changes_the_workflow() -> None:
    recorder = _recorder_with_mixed_targets()
    observed_phases: list[str] = []

    def broken_observer(phase: str, _detail: str) -> None:
        observed_phases.append(phase)
        raise RuntimeError("presentation observer must not stop remediation")

    def terminal(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            json={
                "receipt_version": 1,
                "status": "succeeded",
                "receipt_id": f"observer{request.url.path.replace('/', '-')}",
            },
        )

    report = _run_processor(
        recorder,
        terminal,
        milestone_observer=broken_observer,
    )

    assert observed_phases == ["observe", "decide", "act", "persist"]
    assert [receipt.status for receipt in report.receipts] == [
        "succeeded",
        "succeeded",
        "skipped",
    ]
    assert report.writeback.status == "succeeded"


def test_title_normalization_removes_all_unicode_control_categories() -> None:
    raw = " INC\x00 99\u202e\ud800\x1ftail "

    normalized = _single_line(raw)

    assert normalized == "INC 99 tail"
    assert all(
        not unicodedata.category(character).startswith("C")
        for character in normalized
    )
    assert _single_line("\x00\u202e\ud800") == "unnamed"


def test_processes_mixed_controls_then_writes_one_complete_mcp_summary() -> None:
    recorder = _recorder_with_mixed_targets()
    requests: list[httpx.Request] = []

    async def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        recorder.events.append(f"http:{request.url.path}")
        if request.url.path == "/fail":
            return httpx.Response(503, text="private body")
        return httpx.Response(
            200,
            json={
                "receipt_version": 1,
                "status": "succeeded",
                "receipt_id": f"control{request.url.path.replace('/', '-')}",
            },
        )

    report = _run_processor(recorder, handler)

    assert recorder.events[:2] == ["get_lineage", "get_entities"]
    assert recorder.events[-1] == "save_document"
    assert recorder.events[2:-1] == [
        "http:/succeed",
        "http:/fail",
    ]
    assert [name for name, _ in recorder.calls] == [
        "get_lineage",
        "get_entities",
        "save_document",
    ]

    save_arguments = recorder.calls[-1][1]
    assert set(save_arguments) == {
        "document_type",
        "title",
        "content",
        "urn",
        "related_assets",
    }
    assert save_arguments["document_type"] == "Summary"
    assert save_arguments["title"] == "Aftershock incident INC-9942"
    assert save_arguments["urn"] is None
    assert save_arguments["related_assets"] == [
        DATASET_URN,
        DATA_JOB_URN,
        MODEL_URN,
        SKIPPED_URN,
    ]
    assert "topics" not in save_arguments

    content = save_arguments["content"]
    assert "INC-9942" in content
    assert DATASET_URN in content
    assert "mcp" in content
    assert FIXED_TIMESTAMP in content
    assert content.count(DATA_JOB_URN) == 1
    assert MODEL_URN in content
    assert SKIPPED_URN in content
    assert "succeeded" in content
    assert "outcome_unknown" in content
    assert "skipped" in content
    assert "External receipt ID" in content
    assert "control-succeed" in content
    assert (
        "ADJUST\\|PRICE&lt;script&gt;&#96;code&#96;"
        "\\[link\\](x)\\*bold\\*"
    ) in content
    assert "ADJUST|PRICE<script>" not in content
    assert "<script>" not in content
    assert "`code`" not in content
    assert "not-in-receipt" not in content
    assert "private-fragment" not in content
    assert "private body" not in content

    assert len(requests) == 2
    assert json.loads(requests[1].content)["business_action"] == (
        "ADJUST|PRICE<script>`code`[link](x)*bold*"
    )
    assert report == IncidentReport(
        incident_id="INC-9942",
        dataset_urn=DATASET_URN,
        context_mode="mcp",
        timestamp=FIXED_TIMESTAMP,
        receipts=report.receipts,
        writeback=WriteBackReceipt(
            status="succeeded",
            document_urn=DOCUMENT_RESULT_URN,
            error=None,
        ),
    )
    assert [receipt.status for receipt in report.receipts] == [
        "succeeded",
        "outcome_unknown",
        "skipped",
    ]
    assert [receipt.external_receipt_id for receipt in report.receipts] == [
        "control-succeed",
        None,
        None,
    ]
    assert isinstance(report.receipts, tuple)
    assert report.to_dict() == {
        "incident_id": "INC-9942",
        "dataset_urn": DATASET_URN,
        "context_mode": "mcp",
        "timestamp": FIXED_TIMESTAMP,
        "counts": {
            "succeeded": 1,
            "accepted": 0,
            "failed": 0,
            "skipped": 1,
            "outcome_unknown": 1,
        },
        "receipts": [receipt.to_dict() for receipt in report.receipts],
        "writeback": {
            "status": "succeeded",
            "document_urn": DOCUMENT_RESULT_URN,
            "error": None,
        },
    }
    with pytest.raises(FrozenInstanceError):
        report.writeback.status = "failed"  # type: ignore[misc]


def test_zero_target_incidents_create_documents_without_forcing_update_urns() -> None:
    recorder = MCPCallRecorder(
        lineage_pages={
            0: {
                "downstreams": {
                    "searchResults": [],
                    "total": 0,
                    "offset": 0,
                    "returned": 0,
                    "hasMore": False,
                }
            }
        },
        save_payload={"success": True, "urn": DOCUMENT_RESULT_URN},
    )

    async def handler(_: httpx.Request) -> httpx.Response:
        raise AssertionError("zero-target incidents must not call HTTP controls")

    malicious_id = " INC\x00 99|42\r\n\u202e\ud800control?! "
    first = _run_processor(recorder, handler, incident_id=malicious_id)
    second = _run_processor(recorder, handler, incident_id=malicious_id)
    third = _run_processor(recorder, handler, incident_id="INC 99-43")

    save_calls = [args for name, args in recorder.calls if name == "save_document"]
    assert len(save_calls) == 3
    assert [save_call["urn"] for save_call in save_calls] == [None, None, None]
    assert "\n" not in save_calls[0]["title"]
    assert "\r" not in save_calls[0]["title"]
    assert all(
        not unicodedata.category(character).startswith("C")
        for character in save_calls[0]["title"]
    )
    assert save_calls[0]["related_assets"] == [DATASET_URN]
    assert "No downstream remediation targets were found." in save_calls[0]["content"]
    assert first.receipts == second.receipts == third.receipts == ()
    assert first.to_dict()["counts"] == {
        "succeeded": 0,
        "accepted": 0,
        "failed": 0,
        "skipped": 0,
        "outcome_unknown": 0,
    }
    assert first.writeback.status == "succeeded"
    assert [name for name, _ in recorder.calls].count("get_entities") == 0


@pytest.mark.parametrize(
    "save_payload,fail_tool",
    [
        (
            {
                "success": False,
                "urn": DOCUMENT_RESULT_URN,
                "message": "ordinary failure with writeback-secret",
            },
            None,
        ),
        ({"success": True, "urn": None}, None),
        ({"success": True, "urn": "not-a-document-urn"}, None),
        ({"urn": DOCUMENT_RESULT_URN}, None),
        (
            {"success": True, "urn": DOCUMENT_RESULT_URN},
            "save_document",
        ),
    ],
)
def test_writeback_failures_are_secret_safe_and_preserve_control_receipts(
    save_payload: dict[str, Any], fail_tool: str | None
) -> None:
    recorder = _recorder_with_mixed_targets()
    recorder.lineage_pages[0]["downstreams"]["searchResults"] = [
        {"entity": {"urn": DATA_JOB_URN, "type": "DATA_JOB"}}
    ]
    recorder.lineage_pages[0]["downstreams"].update(
        total=1, returned=1
    )
    recorder.entities_payload = [recorder.entities_payload[0]]  # type: ignore[index]
    recorder.save_payload = save_payload
    recorder.echo_saved_urn = False
    recorder.fail_tool = fail_tool

    async def handler(request: httpx.Request) -> httpx.Response:
        recorder.events.append(f"http:{request.url.path}")
        return httpx.Response(
            200,
            json={
                "receipt_version": 1,
                "status": "succeeded",
                "receipt_id": "writeback-test-control",
            },
        )

    report = _run_processor(recorder, handler)

    assert len(report.receipts) == 1
    assert report.receipts[0].status == "succeeded"
    assert report.receipts[0].http_status == 200
    assert report.writeback == WriteBackReceipt(
        status="failed",
        document_urn=None,
        error=WRITEBACK_ERROR,
    )
    serialized = json.dumps(report.to_dict())
    assert "writeback-secret" not in serialized
    assert "super-secret-value" not in serialized
    assert recorder.events[-1] == "save_document"


def test_server_generated_document_urn_is_accepted_for_a_new_document() -> None:
    recorder = _recorder_with_mixed_targets()
    recorder.lineage_pages[0]["downstreams"]["searchResults"] = []
    recorder.lineage_pages[0]["downstreams"].update(total=0, returned=0)
    recorder.save_payload = {
        "success": True,
        "urn": "urn:li:document:server-selected-different-record",
        "message": "contains writeback-secret",
    }
    recorder.echo_saved_urn = False

    async def handler(_: httpx.Request) -> httpx.Response:
        raise AssertionError("zero-target incidents must not call HTTP controls")

    report = _run_processor(recorder, handler)

    assert recorder.calls[-1][1]["urn"] is None
    assert report.writeback == WriteBackReceipt(
        status="succeeded",
        document_urn="urn:li:document:server-selected-different-record",
        error=None,
    )
    assert "server-selected-different-record" in json.dumps(report.to_dict())
    assert "writeback-secret" not in json.dumps(report.to_dict())


@pytest.mark.parametrize("fail_tool", ["get_lineage", "get_entities"])
def test_mapping_failures_propagate_before_controls_or_writeback(
    fail_tool: str,
) -> None:
    recorder = MCPCallRecorder(fail_tool=fail_tool)
    requests: list[httpx.Request] = []

    async def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(200)

    with pytest.raises(DataHubMCPError):
        _run_processor(recorder, handler)

    expected_events = (
        ["get_lineage"]
        if fail_tool == "get_lineage"
        else ["get_lineage", "get_entities"]
    )
    assert recorder.events == expected_events
    assert requests == []
