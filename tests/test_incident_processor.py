import asyncio
import json
import re
import unicodedata
from dataclasses import FrozenInstanceError
from datetime import datetime, timezone
from typing import Any

import httpx
import pytest

from compensating_action_engine import CompensatingActionEngine, RemediationGrant
from datahub_context import (
    DataHubMCPError,
    FixtureDataHubContext,
    MCPDataHubContext,
)
from incident_processor import (
    AftershockIncidentProcessor,
    _existing_incident_document_urn,
    _incident_document_urn,
    _single_line,
)
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
        search_payload={
            "start": 0,
            "count": 50,
            "total": 0,
            "searchResults": [],
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
        "search_documents",
    ]
    assert [name for name, _ in recorder.calls] == [
        "get_lineage",
        "get_entities",
        "search_documents",
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


def test_replay_reuses_one_matching_document_when_legacy_duplicates_exist() -> None:
    recorder = _recorder_with_mixed_targets()
    canonical_urn = "urn:li:document:shared-aftershock-legacy-a"
    duplicate_urn = "urn:li:document:shared-aftershock-legacy-z"
    title = "Aftershock incident INC-9942"
    recorder.search_payload = {
        "start": 0,
        "count": 50,
        "total": 2,
        "searchResults": [
            {
                "entity": {
                    "urn": duplicate_urn,
                    "subType": "Summary",
                    "info": {"title": title},
                }
            },
            {
                "entity": {
                    "urn": canonical_urn,
                    "subType": "Summary",
                    "info": {"title": title},
                }
            },
        ],
    }
    recorder.grep_payload = {
        "results": [
            {
                "urn": duplicate_urn,
                "title": title,
                "matches": [
                    {"excerpt": f"- Source dataset: {DATASET_URN}", "position": 0}
                ],
                "total_matches": 1,
            },
            {
                "urn": canonical_urn,
                "title": title,
                "matches": [
                    {"excerpt": f"- Source dataset: {DATASET_URN}", "position": 0}
                ],
                "total_matches": 1,
            },
        ],
        "total_matches": 2,
        "documents_with_matches": 2,
    }
    recorder.save_payload = {"success": True, "urn": canonical_urn}
    recorder.echo_saved_urn = True

    def terminal(_: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            json={
                "receipt_version": 1,
                "status": "succeeded",
                "receipt_id": "replayed-receipt",
            },
        )

    report = _run_processor(recorder, terminal)

    assert [name for name, _ in recorder.calls] == [
        "get_lineage",
        "get_entities",
        "search_documents",
        "grep_documents",
        "save_document",
    ]
    search_arguments = recorder.calls[2][1]
    assert search_arguments == {"query": title, "num_results": 50, "offset": 0}
    grep_arguments = recorder.calls[3][1]
    assert grep_arguments["urns"] == [canonical_urn, duplicate_urn]
    assert grep_arguments["pattern"] == (
        rf"(?m)^(?:{re.escape('- Record key: ')}[^\r\n]+|"
        f"{re.escape(f'- Source dataset: {DATASET_URN}')})$"
    )
    assert recorder.calls[4][1]["urn"] == canonical_urn
    assert report.writeback == WriteBackReceipt(
        status="succeeded",
        document_urn=canonical_urn,
        error=None,
    )


def test_replay_prefers_exact_record_key_over_legacy_dataset_match() -> None:
    recorder = _recorder_with_mixed_targets()
    legacy_urn = "urn:li:document:shared-aftershock-a-legacy"
    keyed_urn = "urn:li:document:shared-aftershock-z-keyed"
    title = "Aftershock incident INC-9942"
    record_key = _incident_document_urn("INC-9942", DATASET_URN)
    recorder.search_payload = {
        "start": 0,
        "count": 50,
        "total": 2,
        "searchResults": [
            {"entity": {"urn": legacy_urn, "info": {"title": title}}},
            {"entity": {"urn": keyed_urn, "info": {"title": title}}},
        ],
    }
    recorder.grep_payload = {
        "results": [
            {
                "urn": legacy_urn,
                "title": title,
                "matches": [
                    {"excerpt": f"- Source dataset: {DATASET_URN}", "position": 0}
                ],
                "total_matches": 1,
            },
            {
                "urn": keyed_urn,
                "title": title,
                "matches": [
                    {"excerpt": f"- Record key: {record_key}", "position": 0}
                ],
                "total_matches": 1,
            },
        ],
        "total_matches": 2,
        "documents_with_matches": 2,
    }
    recorder.save_payload = {"success": True, "urn": keyed_urn}
    recorder.echo_saved_urn = True

    def terminal(_: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            json={
                "receipt_version": 1,
                "status": "succeeded",
                "receipt_id": "keyed-replay-receipt",
            },
        )

    report = _run_processor(recorder, terminal)

    assert recorder.calls[-1][1]["urn"] == keyed_urn
    assert report.writeback.document_urn == keyed_urn
    assert record_key in recorder.calls[-1][1]["content"]


def test_record_key_wins_when_dataset_marker_appears_first_in_content() -> None:
    context = FixtureDataHubContext()
    title = "Aftershock incident INC-9942"
    record_key = _incident_document_urn("INC-9942", DATASET_URN)
    legacy_urn = "urn:li:document:shared-aftershock-a-legacy"
    keyed_urn = "urn:li:document:shared-aftershock-z-keyed"

    async def scenario() -> str | None:
        await context.save_document(
            document_type="Summary",
            title=title,
            content=f"- Source dataset: {DATASET_URN}\n",
            urn=legacy_urn,
            related_assets=[DATASET_URN],
        )
        await context.save_document(
            document_type="Summary",
            title=title,
            content=(
                f"- Source dataset: {DATASET_URN}\n"
                f"- Record key: {record_key}\n"
            ),
            urn=keyed_urn,
            related_assets=[DATASET_URN],
        )
        return await _existing_incident_document_urn(
            context,
            title=title,
            dataset_urn=DATASET_URN,
            record_key=record_key,
        )

    assert asyncio.run(scenario()) == keyed_urn


def test_foreign_record_key_is_never_adopted_as_a_legacy_match() -> None:
    context = FixtureDataHubContext()
    requested_incident = "INC 42"
    foreign_incident = "INC  42"
    title = f"Aftershock incident {_single_line(requested_incident)}"
    requested_key = _incident_document_urn(requested_incident, DATASET_URN)
    foreign_key = _incident_document_urn(foreign_incident, DATASET_URN)
    foreign_urn = "urn:li:document:shared-aftershock-foreign-key"
    assert requested_key != foreign_key

    async def scenario() -> str | None:
        await context.save_document(
            document_type="Summary",
            title=title,
            content=(
                f"- Source dataset: {DATASET_URN}\n"
                f"- Record key: {foreign_key}\n"
            ),
            urn=foreign_urn,
            related_assets=[DATASET_URN],
        )
        return await _existing_incident_document_urn(
            context,
            title=title,
            dataset_urn=DATASET_URN,
            record_key=requested_key,
        )

    assert asyncio.run(scenario()) is None


def test_incident_text_cannot_mask_the_generated_record_key_match() -> None:
    context = FixtureDataHubContext()
    incident_id = "INC Record key: attacker-controlled"
    title = f"Aftershock incident {_single_line(incident_id)}"
    record_key = _incident_document_urn(incident_id, DATASET_URN)
    keyed_urn = "urn:li:document:shared-aftershock-injection-safe"

    async def scenario() -> str | None:
        await context.save_document(
            document_type="Summary",
            title=title,
            content=(
                f"- Incident ID: {incident_id}\n"
                f"- Source dataset: {DATASET_URN}\n"
                f"- Record key: {record_key}\n"
            ),
            urn=keyed_urn,
            related_assets=[DATASET_URN],
        )
        return await _existing_incident_document_urn(
            context,
            title=title,
            dataset_urn=DATASET_URN,
            record_key=record_key,
        )

    assert asyncio.run(scenario()) == keyed_urn


def test_truncated_record_identity_evidence_fails_closed() -> None:
    context = FixtureDataHubContext()
    title = "Aftershock incident INC-9942"
    record_key = _incident_document_urn("INC-9942", DATASET_URN)
    poisoned_urn = "urn:li:document:shared-aftershock-poisoned"

    async def scenario() -> str | None:
        await context.save_document(
            document_type="Summary",
            title=title,
            content=(
                f"- Source dataset: {DATASET_URN}\n"
                f"- Record key: {record_key}\n"
                "- Record key: urn:li:document:aftershock-incident-foreign-"
                "00000000000000000000000000000000\n"
            ),
            urn=poisoned_urn,
            related_assets=[DATASET_URN],
        )
        return await _existing_incident_document_urn(
            context,
            title=title,
            dataset_urn=DATASET_URN,
            record_key=record_key,
        )

    with pytest.raises(ValueError, match="content result"):
        asyncio.run(scenario())


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
        search_payload={
            "start": 0,
            "count": 50,
            "total": 0,
            "searchResults": [],
        },
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


@pytest.mark.parametrize("fail_tool", ["search_documents", "grep_documents"])
def test_replay_lookup_failure_never_falls_back_to_document_create(
    fail_tool: str,
) -> None:
    recorder = _recorder_with_mixed_targets()
    title = "Aftershock incident INC-9942"
    existing_urn = "urn:li:document:shared-aftershock-existing"
    recorder.search_payload = {
        "start": 0,
        "count": 50,
        "total": 1,
        "searchResults": [
            {"entity": {"urn": existing_urn, "info": {"title": title}}}
        ],
    }
    recorder.grep_payload = {
        "results": [
            {
                "urn": existing_urn,
                "title": title,
                "matches": [
                    {"excerpt": f"- Source dataset: {DATASET_URN}", "position": 0}
                ],
                "total_matches": 1,
            }
        ],
        "total_matches": 1,
        "documents_with_matches": 1,
    }
    recorder.fail_tool = fail_tool

    def terminal(_: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            json={
                "receipt_version": 1,
                "status": "succeeded",
                "receipt_id": "lookup-failure-receipt",
            },
        )

    report = _run_processor(recorder, terminal)

    assert report.receipts[0].status == "succeeded"
    assert report.writeback == WriteBackReceipt(
        status="failed",
        document_urn=None,
        error=WRITEBACK_ERROR,
    )
    assert all(name != "save_document" for name, _ in recorder.calls)


def test_update_returning_different_urn_never_retries_as_create() -> None:
    recorder = _recorder_with_mixed_targets()
    title = "Aftershock incident INC-9942"
    existing_urn = "urn:li:document:shared-aftershock-existing"
    recorder.search_payload = {
        "start": 0,
        "count": 50,
        "total": 1,
        "searchResults": [
            {"entity": {"urn": existing_urn, "info": {"title": title}}}
        ],
    }
    recorder.grep_payload = {
        "results": [
            {
                "urn": existing_urn,
                "title": title,
                "matches": [
                    {"excerpt": f"- Source dataset: {DATASET_URN}", "position": 0}
                ],
                "total_matches": 1,
            }
        ],
        "total_matches": 1,
        "documents_with_matches": 1,
    }
    recorder.save_payload = {
        "success": True,
        "urn": "urn:li:document:server-returned-different",
    }
    recorder.echo_saved_urn = False

    def terminal(_: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            json={
                "receipt_version": 1,
                "status": "succeeded",
                "receipt_id": "mismatched-update-receipt",
            },
        )

    report = _run_processor(recorder, terminal)

    save_calls = [args for name, args in recorder.calls if name == "save_document"]
    assert save_calls == [recorder.calls[-1][1]]
    assert save_calls[0]["urn"] == existing_urn
    assert report.writeback == WriteBackReceipt(
        status="failed",
        document_urn=None,
        error=WRITEBACK_ERROR,
    )


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
