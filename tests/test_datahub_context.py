"""Contract tests for live and fixture DataHub context adapters."""

from __future__ import annotations

import asyncio
import json
import sys
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

import datahub_context
from datahub_context import (
    DataHubConfigurationError,
    DataHubMCPError,
    FixtureDataHubContext,
    MCPDataHubContext,
    build_datahub_context_from_env,
    build_mcp_client_factory,
    parse_tool_result,
)
from mcp_test_server import (
    DATASET_URN,
    DATA_JOB_URN,
    MODEL_URN,
    MCPCallRecorder,
    make_client_factory,
)


FIXTURE_PATH = (
    Path(__file__).resolve().parents[1] / "mock-data" / "datahub_lineage.json"
)
DOCUMENT_URN = "urn:li:document:aftershock-inc-9942"


def test_get_lineage_uses_real_mcp_and_paginates_downstreams() -> None:
    recorder = MCPCallRecorder(
        lineage_pages={
            0: {
                "downstreams": {
                    "searchResults": [{"entity": {"urn": DATA_JOB_URN}}],
                    "hasMore": True,
                }
            },
            1: {
                "downstreams": {
                    "searchResults": [{"entity": {"urn": MODEL_URN}}],
                    "hasMore": False,
                }
            },
        }
    )
    context = MCPDataHubContext(client_factory=make_client_factory(recorder))

    lineage = asyncio.run(context.get_lineage(DATASET_URN))

    assert recorder.calls == [
        (
            "get_lineage",
            {
                "urn": DATASET_URN,
                "upstream": False,
                "max_hops": 3,
                "max_results": 100,
                "offset": 0,
            },
        ),
        (
            "get_lineage",
            {
                "urn": DATASET_URN,
                "upstream": False,
                "max_hops": 3,
                "max_results": 100,
                "offset": 1,
            },
        ),
    ]
    assert [
        result["entity"]["urn"]
        for result in lineage["downstreams"]["searchResults"]
    ] == [DATA_JOB_URN, MODEL_URN]
    assert lineage["downstreams"]["hasMore"] is False


def test_get_lineage_fails_closed_when_has_more_page_is_empty() -> None:
    recorder = MCPCallRecorder(
        lineage_pages={
            0: {
                "downstreams": {
                    "searchResults": [],
                    "hasMore": True,
                }
            }
        },
        max_lineage_calls=1,
    )
    context = MCPDataHubContext(client_factory=make_client_factory(recorder))

    with pytest.raises(
        DataHubMCPError, match="returned invalid pagination metadata"
    ):
        asyncio.run(context.get_lineage(DATASET_URN))

    assert recorder.calls == [
        (
            "get_lineage",
            {
                "urn": DATASET_URN,
                "upstream": False,
                "max_hops": 3,
                "max_results": 100,
                "offset": 0,
            },
        )
    ]


def test_get_entities_sends_one_batch_over_real_mcp() -> None:
    recorder = MCPCallRecorder()
    context = MCPDataHubContext(client_factory=make_client_factory(recorder))

    entities = asyncio.run(context.get_entities([DATA_JOB_URN, MODEL_URN]))

    assert recorder.calls == [
        ("get_entities", {"urns": [DATA_JOB_URN, MODEL_URN]})
    ]
    assert entities == recorder.entities_payload


def test_get_entities_normalizes_a_dict_payload() -> None:
    recorder = MCPCallRecorder(
        entities_payload={"urn": DATA_JOB_URN, "type": "DATA_JOB"}
    )
    context = MCPDataHubContext(client_factory=make_client_factory(recorder))

    entities = asyncio.run(context.get_entities([DATA_JOB_URN]))

    assert entities == [{"urn": DATA_JOB_URN, "type": "DATA_JOB"}]


def test_save_document_sends_exact_arguments_without_topics() -> None:
    recorder = MCPCallRecorder()
    context = MCPDataHubContext(client_factory=make_client_factory(recorder))

    result = asyncio.run(
        context.save_document(
            document_type="Summary",
            title="Aftershock incident INC-9942",
            content="# Verified receipts",
            urn=DOCUMENT_URN,
            related_assets=[DATASET_URN, DATA_JOB_URN, MODEL_URN],
        )
    )

    assert recorder.calls == [
        (
            "save_document",
            {
                "document_type": "Summary",
                "title": "Aftershock incident INC-9942",
                "content": "# Verified receipts",
                "urn": DOCUMENT_URN,
                "related_assets": [DATASET_URN, DATA_JOB_URN, MODEL_URN],
            },
        )
    ]
    assert result["success"] is True
    assert result["urn"] == DOCUMENT_URN


def test_mcp_tool_error_is_wrapped_without_leaking_server_details() -> None:
    recorder = MCPCallRecorder(fail_tool="get_lineage")
    context = MCPDataHubContext(client_factory=make_client_factory(recorder))

    with pytest.raises(DataHubMCPError) as error:
        asyncio.run(context.get_lineage(DATASET_URN))

    assert "get_lineage" in str(error.value)
    assert "super-secret-value" not in str(error.value)


def test_save_document_rejects_an_ordinary_unsuccessful_payload() -> None:
    recorder = MCPCallRecorder(
        save_payload={
            "success": False,
            "urn": DOCUMENT_URN,
            "message": "failure containing super-secret-value",
        }
    )
    context = MCPDataHubContext(client_factory=make_client_factory(recorder))

    with pytest.raises(DataHubMCPError) as error:
        asyncio.run(
            context.save_document(
                document_type="Summary",
                title="Aftershock incident INC-9942",
                content="# Verified receipts",
                urn=DOCUMENT_URN,
                related_assets=[DATASET_URN],
            )
        )

    assert "save_document" in str(error.value)
    assert "super-secret-value" not in str(error.value)


def test_tool_result_parser_prefers_data_then_structured_content_then_text() -> None:
    text_block = SimpleNamespace(
        type="text", text=json.dumps({"source": "text"})
    )

    assert parse_tool_result(
        SimpleNamespace(
            is_error=False,
            data={"source": "data"},
            structured_content={"source": "structured"},
            content=[text_block],
        ),
        tool_name="get_lineage",
    ) == {"source": "data"}
    assert parse_tool_result(
        SimpleNamespace(
            is_error=False,
            data=None,
            structured_content={"source": "structured"},
            content=[text_block],
        ),
        tool_name="get_lineage",
    ) == {"source": "structured"}
    assert parse_tool_result(
        SimpleNamespace(
            is_error=False,
            data=None,
            structured_content=None,
            content=[text_block],
        ),
        tool_name="get_lineage",
    ) == {"source": "text"}
    assert parse_tool_result(
        [{"urn": DATA_JOB_URN}], tool_name="get_entities"
    ) == [{"urn": DATA_JOB_URN}]


def test_tool_result_parser_rejects_mcp_errors_and_ambiguous_text() -> None:
    with pytest.raises(DataHubMCPError) as error:
        parse_tool_result(
            SimpleNamespace(
                is_error=True,
                data=None,
                structured_content=None,
                content=[
                    SimpleNamespace(
                        type="text", text="failure with super-secret-value"
                    )
                ],
            ),
            tool_name="get_lineage",
        )
    assert "super-secret-value" not in str(error.value)

    with pytest.raises(DataHubMCPError):
        parse_tool_result(
            SimpleNamespace(
                is_error=False,
                data=None,
                structured_content=None,
                content=[
                    SimpleNamespace(type="text", text="{}"),
                    SimpleNamespace(type="text", text="{}"),
                ],
            ),
            tool_name="get_lineage",
        )


def test_fixture_context_reads_lineage_entities_and_records_writeback() -> None:
    context = FixtureDataHubContext(FIXTURE_PATH)

    lineage = asyncio.run(context.get_lineage(DATASET_URN))
    entities = asyncio.run(context.get_entities([MODEL_URN, DATA_JOB_URN]))
    saved = asyncio.run(
        context.save_document(
            document_type="Summary",
            title="Aftershock incident INC-9942",
            content="# Fixture receipts",
            urn=DOCUMENT_URN,
            related_assets=[DATASET_URN, MODEL_URN, DATA_JOB_URN],
        )
    )

    assert context.mode == "fixture"
    assert len(lineage["downstreams"]["searchResults"]) == 2
    assert [entity["urn"] for entity in entities] == [MODEL_URN, DATA_JOB_URN]
    assert context.saved_documents == [
        {
            "document_type": "Summary",
            "title": "Aftershock incident INC-9942",
            "content": "# Fixture receipts",
            "urn": DOCUMENT_URN,
            "related_assets": [DATASET_URN, MODEL_URN, DATA_JOB_URN],
        }
    ]
    assert saved == {
        "success": True,
        "urn": DOCUMENT_URN,
        "message": "Recorded by the in-memory fixture recorder",
        "author": "aftershock-fixture",
    }


def test_mode_factory_requires_an_explicit_valid_mode(monkeypatch) -> None:
    monkeypatch.delenv("AFTERSHOCK_DATAHUB_MODE", raising=False)
    with pytest.raises(DataHubConfigurationError):
        build_datahub_context_from_env()

    monkeypatch.setenv("AFTERSHOCK_DATAHUB_MODE", "automatic")
    with pytest.raises(DataHubConfigurationError):
        build_datahub_context_from_env()

    monkeypatch.setenv("AFTERSHOCK_DATAHUB_MODE", "fixture")
    fixture_context = build_datahub_context_from_env()
    assert isinstance(fixture_context, FixtureDataHubContext)
    assert fixture_context.mode == "fixture"

    monkeypatch.setenv("AFTERSHOCK_DATAHUB_MODE", "mcp")
    mcp_context = build_datahub_context_from_env()
    assert isinstance(mcp_context, MCPDataHubContext)
    assert mcp_context.mode == "mcp"


def test_live_mode_tool_failure_never_falls_back_to_fixture(
    monkeypatch,
) -> None:
    recorder = MCPCallRecorder(fail_tool="get_lineage")
    monkeypatch.setenv("AFTERSHOCK_DATAHUB_MODE", "mcp")
    monkeypatch.setattr(
        datahub_context,
        "build_mcp_client_factory",
        lambda environ=None: make_client_factory(recorder),
    )

    context = build_datahub_context_from_env()

    assert isinstance(context, MCPDataHubContext)
    with pytest.raises(DataHubMCPError):
        asyncio.run(context.get_lineage(DATASET_URN))


def test_remote_transport_uses_only_the_separate_mcp_bearer_token(
    monkeypatch,
) -> None:
    captured: dict[str, Any] = {}

    class FakeTransport:
        def __init__(
            self,
            *,
            url: str,
            headers: dict[str, str] | None = None,
            httpx_client_factory: object | None = None,
        ) -> None:
            captured["url"] = url
            captured["headers"] = headers
            captured["httpx_client_factory"] = httpx_client_factory

    class FakeClient:
        def __init__(self, transport: object) -> None:
            captured["transport"] = transport

    monkeypatch.setattr(datahub_context, "StreamableHttpTransport", FakeTransport)
    monkeypatch.setattr(datahub_context, "Client", FakeClient)

    factory = build_mcp_client_factory(
        {
            "DATAHUB_MCP_URL": "https://mcp.example.test/context",
            "DATAHUB_MCP_TOKEN": "mcp-only-token",
            "DATAHUB_GMS_TOKEN": "must-not-be-forwarded",
        }
    )
    factory()

    assert captured["url"] == "https://mcp.example.test/context"
    assert captured["headers"] == {
        "Authorization": "Bearer mcp-only-token"
    }
    assert callable(captured["httpx_client_factory"])
    assert "must-not-be-forwarded" not in repr(captured)


def test_stdio_transport_receives_only_local_datahub_credentials(
    monkeypatch,
) -> None:
    captured: dict[str, Any] = {}

    class FakeTransport:
        def __init__(
            self,
            *,
            command: str,
            args: list[str],
            env: dict[str, str],
            keep_alive: bool,
        ) -> None:
            captured.update(
                command=command,
                args=args,
                env=env,
                keep_alive=keep_alive,
            )

    class FakeClient:
        def __init__(self, transport: object) -> None:
            captured["transport"] = transport

    monkeypatch.setattr(datahub_context, "StdioTransport", FakeTransport)
    monkeypatch.setattr(datahub_context, "Client", FakeClient)

    factory = build_mcp_client_factory(
        {
            "PATH": "test-path",
            "SYSTEMROOT": "C:\\Windows",
            "DATAHUB_GMS_URL": "https://gms.example.test",
            "DATAHUB_GMS_TOKEN": "local-gms-token",
            "DATAHUB_MCP_TOKEN": "remote-only-token",
            "UNRELATED_SECRET": "do-not-copy",
        }
    )
    factory()

    assert captured["command"] == sys.executable
    assert captured["args"] == [
        "-m",
        "mcp_server_datahub",
        "--transport",
        "stdio",
    ]
    assert captured["keep_alive"] is False
    assert captured["env"] == {
        "PATH": "test-path",
        "SYSTEMROOT": "C:\\Windows",
        "DATAHUB_GMS_URL": "https://gms.example.test",
        "DATAHUB_GMS_TOKEN": "local-gms-token",
        "TOOLS_IS_MUTATION_ENABLED": "true",
    }
