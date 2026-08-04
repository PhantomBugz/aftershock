"""Contract tests for live and fixture DataHub context adapters."""

from __future__ import annotations

import asyncio
import json
import math
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
                    "total": 2,
                    "offset": 0,
                    "returned": 1,
                    "hasMore": True,
                }
            },
            1: {
                "downstreams": {
                    "searchResults": [{"entity": {"urn": MODEL_URN}}],
                    "total": 2,
                    "offset": 1,
                    "returned": 1,
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
    assert lineage["downstreams"] == {
        "searchResults": [
            {"entity": {"urn": DATA_JOB_URN}},
            {"entity": {"urn": MODEL_URN}},
        ],
        "total": 2,
        "offset": 0,
        "returned": 2,
        "hasMore": False,
    }


def test_get_lineage_fails_closed_when_has_more_page_is_empty() -> None:
    recorder = MCPCallRecorder(
        lineage_pages={
            0: {
                "downstreams": {
                    "searchResults": [],
                    "total": 1,
                    "offset": 0,
                    "returned": 0,
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


def test_get_lineage_normalizes_legitimate_cleaned_zero_result_shape() -> None:
    recorder = MCPCallRecorder(
        lineage_pages={0: {"downstreams": {"total": 0}}}
    )
    context = MCPDataHubContext(client_factory=make_client_factory(recorder))

    lineage = asyncio.run(context.get_lineage(DATASET_URN))

    assert lineage == {
        "downstreams": {
            "searchResults": [],
            "total": 0,
            "offset": 0,
            "returned": 0,
            "hasMore": False,
        }
    }


@pytest.mark.parametrize(
    "downstreams",
    [
        None,
        [],
        {},
        {"facets": []},
        {
            "searchResults": {},
            "total": 1,
            "offset": 0,
            "returned": 1,
            "hasMore": False,
        },
        {
            "total": 1,
            "offset": 0,
            "returned": 1,
            "hasMore": False,
        },
        {
            "searchResults": [{"entity": {"urn": DATA_JOB_URN}}],
            "total": 1,
            "offset": 0,
            "returned": 2,
            "hasMore": False,
        },
        {
            "searchResults": [{"entity": {"urn": DATA_JOB_URN}}],
            "total": 1,
            "offset": 1,
            "returned": 1,
            "hasMore": False,
        },
        {
            "searchResults": [{"entity": {"urn": DATA_JOB_URN}}],
            "total": 1,
            "offset": 0,
            "returned": 1,
            "hasMore": "false",
        },
        {"total": False},
        {"total": 0, "offset": False},
        {"total": 0, "returned": False},
    ],
)
def test_get_lineage_rejects_inconsistent_official_metadata(
    downstreams: Any,
) -> None:
    recorder = MCPCallRecorder(
        lineage_pages={0: {"downstreams": downstreams}}
    )
    context = MCPDataHubContext(client_factory=make_client_factory(recorder))

    with pytest.raises(DataHubMCPError):
        asyncio.run(context.get_lineage(DATASET_URN))


def test_get_lineage_rejects_incomplete_datahub_060_capped_response() -> None:
    recorder = MCPCallRecorder(
        lineage_pages={
            0: {
                "downstreams": {
                    "searchResults": [{"entity": {"urn": DATA_JOB_URN}}],
                    "total": 2,
                    "offset": 0,
                    "returned": 1,
                    "hasMore": False,
                }
            }
        }
    )
    context = MCPDataHubContext(client_factory=make_client_factory(recorder))

    with pytest.raises(DataHubMCPError, match="incomplete lineage response"):
        asyncio.run(context.get_lineage(DATASET_URN))


def test_get_lineage_enforces_a_finite_page_limit(monkeypatch) -> None:
    monkeypatch.setattr(datahub_context, "_MAX_LINEAGE_PAGES", 1, raising=False)
    recorder = MCPCallRecorder(
        lineage_pages={
            0: {
                "downstreams": {
                    "searchResults": [{"entity": {"urn": DATA_JOB_URN}}],
                    "total": 2,
                    "offset": 0,
                    "returned": 1,
                    "hasMore": True,
                }
            },
            1: {
                "downstreams": {
                    "searchResults": [{"entity": {"urn": MODEL_URN}}],
                    "total": 2,
                    "offset": 1,
                    "returned": 1,
                    "hasMore": False,
                }
            },
        },
        max_lineage_calls=1,
    )
    context = MCPDataHubContext(client_factory=make_client_factory(recorder))

    with pytest.raises(DataHubMCPError, match="pagination safety limit"):
        asyncio.run(context.get_lineage(DATASET_URN))


def test_get_lineage_enforces_a_finite_result_limit(monkeypatch) -> None:
    monkeypatch.setattr(datahub_context, "_MAX_LINEAGE_RESULTS", 1, raising=False)
    recorder = MCPCallRecorder(
        lineage_pages={
            0: {
                "downstreams": {
                    "searchResults": [
                        {"entity": {"urn": DATA_JOB_URN}},
                        {"entity": {"urn": MODEL_URN}},
                    ],
                    "total": 2,
                    "offset": 0,
                    "returned": 2,
                    "hasMore": False,
                }
            }
        }
    )
    context = MCPDataHubContext(client_factory=make_client_factory(recorder))

    with pytest.raises(DataHubMCPError, match="result safety limit"):
        asyncio.run(context.get_lineage(DATASET_URN))


def test_get_lineage_rejects_a_repeated_page() -> None:
    repeated_result = {"entity": {"urn": DATA_JOB_URN}}
    recorder = MCPCallRecorder(
        lineage_pages={
            0: {
                "downstreams": {
                    "searchResults": [repeated_result],
                    "total": 2,
                    "offset": 0,
                    "returned": 1,
                    "hasMore": True,
                }
            },
            1: {
                "downstreams": {
                    "searchResults": [repeated_result],
                    "total": 2,
                    "offset": 1,
                    "returned": 1,
                    "hasMore": False,
                }
            },
        }
    )
    context = MCPDataHubContext(client_factory=make_client_factory(recorder))

    with pytest.raises(DataHubMCPError, match="repeated lineage page"):
        asyncio.run(context.get_lineage(DATASET_URN))


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


def test_client_initialization_timeout_is_sanitized() -> None:
    class InitializationTimeoutClient:
        async def __aenter__(self):
            raise TimeoutError("timeout containing super-secret-value")

        async def __aexit__(self, *args: object) -> None:
            return None

    context = MCPDataHubContext(
        client_factory=lambda: InitializationTimeoutClient()  # type: ignore[arg-type]
    )

    with pytest.raises(DataHubMCPError) as error:
        asyncio.run(context.get_entities([DATA_JOB_URN]))

    assert "get_entities" in str(error.value)
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

    list_payload = [{"urn": DATA_JOB_URN}, {"urn": MODEL_URN}]
    assert parse_tool_result(
        SimpleNamespace(
            is_error=False,
            data=None,
            structured_content={"result": list_payload},
            content=[],
        ),
        tool_name="get_entities",
    ) == list_payload
    assert parse_tool_result(
        SimpleNamespace(
            is_error=False,
            data=None,
            structured_content={"result": list_payload, "metadata": {}},
            content=[],
        ),
        tool_name="get_entities",
    ) == {"result": list_payload, "metadata": {}}


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
    monkeypatch.setenv("DATAHUB_MCP_URL", "https://mcp.example.test/context")
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
        def __init__(
            self,
            transport: object,
            *,
            timeout: float,
            init_timeout: float,
        ) -> None:
            captured["transport"] = transport
            captured["timeout"] = timeout
            captured["init_timeout"] = init_timeout

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
    assert math.isfinite(captured["timeout"]) and captured["timeout"] > 0
    assert (
        math.isfinite(captured["init_timeout"])
        and captured["init_timeout"] > 0
    )
    assert "must-not-be-forwarded" not in repr(captured)


@pytest.mark.parametrize(
    "environ",
    [
        {"PATH": "test-path"},
        {"DATAHUB_GMS_URL": "   "},
    ],
)
def test_stdio_transport_requires_an_explicit_gms_url(
    environ: dict[str, str],
) -> None:
    with pytest.raises(DataHubConfigurationError, match="DATAHUB_GMS_URL"):
        build_mcp_client_factory(environ)


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
        def __init__(
            self,
            transport: object,
            *,
            timeout: float,
            init_timeout: float,
        ) -> None:
            captured["transport"] = transport
            captured["timeout"] = timeout
            captured["init_timeout"] = init_timeout

    monkeypatch.setattr(datahub_context, "StdioTransport", FakeTransport)
    monkeypatch.setattr(datahub_context, "Client", FakeClient)

    factory = build_mcp_client_factory(
        {
            "PATH": "test-path",
            "SYSTEMROOT": "C:\\Windows",
            "DATAHUB_GMS_URL": "https://gms.example.test",
            "DATAHUB_GMS_TOKEN": "local-gms-token",
            "DATAHUB_MCP_TOKEN": "remote-only-token",
            "DATAHUB_SKIP_CONFIG": "false",
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
        "DATAHUB_SKIP_CONFIG": "true",
        "TOOLS_IS_MUTATION_ENABLED": "true",
    }
    assert math.isfinite(captured["timeout"]) and captured["timeout"] > 0
    assert (
        math.isfinite(captured["init_timeout"])
        and captured["init_timeout"] > 0
    )

    captured.clear()
    tokenless_factory = build_mcp_client_factory(
        {"DATAHUB_GMS_URL": "http://localhost:8080"}
    )
    tokenless_factory()

    assert captured["env"] == {
        "DATAHUB_GMS_URL": "http://localhost:8080",
        "DATAHUB_SKIP_CONFIG": "true",
        "TOOLS_IS_MUTATION_ENABLED": "true",
    }
