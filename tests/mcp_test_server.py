"""In-process DataHub-shaped MCP server used by adapter tests."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from fastmcp import Client, FastMCP
from fastmcp.client.transports import FastMCPTransport


DATASET_URN = (
    "urn:li:dataset:(urn:li:dataPlatform:postgres,inventory_pricing,PROD)"
)
DATA_JOB_URN = (
    "urn:li:dataJob:(urn:li:dataFlow:(airflow,aftershock_demo,PROD),"
    "purchase_order_generator)"
)
MODEL_URN = (
    "urn:li:mlModel:(urn:li:dataPlatform:sagemaker,dynamic_pricing_model,PROD)"
)
DOCUMENT_URN = "urn:li:document:aftershock-inc-9942"


@dataclass
class MCPCallRecorder:
    """Mutable server state and an exact record of received tool arguments."""

    calls: list[tuple[str, dict[str, Any]]] = field(default_factory=list)
    events: list[str] = field(default_factory=list)
    lineage_pages: dict[int, dict[str, Any]] = field(
        default_factory=lambda: {
            0: {
                "downstreams": {
                    "searchResults": [{"entity": {"urn": DATA_JOB_URN}}],
                    "total": 1,
                    "offset": 0,
                    "returned": 1,
                    "hasMore": False,
                }
            }
        }
    )
    entities_payload: list[dict[str, Any]] | dict[str, Any] = field(
        default_factory=lambda: [
            {"urn": DATA_JOB_URN, "type": "DATA_JOB"},
            {"urn": MODEL_URN, "type": "ML_MODEL"},
        ]
    )
    save_payload: dict[str, Any] = field(
        default_factory=lambda: {
            "success": True,
            "urn": DOCUMENT_URN,
            "message": "saved",
            "author": "urn:li:corpuser:test",
        }
    )
    search_payload: dict[str, Any] | list[Any] = field(
        default_factory=lambda: {
            "start": 0,
            "count": 10,
            "total": 1,
            "searchResults": [
                {
                    "entity": {
                        "urn": DOCUMENT_URN,
                        "subType": "Summary",
                        "info": {"title": "Aftershock incident INC-9942"},
                    }
                }
            ],
        }
    )
    grep_payload: dict[str, Any] | list[Any] = field(
        default_factory=lambda: {
            "results": [
                {
                    "urn": DOCUMENT_URN,
                    "title": "Aftershock incident INC-9942",
                    "matches": [
                        {"excerpt": "marker: AFTERSHOCK-UNIQUE", "position": 8}
                    ],
                    "total_matches": 1,
                }
            ],
            "total_matches": 1,
            "documents_with_matches": 1,
        }
    )
    echo_saved_urn: bool = False
    fail_tool: str | None = None
    failure_message: str = "server failure containing super-secret-value"
    max_lineage_calls: int | None = None
    datahub_060_lineage_results: list[dict[str, Any]] | None = None


def build_test_server(recorder: MCPCallRecorder) -> FastMCP:
    """Build a real FastMCP server with DataHub's three required tools."""

    server = FastMCP("Aftershock DataHub MCP test server")

    @server.tool
    async def get_lineage(
        urn: str,
        upstream: bool,
        max_hops: int,
        max_results: int,
        offset: int,
    ) -> dict[str, Any]:
        arguments = {
            "urn": urn,
            "upstream": upstream,
            "max_hops": max_hops,
            "max_results": max_results,
            "offset": offset,
        }
        recorder.calls.append(("get_lineage", arguments))
        recorder.events.append("get_lineage")
        if recorder.fail_tool == "get_lineage":
            raise RuntimeError(recorder.failure_message)
        lineage_call_count = sum(
            tool_name == "get_lineage" for tool_name, _ in recorder.calls
        )
        if (
            recorder.max_lineage_calls is not None
            and lineage_call_count > recorder.max_lineage_calls
        ):
            raise RuntimeError("lineage pagination did not advance")
        if recorder.datahub_060_lineage_results is not None:
            # mcp-server-datahub 0.6.0 fetches only ``max_results`` from
            # GraphQL starting at zero, then applies the requested offset.
            all_results = recorder.datahub_060_lineage_results
            fetched_results = all_results[:max_results]
            page_results = fetched_results[offset:]
            return {
                "downstreams": {
                    "searchResults": page_results,
                    "total": len(all_results),
                    "offset": offset,
                    "returned": len(page_results),
                    "hasMore": offset + len(page_results) < len(fetched_results),
                }
            }
        return recorder.lineage_pages[offset]

    @server.tool
    async def get_entities(
        urns: list[str],
    ) -> list[dict[str, Any]] | dict[str, Any]:
        arguments = {"urns": list(urns)}
        recorder.calls.append(("get_entities", arguments))
        recorder.events.append("get_entities")
        if recorder.fail_tool == "get_entities":
            raise RuntimeError(recorder.failure_message)
        return recorder.entities_payload

    @server.tool
    async def search_documents(
        query: str,
        num_results: int,
        offset: int,
    ) -> dict[str, Any] | list[Any]:
        arguments = {
            "query": query,
            "num_results": num_results,
            "offset": offset,
        }
        recorder.calls.append(("search_documents", arguments))
        recorder.events.append("search_documents")
        if recorder.fail_tool == "search_documents":
            raise RuntimeError(recorder.failure_message)
        return recorder.search_payload

    @server.tool
    async def grep_documents(
        urns: list[str],
        pattern: str,
        context_chars: int,
        max_matches_per_doc: int,
        start_offset: int,
    ) -> dict[str, Any] | list[Any]:
        arguments = {
            "urns": list(urns),
            "pattern": pattern,
            "context_chars": context_chars,
            "max_matches_per_doc": max_matches_per_doc,
            "start_offset": start_offset,
        }
        recorder.calls.append(("grep_documents", arguments))
        recorder.events.append("grep_documents")
        if recorder.fail_tool == "grep_documents":
            raise RuntimeError(recorder.failure_message)
        return recorder.grep_payload

    @server.tool
    async def save_document(
        document_type: str,
        title: str,
        content: str,
        related_assets: list[str],
        urn: str | None = None,
    ) -> dict[str, Any]:
        arguments = {
            "document_type": document_type,
            "title": title,
            "content": content,
            "urn": urn,
            "related_assets": list(related_assets),
        }
        recorder.calls.append(("save_document", arguments))
        recorder.events.append("save_document")
        if recorder.fail_tool == "save_document":
            raise RuntimeError(recorder.failure_message)
        if recorder.echo_saved_urn and urn is not None:
            return {**recorder.save_payload, "urn": urn}
        return recorder.save_payload

    return server


def make_client_factory(recorder: MCPCallRecorder):
    """Return fresh connected clients for one shared in-process server."""

    server = build_test_server(recorder)

    def factory() -> Client:
        return Client(FastMCPTransport(server, raise_exceptions=True))

    return factory
