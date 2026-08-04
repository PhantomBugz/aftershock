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


@dataclass
class MCPCallRecorder:
    """Mutable server state and an exact record of received tool arguments."""

    calls: list[tuple[str, dict[str, Any]]] = field(default_factory=list)
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
            "urn": "urn:li:document:aftershock-inc-9942",
            "message": "saved",
            "author": "urn:li:corpuser:test",
        }
    )
    fail_tool: str | None = None
    failure_message: str = "server failure containing super-secret-value"
    max_lineage_calls: int | None = None


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
        return recorder.lineage_pages[offset]

    @server.tool
    async def get_entities(
        urns: list[str],
    ) -> list[dict[str, Any]] | dict[str, Any]:
        arguments = {"urns": list(urns)}
        recorder.calls.append(("get_entities", arguments))
        if recorder.fail_tool == "get_entities":
            raise RuntimeError(recorder.failure_message)
        return recorder.entities_payload

    @server.tool
    async def save_document(
        document_type: str,
        title: str,
        content: str,
        urn: str | None,
        related_assets: list[str],
    ) -> dict[str, Any]:
        arguments = {
            "document_type": document_type,
            "title": title,
            "content": content,
            "urn": urn,
            "related_assets": list(related_assets),
        }
        recorder.calls.append(("save_document", arguments))
        if recorder.fail_tool == "save_document":
            raise RuntimeError(recorder.failure_message)
        return recorder.save_payload

    return server


def make_client_factory(recorder: MCPCallRecorder):
    """Return fresh connected clients for one shared in-process server."""

    server = build_test_server(recorder)

    def factory() -> Client:
        return Client(FastMCPTransport(server, raise_exceptions=True))

    return factory
