"""DataHub context adapters backed by FastMCP or an explicit local fixture."""

from __future__ import annotations

import json
import os
import sys
from collections.abc import Callable, Mapping, Sequence
from copy import deepcopy
from pathlib import Path
from typing import Any, Protocol, runtime_checkable

import httpx
from fastmcp import Client
from fastmcp.client.transports import StdioTransport, StreamableHttpTransport


DEFAULT_FIXTURE_PATH = (
    Path(__file__).resolve().parents[1] / "mock-data" / "datahub_lineage.json"
)
_PAGE_SIZE = 100
_PRESERVED_SUBPROCESS_ENV = (
    "PATH",
    "PATHEXT",
    "SYSTEMROOT",
    "WINDIR",
    "COMSPEC",
    "TEMP",
    "TMP",
    "HOME",
    "USERPROFILE",
    "VIRTUAL_ENV",
    "PYTHONHOME",
    "PYTHONPATH",
    "LANG",
    "LC_ALL",
    "SSL_CERT_FILE",
    "REQUESTS_CA_BUNDLE",
    "HTTP_PROXY",
    "HTTPS_PROXY",
    "NO_PROXY",
)

ClientFactory = Callable[[], Client]


class DataHubMCPError(RuntimeError):
    """A sanitized MCP connection, protocol, tool, or payload failure."""


class DataHubConfigurationError(RuntimeError):
    """Invalid or missing Aftershock DataHub context configuration."""


@runtime_checkable
class DataHubContextPort(Protocol):
    """DataHub reads and write-back needed by the incident workflow."""

    mode: str

    async def get_lineage(self, dataset_urn: str) -> dict[str, Any]:
        """Return downstream lineage for one dataset."""

    async def get_entities(
        self, urns: Sequence[str]
    ) -> list[dict[str, Any]]:
        """Return entity details for a batch of URNs."""

    async def save_document(
        self,
        *,
        document_type: str,
        title: str,
        content: str,
        urn: str | None,
        related_assets: Sequence[str],
    ) -> dict[str, Any]:
        """Persist an incident document and return the tool receipt."""


def _tool_error(tool_name: str, detail: str) -> DataHubMCPError:
    """Build an error containing only controlled, non-secret text."""

    return DataHubMCPError(f"DataHub MCP tool {tool_name!r} {detail}")


def parse_tool_result(result: Any, *, tool_name: str) -> dict[str, Any] | list[Any]:
    """Extract a JSON collection from a FastMCP call result.

    FastMCP's decoded ``data`` wins over protocol structured content. A
    single text block is accepted only when it contains a JSON object or list.
    Error content is intentionally omitted from exceptions because servers can
    echo request credentials in diagnostic messages.
    """

    if isinstance(result, (dict, list)):
        return result

    if bool(getattr(result, "is_error", False)):
        raise _tool_error(tool_name, "returned an error")

    data = getattr(result, "data", None)
    if data is not None:
        payload = data
    else:
        structured_content = getattr(result, "structured_content", None)
        if structured_content is not None:
            payload = structured_content
        else:
            content = getattr(result, "content", None)
            if not isinstance(content, Sequence) or isinstance(
                content, (str, bytes)
            ) or len(content) != 1:
                raise _tool_error(tool_name, "returned an unsupported payload")

            block = content[0]
            if isinstance(block, Mapping):
                block_type = block.get("type")
                text = block.get("text")
            else:
                block_type = getattr(block, "type", None)
                text = getattr(block, "text", None)
            if block_type != "text" or not isinstance(text, str):
                raise _tool_error(tool_name, "returned an unsupported payload")
            try:
                payload = json.loads(text)
            except (TypeError, ValueError):
                raise _tool_error(tool_name, "returned invalid JSON") from None

    if not isinstance(payload, (dict, list)):
        raise _tool_error(tool_name, "returned an unsupported payload")
    return payload


class MCPDataHubContext:
    """DataHub context implementation that invokes official MCP tools."""

    mode = "mcp"

    def __init__(self, *, client_factory: ClientFactory) -> None:
        self._client_factory = client_factory

    async def _call_tool(
        self, tool_name: str, arguments: dict[str, Any]
    ) -> dict[str, Any] | list[Any]:
        try:
            async with self._client_factory() as client:
                result = await client.call_tool(
                    tool_name,
                    arguments,
                    raise_on_error=False,
                )
        except Exception:
            raise _tool_error(tool_name, "failed") from None
        return parse_tool_result(result, tool_name=tool_name)

    async def get_lineage(self, dataset_urn: str) -> dict[str, Any]:
        """Fetch available lineage pages with upstream traversal disabled."""

        offset = 0
        combined: dict[str, Any] | None = None
        combined_results: list[Any] = []

        while True:
            payload = await self._call_tool(
                "get_lineage",
                {
                    "urn": dataset_urn,
                    "upstream": False,
                    "max_hops": 3,
                    "max_results": _PAGE_SIZE,
                    "offset": offset,
                },
            )
            if not isinstance(payload, dict):
                raise _tool_error("get_lineage", "returned an unsupported payload")

            downstreams = payload.get("downstreams")
            page_result_count = 0
            if combined is None:
                combined = deepcopy(payload)
            if isinstance(downstreams, Mapping):
                search_results = downstreams.get("searchResults", [])
                if isinstance(search_results, Sequence) and not isinstance(
                    search_results, (str, bytes)
                ):
                    page_result_count = len(search_results)
                    combined_results.extend(search_results)

                combined_downstreams = combined.setdefault("downstreams", {})
                if not isinstance(combined_downstreams, dict):
                    raise _tool_error(
                        "get_lineage", "returned an unsupported payload"
                    )
                combined_downstreams.update(deepcopy(dict(downstreams)))
                combined_downstreams["searchResults"] = combined_results

            if not _lineage_has_more(payload, offset=offset):
                return combined
            if page_result_count == 0:
                raise _tool_error(
                    "get_lineage", "returned invalid pagination metadata"
                )
            offset += page_result_count

    async def get_entities(
        self, urns: Sequence[str]
    ) -> list[dict[str, Any]]:
        """Fetch entity details in one MCP batch."""

        payload = await self._call_tool("get_entities", {"urns": list(urns)})
        if isinstance(payload, dict):
            return [payload]
        if isinstance(payload, list) and all(
            isinstance(entity, dict) for entity in payload
        ):
            return payload
        raise _tool_error("get_entities", "returned an unsupported payload")

    async def save_document(
        self,
        *,
        document_type: str,
        title: str,
        content: str,
        urn: str | None,
        related_assets: Sequence[str],
    ) -> dict[str, Any]:
        """Persist a DataHub document without sending unseeded topic tags."""

        payload = await self._call_tool(
            "save_document",
            {
                "document_type": document_type,
                "title": title,
                "content": content,
                "urn": urn,
                "related_assets": list(related_assets),
            },
        )
        if not isinstance(payload, dict):
            raise _tool_error("save_document", "returned an unsupported payload")
        if payload.get("success") is False:
            raise _tool_error("save_document", "reported failure")
        return payload


def _lineage_has_more(payload: Mapping[str, Any], *, offset: int) -> bool:
    downstreams = payload.get("downstreams")
    if not isinstance(downstreams, Mapping):
        return False

    for key in ("hasMore", "has_more"):
        value = downstreams.get(key)
        if isinstance(value, bool):
            return value

    page_info = downstreams.get("pageInfo")
    if isinstance(page_info, Mapping):
        value = page_info.get("hasNextPage")
        if isinstance(value, bool):
            return value

    total = downstreams.get("total")
    search_results = downstreams.get("searchResults")
    if isinstance(total, int) and isinstance(search_results, Sequence) and not isinstance(
        search_results, (str, bytes)
    ):
        return offset + len(search_results) < total
    return False


class FixtureDataHubContext:
    """Deterministic offline context with in-memory document recording."""

    mode = "fixture"

    def __init__(self, fixture_path: str | Path = DEFAULT_FIXTURE_PATH) -> None:
        self.fixture_path = Path(fixture_path)
        self._fixture = json.loads(self.fixture_path.read_text(encoding="utf-8"))
        self.saved_documents: list[dict[str, Any]] = []

    def _dataset(self) -> Mapping[str, Any] | None:
        data = self._fixture.get("data")
        if not isinstance(data, Mapping):
            return None
        dataset = data.get("dataset")
        return dataset if isinstance(dataset, Mapping) else None

    def _search_results(self) -> list[dict[str, Any]]:
        dataset = self._dataset()
        if dataset is None:
            return []
        lineage = dataset.get("downstreamLineage")
        if not isinstance(lineage, Mapping):
            return []
        entities = lineage.get("entities", [])
        if not isinstance(entities, Sequence) or isinstance(entities, (str, bytes)):
            return []

        results: list[dict[str, Any]] = []
        for item in entities:
            if not isinstance(item, Mapping):
                continue
            entity = item.get("entity")
            if isinstance(entity, Mapping):
                results.append({"entity": deepcopy(dict(entity))})
            else:
                results.append({"entity": deepcopy(dict(item))})
        return results

    async def get_lineage(self, dataset_urn: str) -> dict[str, Any]:
        """Return downstream lineage from the explicit JSON fixture."""

        dataset = self._dataset()
        results = (
            self._search_results()
            if dataset is not None and dataset.get("urn") == dataset_urn
            else []
        )
        return {
            "downstreams": {
                "searchResults": results,
                "total": len(results),
                "hasMore": False,
            }
        }

    async def get_entities(
        self, urns: Sequence[str]
    ) -> list[dict[str, Any]]:
        """Return fixture entity details in the caller's requested order."""

        entities_by_urn = {
            result["entity"]["urn"]: result["entity"]
            for result in self._search_results()
            if isinstance(result.get("entity"), Mapping)
            and isinstance(result["entity"].get("urn"), str)
        }
        return [
            deepcopy(entities_by_urn[urn])
            for urn in urns
            if urn in entities_by_urn
        ]

    async def save_document(
        self,
        *,
        document_type: str,
        title: str,
        content: str,
        urn: str | None,
        related_assets: Sequence[str],
    ) -> dict[str, Any]:
        """Record a write-back call in memory and return a fixture receipt."""

        resolved_urn = urn or "urn:li:document:aftershock-fixture"
        self.saved_documents.append(
            {
                "document_type": document_type,
                "title": title,
                "content": content,
                "urn": urn,
                "related_assets": list(related_assets),
            }
        )
        return {
            "success": True,
            "urn": resolved_urn,
            "message": "Recorded by the in-memory fixture recorder",
            "author": "aftershock-fixture",
        }


def _new_httpx_client(**kwargs: Any) -> httpx.AsyncClient:
    """Provide the HTTP client FastMCP owns and closes for remote sessions."""

    return httpx.AsyncClient(**kwargs)


def build_mcp_client_factory(
    environ: Mapping[str, str] | None = None,
) -> ClientFactory:
    """Build a live FastMCP client factory for HTTP or local stdio."""

    source = dict(os.environ if environ is None else environ)
    mcp_url = source.get("DATAHUB_MCP_URL")
    if mcp_url:
        headers: dict[str, str] = {}
        mcp_token = source.get("DATAHUB_MCP_TOKEN")
        if mcp_token:
            headers["Authorization"] = f"Bearer {mcp_token}"

        def remote_factory() -> Client:
            transport = StreamableHttpTransport(
                url=mcp_url,
                headers=dict(headers),
                httpx_client_factory=_new_httpx_client,
            )
            return Client(transport)

        return remote_factory

    child_env = {
        key: source[key]
        for key in _PRESERVED_SUBPROCESS_ENV
        if source.get(key) is not None
    }
    for key in ("DATAHUB_GMS_URL", "DATAHUB_GMS_TOKEN"):
        if source.get(key):
            child_env[key] = source[key]
    child_env["TOOLS_IS_MUTATION_ENABLED"] = "true"

    def stdio_factory() -> Client:
        transport = StdioTransport(
            command=sys.executable,
            args=["-m", "mcp_server_datahub", "--transport", "stdio"],
            env=dict(child_env),
            keep_alive=False,
        )
        return Client(transport)

    return stdio_factory


def build_datahub_context_from_env() -> DataHubContextPort:
    """Select an explicit fixture or live MCP context; never auto-fallback."""

    mode = os.environ.get("AFTERSHOCK_DATAHUB_MODE")
    if mode == "fixture":
        return FixtureDataHubContext()
    if mode == "mcp":
        return MCPDataHubContext(client_factory=build_mcp_client_factory())
    raise DataHubConfigurationError(
        "AFTERSHOCK_DATAHUB_MODE must be explicitly set to 'fixture' or 'mcp'"
    )
