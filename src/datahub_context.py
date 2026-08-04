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
_MAX_LINEAGE_PAGES = 100
_MAX_LINEAGE_RESULTS = 10_000
_CLIENT_TIMEOUT_SECONDS = 30.0
_CLIENT_INIT_TIMEOUT_SECONDS = 10.0
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
            if (
                isinstance(structured_content, dict)
                and set(structured_content) == {"result"}
            ):
                payload = structured_content["result"]
            else:
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
        seen_pages: list[list[Any]] = []
        expected_total: int | None = None

        for page_number in range(_MAX_LINEAGE_PAGES):
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

            downstreams, page_results, total, has_more = _parse_lineage_page(
                payload,
                expected_offset=offset,
            )
            if expected_total is None:
                expected_total = total
            elif total != expected_total:
                raise _tool_error(
                    "get_lineage", "returned inconsistent lineage metadata"
                )
            if page_results and any(
                page_results == prior_page for prior_page in seen_pages
            ):
                raise _tool_error(
                    "get_lineage", "returned a repeated lineage page"
                )
            if page_results:
                seen_pages.append(deepcopy(page_results))

            if total > _MAX_LINEAGE_RESULTS or (
                len(combined_results) + len(page_results)
                > _MAX_LINEAGE_RESULTS
            ):
                raise _tool_error(
                    "get_lineage", "exceeded the result safety limit"
                )

            if combined is None:
                combined = deepcopy(payload)
            combined_results.extend(page_results)

            combined_downstreams = combined.setdefault("downstreams", {})
            if not isinstance(combined_downstreams, dict):
                raise _tool_error(
                    "get_lineage", "returned an unsupported payload"
                )
            combined_downstreams.update(deepcopy(dict(downstreams)))
            combined_downstreams["searchResults"] = combined_results

            if not has_more:
                if total != len(combined_results):
                    raise _tool_error(
                        "get_lineage", "returned an incomplete lineage response"
                    )
                combined_downstreams.update(
                    {
                        "total": total,
                        "offset": 0,
                        "returned": len(combined_results),
                        "hasMore": False,
                    }
                )
                return combined
            if not page_results:
                raise _tool_error(
                    "get_lineage", "returned invalid pagination metadata"
                )
            if page_number + 1 >= _MAX_LINEAGE_PAGES:
                raise _tool_error(
                    "get_lineage", "exceeded the pagination safety limit"
                )
            offset += len(page_results)

        raise _tool_error("get_lineage", "exceeded the pagination safety limit")

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


def _parse_lineage_page(
    payload: Mapping[str, Any],
    *,
    expected_offset: int,
) -> tuple[Mapping[str, Any], list[Any], int, bool]:
    """Validate one official DataHub lineage page and return typed metadata."""

    downstreams = payload.get("downstreams")
    if not isinstance(downstreams, Mapping):
        raise _tool_error("get_lineage", "returned invalid lineage metadata")

    if "searchResults" not in downstreams:
        total = downstreams.get("total")
        offset = downstreams.get("offset", expected_offset)
        returned = downstreams.get("returned", 0)
        has_more = downstreams.get("hasMore", False)
        if (
            (
                "total" not in downstreams
                or not _is_nonnegative_int(total)
                or total != 0
            )
            or (
                "offset" in downstreams
                and (
                    not _is_nonnegative_int(offset)
                    or offset != expected_offset
                )
            )
            or (
                "returned" in downstreams
                and (not _is_nonnegative_int(returned) or returned != 0)
            )
            or not isinstance(has_more, bool)
            or has_more
        ):
            raise _tool_error("get_lineage", "returned invalid lineage metadata")
        return downstreams, [], 0, False

    search_results = downstreams["searchResults"]
    if not isinstance(search_results, Sequence) or isinstance(
        search_results, (str, bytes)
    ):
        raise _tool_error("get_lineage", "returned invalid lineage metadata")

    total = downstreams.get("total")
    offset = downstreams.get("offset")
    returned = downstreams.get("returned")
    has_more = downstreams.get("hasMore")
    if (
        not _is_nonnegative_int(total)
        or not _is_nonnegative_int(offset)
        or not _is_nonnegative_int(returned)
        or not isinstance(has_more, bool)
        or offset != expected_offset
        or returned != len(search_results)
        or total < offset + returned
    ):
        raise _tool_error("get_lineage", "returned invalid lineage metadata")

    if has_more and total <= offset + returned:
        raise _tool_error("get_lineage", "returned invalid lineage metadata")
    if not has_more and total > offset + returned:
        raise _tool_error("get_lineage", "returned an incomplete lineage response")

    return downstreams, list(search_results), total, has_more


def _is_nonnegative_int(value: Any) -> bool:
    return isinstance(value, int) and not isinstance(value, bool) and value >= 0


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
            return Client(
                transport,
                timeout=_CLIENT_TIMEOUT_SECONDS,
                init_timeout=_CLIENT_INIT_TIMEOUT_SECONDS,
            )

        return remote_factory

    gms_url = source.get("DATAHUB_GMS_URL")
    if not gms_url or not gms_url.strip():
        raise DataHubConfigurationError(
            "DATAHUB_GMS_URL must be explicitly set for local stdio MCP mode"
        )

    child_env = {
        key: source[key]
        for key in _PRESERVED_SUBPROCESS_ENV
        if source.get(key) is not None
    }
    child_env["DATAHUB_GMS_URL"] = gms_url
    if source.get("DATAHUB_GMS_TOKEN"):
        child_env["DATAHUB_GMS_TOKEN"] = source["DATAHUB_GMS_TOKEN"]
    child_env["DATAHUB_SKIP_CONFIG"] = "true"
    child_env["TOOLS_IS_MUTATION_ENABLED"] = "true"

    def stdio_factory() -> Client:
        transport = StdioTransport(
            command=sys.executable,
            args=["-m", "mcp_server_datahub", "--transport", "stdio"],
            env=dict(child_env),
            keep_alive=False,
        )
        return Client(
            transport,
            timeout=_CLIENT_TIMEOUT_SECONDS,
            init_timeout=_CLIENT_INIT_TIMEOUT_SECONDS,
        )

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
