"""DataHub context adapters backed by FastMCP or an explicit local fixture."""

from __future__ import annotations

import ipaddress
import json
import os
import re
import sys
import unicodedata
from collections.abc import Callable, Mapping, Sequence
from copy import deepcopy
from pathlib import Path
from typing import Any, Protocol, runtime_checkable
from urllib.parse import urlsplit

import httpx
from fastmcp import Client
from fastmcp.client.transports import StdioTransport, StreamableHttpTransport


DEFAULT_FIXTURE_PATH = (
    Path(__file__).resolve().parents[1] / "mock-data" / "datahub_lineage.json"
)
_MAX_LINEAGE_PAGES = 100
_MAX_LINEAGE_RESULTS = 10_000
_MAX_DOCUMENT_SEARCH_RESULTS = 50
_MAX_GREP_DOCUMENTS = 50
_MAX_GREP_PATTERN_LENGTH = 4_096
_MAX_GREP_CONTEXT_CHARS = 8_000
_MAX_GREP_MATCHES_PER_DOCUMENT = 20
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

    async def search_documents(
        self,
        *,
        query: str,
        num_results: int = 10,
        offset: int = 0,
    ) -> dict[str, Any]:
        """Return validated DataHub document search metadata."""

    async def grep_documents(
        self,
        *,
        urns: Sequence[str],
        pattern: str,
        context_chars: int = 200,
        max_matches_per_doc: int = 5,
        start_offset: int = 0,
    ) -> dict[str, Any]:
        """Return validated excerpts from DataHub document content."""

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
                    # mcp-server-datahub 0.6.0 fetches from GraphQL at start=0
                    # before applying ``offset``. Fetch the full bounded set so
                    # later offset pages cannot be hidden by the server cap.
                    "max_results": _MAX_LINEAGE_RESULTS,
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

    async def search_documents(
        self,
        *,
        query: str,
        num_results: int = 10,
        offset: int = 0,
    ) -> dict[str, Any]:
        """Search document metadata through the pinned DataHub MCP tool."""

        _validate_document_search_arguments(query, num_results, offset)
        payload = await self._call_tool(
            "search_documents",
            {
                "query": query,
                "num_results": num_results,
                "offset": offset,
            },
        )
        return _validated_document_search_payload(
            payload,
            expected_offset=offset,
            requested_count=num_results,
        )

    async def grep_documents(
        self,
        *,
        urns: Sequence[str],
        pattern: str,
        context_chars: int = 200,
        max_matches_per_doc: int = 5,
        start_offset: int = 0,
    ) -> dict[str, Any]:
        """Search document content through the pinned DataHub MCP tool."""

        document_urns = _validate_grep_arguments(
            urns,
            pattern,
            context_chars,
            max_matches_per_doc,
            start_offset,
        )
        payload = await self._call_tool(
            "grep_documents",
            {
                "urns": document_urns,
                "pattern": pattern,
                "context_chars": context_chars,
                "max_matches_per_doc": max_matches_per_doc,
                "start_offset": start_offset,
            },
        )
        return _validated_grep_documents_payload(
            payload,
            minimum_position=start_offset,
        )

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

        arguments: dict[str, Any] = {
            "document_type": document_type,
            "title": title,
            "content": content,
            "related_assets": list(related_assets),
        }
        if urn is not None:
            arguments["urn"] = urn
        payload = await self._call_tool(
            "save_document",
            arguments,
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


def _is_document_urn(value: object) -> bool:
    return (
        isinstance(value, str)
        and value.startswith("urn:li:document:")
        and len(value) > len("urn:li:document:")
        and not any(
            character.isspace()
            or unicodedata.category(character).startswith("C")
            for character in value
        )
    )


def _validate_document_search_arguments(
    query: object,
    num_results: object,
    offset: object,
) -> None:
    if (
        not isinstance(query, str)
        or not query.strip()
        or not _is_nonnegative_int(num_results)
        or not 1 <= num_results <= _MAX_DOCUMENT_SEARCH_RESULTS
        or not _is_nonnegative_int(offset)
    ):
        raise _tool_error("search_documents", "received invalid arguments")


def _validated_document_search_payload(
    payload: object,
    *,
    expected_offset: int,
    requested_count: int,
) -> dict[str, Any]:
    if not isinstance(payload, dict):
        raise _tool_error("search_documents", "returned an unsupported payload")

    normalized = deepcopy(payload)
    if "searchResults" not in payload:
        total = payload.get("total")
        count = payload.get("count")
        if (
            not _is_nonnegative_int(total)
            or expected_offset < total
            or ("count" in payload and count != requested_count)
        ):
            raise _tool_error(
                "search_documents", "returned invalid search metadata"
            )
        normalized["searchResults"] = []

    search_results = normalized.get("searchResults")
    if not isinstance(search_results, list):
        raise _tool_error("search_documents", "returned invalid search metadata")
    if len(search_results) > requested_count:
        raise _tool_error("search_documents", "returned invalid search metadata")

    seen_urns: set[str] = set()
    for result in search_results:
        if not isinstance(result, Mapping):
            raise _tool_error(
                "search_documents", "returned invalid search metadata"
            )
        entity = result.get("entity")
        if not isinstance(entity, Mapping):
            raise _tool_error(
                "search_documents", "returned invalid search metadata"
            )
        urn = entity.get("urn")
        info = entity.get("info")
        title = info.get("title") if isinstance(info, Mapping) else None
        if (
            not _is_document_urn(urn)
            or urn in seen_urns
            or not isinstance(title, str)
            or not title.strip()
        ):
            raise _tool_error(
                "search_documents", "returned invalid search metadata"
            )
        seen_urns.add(urn)

    start = normalized.get("start")
    count = normalized.get("count")
    total = normalized.get("total")
    if "start" in normalized and (
        not _is_nonnegative_int(start) or start != expected_offset
    ):
        raise _tool_error("search_documents", "returned invalid search metadata")
    if "count" in normalized and (
        not _is_nonnegative_int(count) or count != requested_count
    ):
        raise _tool_error("search_documents", "returned invalid search metadata")
    if "total" in normalized and (
        not _is_nonnegative_int(total)
        or (
            bool(search_results)
            and total < expected_offset + len(search_results)
        )
        or (not search_results and expected_offset < total)
    ):
        raise _tool_error("search_documents", "returned invalid search metadata")
    return normalized


def _validate_grep_arguments(
    urns: object,
    pattern: object,
    context_chars: object,
    max_matches_per_doc: object,
    start_offset: object,
) -> list[str]:
    if (
        not isinstance(urns, Sequence)
        or isinstance(urns, (str, bytes))
        or not 1 <= len(urns) <= _MAX_GREP_DOCUMENTS
        or not all(_is_document_urn(urn) for urn in urns)
        or not isinstance(pattern, str)
        or not pattern
        or len(pattern) > _MAX_GREP_PATTERN_LENGTH
        or not _is_nonnegative_int(context_chars)
        or context_chars > _MAX_GREP_CONTEXT_CHARS
        or not _is_nonnegative_int(max_matches_per_doc)
        or not 1 <= max_matches_per_doc <= _MAX_GREP_MATCHES_PER_DOCUMENT
        or not _is_nonnegative_int(start_offset)
    ):
        raise _tool_error("grep_documents", "received invalid arguments")
    return list(urns)


def _validated_grep_documents_payload(
    payload: object,
    *,
    minimum_position: int,
) -> dict[str, Any]:
    if not isinstance(payload, dict) or "error" in payload:
        raise _tool_error("grep_documents", "returned an unsupported payload")

    results = payload.get("results")
    total_matches = payload.get("total_matches")
    documents_with_matches = payload.get("documents_with_matches")
    if (
        not isinstance(results, list)
        or not _is_nonnegative_int(total_matches)
        or not _is_nonnegative_int(documents_with_matches)
        or documents_with_matches != len(results)
    ):
        raise _tool_error("grep_documents", "returned invalid match metadata")

    seen_urns: set[str] = set()
    observed_total = 0
    for result in results:
        if not isinstance(result, Mapping):
            raise _tool_error("grep_documents", "returned invalid match metadata")
        urn = result.get("urn")
        title = result.get("title")
        matches = result.get("matches")
        document_total = result.get("total_matches")
        if (
            not _is_document_urn(urn)
            or urn in seen_urns
            or not isinstance(title, str)
            or not title.strip()
            or not isinstance(matches, list)
            or not matches
            or not _is_nonnegative_int(document_total)
            or document_total < len(matches)
        ):
            raise _tool_error("grep_documents", "returned invalid match metadata")
        content_length = result.get("content_length")
        if "content_length" in result and not _is_nonnegative_int(content_length):
            raise _tool_error("grep_documents", "returned invalid match metadata")
        for match in matches:
            if not isinstance(match, Mapping):
                raise _tool_error(
                    "grep_documents", "returned invalid match metadata"
                )
            excerpt = match.get("excerpt")
            position = match.get("position")
            if (
                not isinstance(excerpt, str)
                or not _is_nonnegative_int(position)
                or position < minimum_position
            ):
                raise _tool_error(
                    "grep_documents", "returned invalid match metadata"
                )
        seen_urns.add(urn)
        observed_total += document_total

    if total_matches != observed_total:
        raise _tool_error("grep_documents", "returned invalid match metadata")
    return payload


class FixtureDataHubContext:
    """Deterministic offline context with in-memory document recording."""

    mode = "fixture"

    def __init__(self, fixture_path: str | Path = DEFAULT_FIXTURE_PATH) -> None:
        self.fixture_path = Path(fixture_path)
        self._fixture = json.loads(self.fixture_path.read_text(encoding="utf-8"))
        self.saved_documents: list[dict[str, Any]] = []
        self._saved_document_records: list[dict[str, Any]] = []
        self._created_document_count = 0

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

        entities_by_urn: dict[str, dict[str, Any]] = {
            result["entity"]["urn"]: result["entity"]
            for result in self._search_results()
            if isinstance(result.get("entity"), Mapping)
            and isinstance(result["entity"].get("urn"), str)
        }
        dataset = self._dataset()
        if dataset is not None and isinstance(dataset.get("urn"), str):
            entities_by_urn[dataset["urn"]] = {
                "urn": dataset["urn"],
                "type": "DATASET",
            }
        for document in self._saved_document_records:
            entities_by_urn[document["urn"]] = {
                "urn": document["urn"],
                "type": "DOCUMENT",
            }

        entities: list[dict[str, Any]] = []
        for urn in urns:
            if urn not in entities_by_urn:
                continue
            entity = deepcopy(entities_by_urn[urn])
            related_documents = [
                {
                    "urn": document["urn"],
                    "type": "DOCUMENT",
                    "info": {"title": document["title"]},
                }
                for document in self._saved_document_records
                if urn in document["related_assets"]
            ]
            if related_documents:
                entity["relatedDocuments"] = {
                    "start": 0,
                    "count": len(related_documents),
                    "total": len(related_documents),
                    "documents": related_documents,
                }
            entities.append(entity)
        return entities

    async def search_documents(
        self,
        *,
        query: str,
        num_results: int = 10,
        offset: int = 0,
    ) -> dict[str, Any]:
        """Search fixture documents by title using DataHub's result shape."""

        _validate_document_search_arguments(query, num_results, offset)
        normalized_query = query.casefold()
        matches = [
            document
            for document in self._saved_document_records
            if query == "*" or normalized_query in document["title"].casefold()
        ]
        page = matches[offset : offset + num_results]
        payload = {
            "start": offset,
            "count": num_results,
            "total": len(matches),
            "searchResults": [
                {
                    "entity": {
                        "urn": document["urn"],
                        "subType": document["document_type"],
                        "info": {"title": document["title"]},
                    }
                }
                for document in page
            ],
        }
        return _validated_document_search_payload(
            payload,
            expected_offset=offset,
            requested_count=num_results,
        )

    async def grep_documents(
        self,
        *,
        urns: Sequence[str],
        pattern: str,
        context_chars: int = 200,
        max_matches_per_doc: int = 5,
        start_offset: int = 0,
    ) -> dict[str, Any]:
        """Search fixture document content using DataHub's result shape."""

        document_urns = _validate_grep_arguments(
            urns,
            pattern,
            context_chars,
            max_matches_per_doc,
            start_offset,
        )
        try:
            expression = re.compile(pattern)
        except re.error:
            raise _tool_error("grep_documents", "received an invalid pattern") from None

        results: list[dict[str, Any]] = []
        total_matches = 0
        for document in self._saved_document_records:
            if document["urn"] not in document_urns:
                continue
            content = document["content"]
            if start_offset >= len(content):
                continue
            searchable = content[start_offset:]
            found = list(expression.finditer(searchable))
            if not found:
                continue
            excerpts: list[dict[str, Any]] = []
            for match in found[:max_matches_per_doc]:
                excerpt_start = max(0, match.start() - context_chars)
                excerpt_end = min(len(searchable), match.end() + context_chars)
                excerpt = searchable[excerpt_start:excerpt_end]
                if excerpt_start > 0:
                    excerpt = "..." + excerpt
                if excerpt_end < len(searchable):
                    excerpt += "..."
                excerpts.append(
                    {
                        "excerpt": excerpt,
                        "position": match.start() + start_offset,
                    }
                )
            entry: dict[str, Any] = {
                "urn": document["urn"],
                "title": document["title"],
                "matches": excerpts,
                "total_matches": len(found),
            }
            if start_offset > 0:
                entry["content_length"] = len(content)
            results.append(entry)
            total_matches += len(found)
        payload = {
            "results": results,
            "total_matches": total_matches,
            "documents_with_matches": len(results),
        }
        return _validated_grep_documents_payload(
            payload,
            minimum_position=start_offset,
        )

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

        if urn is None:
            self._created_document_count += 1
            suffix = (
                ""
                if self._created_document_count == 1
                else f"-{self._created_document_count}"
            )
            resolved_urn = f"urn:li:document:aftershock-fixture{suffix}"
        else:
            resolved_urn = urn
        self.saved_documents.append(
            {
                "document_type": document_type,
                "title": title,
                "content": content,
                "urn": urn,
                "related_assets": list(related_assets),
            }
        )
        record = {
            "document_type": document_type,
            "title": title,
            "content": content,
            "urn": resolved_urn,
            "related_assets": list(related_assets),
        }
        if urn is None:
            self._saved_document_records.append(record)
        else:
            for index, existing in enumerate(self._saved_document_records):
                if existing["urn"] == resolved_urn:
                    self._saved_document_records[index] = record
                    break
            else:
                self._saved_document_records.append(record)
        return {
            "success": True,
            "urn": resolved_urn,
            "message": "Recorded by the in-memory fixture recorder",
            "author": "aftershock-fixture",
        }


def _new_httpx_client(**kwargs: Any) -> httpx.AsyncClient:
    """Provide the HTTP client FastMCP owns and closes for remote sessions."""

    return httpx.AsyncClient(**kwargs)


def _validated_credential_url(value: object, *, variable_name: str) -> str:
    """Validate an HTTP endpoint before attaching bearer credentials."""

    error = DataHubConfigurationError(
        f"{variable_name} must be HTTPS, or HTTP on a loopback host"
    )
    if not isinstance(value, str) or not value or any(
        character.isspace()
        or unicodedata.category(character).startswith("C")
        for character in value
    ):
        raise error

    try:
        parsed = urlsplit(value)
        port = parsed.port
    except (TypeError, ValueError):
        raise error from None

    if (
        parsed.scheme not in {"http", "https"}
        or not parsed.netloc
        or parsed.hostname is None
        or parsed.username is not None
        or parsed.password is not None
        or "#" in value
    ):
        raise error

    # ``urlsplit`` accepts an explicitly empty port, but it is not a valid
    # endpoint authority for this credential-bearing transport.
    authority = parsed.netloc.rsplit("@", 1)[-1]
    if authority.endswith(":") or port is not None and port <= 0:
        raise error

    if parsed.scheme == "http":
        hostname = parsed.hostname.casefold()
        if hostname != "localhost":
            try:
                is_loopback = ipaddress.ip_address(hostname).is_loopback
            except ValueError:
                is_loopback = False
            if not is_loopback:
                raise error

    return value


def build_mcp_client_factory(
    environ: Mapping[str, str] | None = None,
) -> ClientFactory:
    """Build a live FastMCP client factory for HTTP or local stdio."""

    source = dict(os.environ if environ is None else environ)
    mcp_url = source.get("DATAHUB_MCP_URL")
    if mcp_url:
        mcp_url = _validated_credential_url(
            mcp_url,
            variable_name="DATAHUB_MCP_URL",
        )
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
    gms_url = _validated_credential_url(
        gms_url,
        variable_name="DATAHUB_GMS_URL",
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
            log_file=Path(os.devnull),
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
