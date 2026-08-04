"""Map downstream DataHub MCP lineage to typed remediation targets."""

from __future__ import annotations

import unicodedata
from collections.abc import Mapping, Sequence
from typing import Any

from datahub_context import DataHubContextPort, build_datahub_context_from_env
from remediation_models import ActionableTarget


BUSINESS_ACTION_PROPERTY = "aftershock.businessAction"
REMEDIATION_WEBHOOK_PROPERTY = "aftershock.remediationWebhook"
UNKNOWN_ENTITY_TYPE = "UNKNOWN"


class BlastRadiusMappingError(RuntimeError):
    """The lineage response contained a node that could not be identified."""


class BlastRadiusMapper:
    """Resolve downstream lineage and its structured remediation properties."""

    def __init__(self, context: DataHubContextPort | None = None) -> None:
        self.context = (
            context if context is not None else build_datahub_context_from_env()
        )

    async def get_targets(self, dataset_urn: str) -> list[ActionableTarget]:
        """Return every unique identifiable downstream node in lineage order.

        Detail lookup is intentionally batched and joined by URN because the MCP
        response order is not part of the mapper's contract. Nodes without a
        complete playbook remain in the result so execution can report them as
        skipped rather than hiding part of the blast radius.
        """

        lineage = await self.context.get_lineage(dataset_urn)
        lineage_entities = _lineage_entities(lineage)
        if not lineage_entities:
            return []

        unique_urns = list(dict.fromkeys(entity["urn"] for entity in lineage_entities))
        details = await self.context.get_entities(unique_urns)
        details_by_urn = _details_by_urn(details)

        targets: list[ActionableTarget] = []
        for lineage_entity in lineage_entities:
            urn = lineage_entity["urn"]
            detail = details_by_urn.get(urn)
            detail_is_usable = detail is not None and not _has_entity_error(detail)
            source = detail if detail_is_usable else None

            targets.append(
                ActionableTarget(
                    urn=urn,
                    entity_type=_entity_type(source, fallback=lineage_entity),
                    business_action=_structured_string(
                        source, BUSINESS_ACTION_PROPERTY
                    ),
                    remediation_webhook=_structured_string(
                        source, REMEDIATION_WEBHOOK_PROPERTY
                    ),
                )
            )
        return targets

    async def get_actionable_targets(self, dataset_urn: str) -> list[dict[str, Any]]:
        """Deprecated compatibility view for the pre-receipt action engine.

        New callers should consume :meth:`get_targets` and ``ActionableTarget``
        directly. This adapter can be removed when the listener is migrated.
        """

        targets = await self.get_targets(dataset_urn)
        return [
            {
                "entity": {
                    "urn": target.urn,
                    "type": target.entity_type,
                    "customProperties": {
                        "business_action": target.business_action,
                        "remediation_webhook": target.remediation_webhook,
                    },
                }
            }
            for target in targets
        ]


def _lineage_entities(payload: Mapping[str, Any]) -> list[dict[str, str]]:
    downstreams = payload.get("downstreams")
    if not isinstance(downstreams, Mapping):
        raise BlastRadiusMappingError("DataHub lineage response has no downstreams")

    search_results = downstreams.get("searchResults", [])
    if not isinstance(search_results, Sequence) or isinstance(
        search_results, (str, bytes)
    ):
        raise BlastRadiusMappingError("DataHub lineage searchResults is malformed")

    entities: list[dict[str, str]] = []
    seen_urns: set[str] = set()
    for index, result in enumerate(search_results):
        if not isinstance(result, Mapping):
            raise BlastRadiusMappingError(
                f"DataHub lineage result {index} is malformed"
            )
        entity = result.get("entity")
        if not isinstance(entity, Mapping):
            raise BlastRadiusMappingError(
                f"DataHub lineage result {index} has no entity"
            )
        urn = entity.get("urn")
        if not isinstance(urn, str) or not urn.strip():
            raise BlastRadiusMappingError(
                f"DataHub lineage result {index} has no valid URN"
            )
        if urn in seen_urns:
            continue
        seen_urns.add(urn)
        entity_type = entity.get("type")
        entities.append(
            {
                "urn": urn,
                "type": (
                    entity_type.strip()
                    if isinstance(entity_type, str) and entity_type.strip()
                    else UNKNOWN_ENTITY_TYPE
                ),
            }
        )
    return entities


def _details_by_urn(
    details: Sequence[Mapping[str, Any]],
) -> dict[str, Mapping[str, Any]]:
    by_urn: dict[str, Mapping[str, Any]] = {}
    ambiguous_urns: set[str] = set()
    for detail in details:
        if not isinstance(detail, Mapping):
            continue
        urn = detail.get("urn")
        if not isinstance(urn, str) or not urn or urn in ambiguous_urns:
            continue
        if urn in by_urn:
            # The MCP contract expects one detail per requested URN. A duplicate
            # cannot safely authorize either record's remediation endpoint.
            del by_urn[urn]
            ambiguous_urns.add(urn)
            continue
        by_urn[urn] = detail
    return by_urn


def _has_entity_error(detail: Mapping[str, Any]) -> bool:
    return bool(detail.get("error") or detail.get("errors"))


def _entity_type(
    detail: Mapping[str, Any] | None,
    *,
    fallback: Mapping[str, Any],
) -> str:
    if detail is not None:
        entity_type = detail.get("type")
        if isinstance(entity_type, str) and entity_type.strip():
            return entity_type.strip()
    fallback_type = fallback.get("type")
    if isinstance(fallback_type, str) and fallback_type.strip():
        return fallback_type.strip()
    return UNKNOWN_ENTITY_TYPE


def _structured_string(
    detail: Mapping[str, Any] | None,
    qualified_name: str,
) -> str | None:
    if detail is None:
        return None
    structured = detail.get("structuredProperties")
    if not isinstance(structured, Mapping):
        return None
    properties = structured.get("properties")
    if not isinstance(properties, Sequence) or isinstance(
        properties, (str, bytes)
    ):
        return None

    matching_properties: list[Mapping[str, Any]] = []
    for property_value in properties:
        if not isinstance(property_value, Mapping):
            continue
        structured_property = property_value.get("structuredProperty")
        if not isinstance(structured_property, Mapping):
            continue
        definition = structured_property.get("definition")
        if not isinstance(definition, Mapping):
            continue
        if definition.get("qualifiedName") != qualified_name:
            continue
        matching_properties.append(property_value)

    if len(matching_properties) != 1:
        return None
    values = matching_properties[0].get("values")
    if (
        not isinstance(values, Sequence)
        or isinstance(values, (str, bytes))
        or len(values) != 1
    ):
        return None
    value = values[0]
    if not isinstance(value, Mapping):
        return None
    string_value = value.get("stringValue")
    if (
        not isinstance(string_value, str)
        or not string_value
        or string_value != string_value.strip()
        or any(
            unicodedata.category(character).startswith("C")
            for character in string_value
        )
    ):
        return None
    return string_value
