"""Resolve actionable downstream entities from a DataHub lineage graph."""

from __future__ import annotations

import json
import os
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

import httpx


DEFAULT_GRAPH_PATH = (
    Path(__file__).resolve().parents[1] / "mock-data" / "datahub_lineage.json"
)

DOWNSTREAM_LINEAGE_QUERY = """
query AftershockDownstreamLineage($urn: String!) {
  dataset(urn: $urn) {
    urn
    downstreamLineage: lineage(input: { direction: DOWNSTREAM }) {
      entities {
        urn
        type
        ... on DataJob {
          properties {
            customProperties {
              key
              value
            }
          }
        }
        ... on MLModel {
          properties {
            customProperties {
              key
              value
            }
          }
        }
      }
    }
  }
}
"""


class BlastRadiusMapper:
    """Resolve downstream entities from live DataHub or an offline fixture."""

    def __init__(
        self,
        graph_path: str | Path | None = None,
        *,
        http_client: httpx.AsyncClient | None = None,
        gms_url: str | None = None,
    ) -> None:
        self.graph_path = Path(graph_path) if graph_path else DEFAULT_GRAPH_PATH
        self.http_client = http_client
        self.gms_url = (
            os.getenv("DATAHUB_GMS_URL") if gms_url is None else gms_url
        )

    async def get_actionable_targets(self, dataset_urn: str) -> list[dict[str, Any]]:
        """Return normalized downstream entities for a dataset URN."""

        if self.gms_url:
            return await self._get_live_targets(dataset_urn)

        graph = json.loads(self.graph_path.read_text(encoding="utf-8"))
        dataset = graph["data"]["dataset"]
        if dataset["urn"] != dataset_urn:
            return []
        return dataset["downstreamLineage"]["entities"]

    async def _get_live_targets(self, dataset_urn: str) -> list[dict[str, Any]]:
        endpoint = f"{self.gms_url.rstrip('/')}/api/graphql"
        payload = {
            "query": DOWNSTREAM_LINEAGE_QUERY,
            "variables": {"urn": dataset_urn},
        }

        if self.http_client is not None:
            response = await self.http_client.post(endpoint, json=payload)
        else:
            async with httpx.AsyncClient(timeout=10.0) as client:
                response = await client.post(endpoint, json=payload)

        response.raise_for_status()
        body = response.json()
        if body.get("errors"):
            raise RuntimeError(f"DataHub GraphQL query failed: {body['errors']}")

        dataset = body.get("data", {}).get("dataset")
        if not isinstance(dataset, Mapping):
            return []

        lineage = dataset.get("downstreamLineage")
        if not isinstance(lineage, Mapping):
            return []

        entities = lineage.get("entities", [])
        if not isinstance(entities, Sequence) or isinstance(entities, (str, bytes)):
            return []

        return [
            {"entity": self._normalize_entity(entity)}
            for entity in entities
            if isinstance(entity, Mapping)
        ]

    @staticmethod
    def _normalize_entity(entity: Mapping[str, Any]) -> dict[str, Any]:
        properties = entity.get("properties")
        if isinstance(properties, Mapping):
            raw_custom_properties = properties.get("customProperties", [])
        else:
            raw_custom_properties = entity.get("customProperties", [])

        return {
            "urn": entity.get("urn", "UNKNOWN_URN"),
            "type": entity.get("type", "UNKNOWN"),
            "customProperties": BlastRadiusMapper._normalize_custom_properties(
                raw_custom_properties
            ),
        }

    @staticmethod
    def _normalize_custom_properties(raw_properties: Any) -> dict[str, Any]:
        if isinstance(raw_properties, Mapping):
            return dict(raw_properties)
        if not isinstance(raw_properties, Sequence) or isinstance(
            raw_properties, (str, bytes)
        ):
            return {}

        normalized: dict[str, Any] = {}
        for item in raw_properties:
            if not isinstance(item, Mapping):
                continue
            key = item.get("key")
            if key is not None:
                normalized[str(key)] = item.get("value")
        return normalized
