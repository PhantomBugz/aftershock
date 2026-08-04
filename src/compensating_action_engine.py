"""Execute compensating playbooks for entities in a DataHub blast radius."""

from __future__ import annotations

import asyncio
import json
import logging
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

import httpx


logger = logging.getLogger("Aftershock-Engine")


class CompensatingActionEngine:
    """Translate DataHub entity properties into asynchronous rollback requests."""

    def __init__(self, http_client: httpx.AsyncClient | None = None) -> None:
        self.http_client = http_client or httpx.AsyncClient(timeout=10.0)
        self._owns_client = http_client is None

    async def aclose(self) -> None:
        """Close the internally-created HTTP client, when one is in use."""

        if self._owns_client:
            await self.http_client.aclose()

    async def execute_rollback(
        self, affected_entity: Mapping[str, Any], incident_id: str
    ) -> bool:
        """Invoke the remediation webhook declared on one DataHub entity."""

        urn = str(affected_entity.get("urn", "UNKNOWN_URN"))
        props = affected_entity.get("customProperties", {})
        if not isinstance(props, Mapping):
            logger.warning("Invalid customProperties for %s", urn)
            return False

        action_type = props.get("business_action")
        webhook_url = props.get("remediation_webhook")
        if not action_type or not webhook_url:
            logger.warning("No remediation playbook defined for %s", urn)
            return False

        logger.info(
            "Initiating rollback playbook [%s] for entity: %s", action_type, urn
        )
        payload = {
            "incident_id": incident_id,
            "target_urn": urn,
            "action": "REVERT_STATE",
        }

        try:
            response = await self.http_client.post(str(webhook_url), json=payload)
            response.raise_for_status()
        except httpx.HTTPError as exc:
            logger.error(
                "FAILED: Remediation endpoint for %s returned an error: %s", urn, exc
            )
            return False

        logger.info("SUCCESS: Remediation '%s' completed for %s", action_type, urn)
        return True

    async def process_blast_radius(
        self, downstream_entities: Sequence[Mapping[str, Any]], incident_id: str
    ) -> list[bool]:
        """Concurrently execute rollback playbooks for downstream entities."""

        logger.info("Processing Aftershock blast radius for Incident %s...", incident_id)
        tasks = []
        for wrapped_entity in downstream_entities:
            entity = wrapped_entity.get("entity")
            if not isinstance(entity, Mapping):
                logger.warning("Skipping malformed downstream entity: %s", wrapped_entity)
                tasks.append(asyncio.sleep(0, result=False))
                continue
            tasks.append(self.execute_rollback(entity, incident_id))

        results = list(await asyncio.gather(*tasks))
        success_count = sum(results)
        logger.info(
            "Aftershock Remediation Complete. %d/%d actions rolled back.",
            success_count,
            len(downstream_entities),
        )
        return results


async def _run_demo() -> list[bool]:
    """Run real async HTTP requests against an in-process remediation transport."""

    async def demo_remediation_service(request: httpx.Request) -> httpx.Response:
        payload = json.loads(request.content)
        logger.info(
            "DEMO ENDPOINT: %s %s payload=%s",
            request.method,
            request.url.path,
            payload,
        )
        return httpx.Response(200, json={"status": "accepted"})

    project_root = Path(__file__).resolve().parents[1]
    fixture_path = project_root / "mock-data" / "datahub_lineage.json"
    graph = json.loads(fixture_path.read_text(encoding="utf-8"))
    entities = graph["data"]["dataset"]["downstreamLineage"]["entities"]

    transport = httpx.MockTransport(demo_remediation_service)
    async with httpx.AsyncClient(transport=transport, timeout=10.0) as client:
        engine = CompensatingActionEngine(http_client=client)
        return await engine.process_blast_radius(entities, incident_id="INC-9942")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")
    asyncio.run(_run_demo())
