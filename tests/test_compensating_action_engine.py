import asyncio
import json
from pathlib import Path

import httpx

from compensating_action_engine import CompensatingActionEngine


FIXTURE_PATH = (
    Path(__file__).resolve().parents[1] / "mock-data" / "datahub_lineage.json"
)


def test_processes_each_downstream_entity_with_real_async_posts() -> None:
    graph = json.loads(FIXTURE_PATH.read_text(encoding="utf-8"))
    entities = graph["data"]["dataset"]["downstreamLineage"]["entities"]
    requests: list[httpx.Request] = []

    async def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(200, json={"status": "accepted"})

    async def scenario() -> list[bool]:
        async with httpx.AsyncClient(
            transport=httpx.MockTransport(handler), timeout=10.0
        ) as client:
            engine = CompensatingActionEngine(http_client=client)
            return await engine.process_blast_radius(entities, "INC-9942")

    results = asyncio.run(scenario())

    assert results == [True, True]
    assert {request.url.path for request in requests} == {
        "/remediate/cancel_po",
        "/remediate/revert_pricing",
    }
    payloads = [json.loads(request.content) for request in requests]
    assert {payload["incident_id"] for payload in payloads} == {"INC-9942"}
    assert {payload["action"] for payload in payloads} == {"REVERT_STATE"}
