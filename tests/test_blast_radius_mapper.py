import asyncio
import json
from pathlib import Path

import httpx

from blast_radius_mapper import BlastRadiusMapper


FIXTURE_PATH = (
    Path(__file__).resolve().parents[1] / "mock-data" / "datahub_lineage.json"
)
DATASET_URN = "urn:li:dataset:(urn:li:dataPlatform:postgres,inventory_pricing,PROD)"


def test_returns_actionable_entities_for_root_dataset() -> None:
    mapper = BlastRadiusMapper(FIXTURE_PATH)

    targets = asyncio.run(mapper.get_actionable_targets(DATASET_URN))

    assert len(targets) == 2
    assert {target["entity"]["type"] for target in targets} == {
        "DATA_JOB",
        "ML_MODEL",
    }


def test_returns_empty_for_unknown_dataset() -> None:
    mapper = BlastRadiusMapper(FIXTURE_PATH)

    targets = asyncio.run(mapper.get_actionable_targets("urn:li:dataset:unknown"))

    assert targets == []


def test_live_datahub_query_is_constructed_and_normalized(monkeypatch) -> None:
    requests: list[dict] = []

    async def datahub_gms(request: httpx.Request) -> httpx.Response:
        requests.append(
            {
                "method": request.method,
                "url": str(request.url),
                "payload": json.loads(request.content),
            }
        )
        return httpx.Response(
            200,
            json={
                "data": {
                    "dataset": {
                        "urn": DATASET_URN,
                        "downstreamLineage": {
                            "entities": [
                                {
                                    "urn": "urn:li:dataJob:(urn:li:dataFlow:airflow,purchase_order_generator,PROD)",
                                    "type": "DATA_JOB",
                                    "properties": {
                                        "customProperties": [
                                            {
                                                "key": "business_action",
                                                "value": "ISSUE_PO",
                                            },
                                            {
                                                "key": "remediation_webhook",
                                                "value": "https://api.internal.corp/remediate/cancel_po",
                                            },
                                        ]
                                    },
                                },
                                {
                                    "urn": "urn:li:mlModel:(urn:li:dataPlatform:sagemaker,dynamic_pricing_model,PROD)",
                                    "type": "ML_MODEL",
                                    "properties": {
                                        "customProperties": [
                                            {
                                                "key": "business_action",
                                                "value": "ADJUST_PRICE",
                                            },
                                            {
                                                "key": "remediation_webhook",
                                                "value": "https://api.internal.corp/remediate/revert_pricing",
                                            },
                                        ]
                                    },
                                },
                            ]
                        },
                    }
                }
            },
        )

    async def scenario() -> list[dict]:
        transport = httpx.MockTransport(datahub_gms)
        async with httpx.AsyncClient(transport=transport) as client:
            mapper = BlastRadiusMapper(FIXTURE_PATH, http_client=client)
            return await mapper.get_actionable_targets(DATASET_URN)

    monkeypatch.setenv("DATAHUB_GMS_URL", "https://datahub.example/gms/")
    targets = asyncio.run(scenario())

    assert requests == [
        {
            "method": "POST",
            "url": "https://datahub.example/gms/api/graphql",
            "payload": requests[0]["payload"],
        }
    ]
    graphql_payload = requests[0]["payload"]
    assert graphql_payload["variables"] == {"urn": DATASET_URN}
    assert "downstreamLineage: lineage" in graphql_payload["query"]
    assert targets == [
        {
            "entity": {
                "urn": "urn:li:dataJob:(urn:li:dataFlow:airflow,purchase_order_generator,PROD)",
                "type": "DATA_JOB",
                "customProperties": {
                    "business_action": "ISSUE_PO",
                    "remediation_webhook": "https://api.internal.corp/remediate/cancel_po",
                },
            }
        },
        {
            "entity": {
                "urn": "urn:li:mlModel:(urn:li:dataPlatform:sagemaker,dynamic_pricing_model,PROD)",
                "type": "ML_MODEL",
                "customProperties": {
                    "business_action": "ADJUST_PRICE",
                    "remediation_webhook": "https://api.internal.corp/remediate/revert_pricing",
                },
            }
        },
    ]
