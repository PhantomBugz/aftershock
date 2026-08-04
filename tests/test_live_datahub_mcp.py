"""Opt-in live contract against a seeded DataHub instance through real MCP."""

from __future__ import annotations

import asyncio
import os

import pytest

from blast_radius_mapper import BlastRadiusMapper
from datahub_context import MCPDataHubContext, build_datahub_context_from_env


pytestmark = [
    pytest.mark.live_datahub,
    pytest.mark.skipif(
        os.environ.get("RUN_LIVE_DATAHUB_TESTS") != "1",
        reason="set RUN_LIVE_DATAHUB_TESTS=1 to run against a real DataHub MCP server",
    ),
]

DATASET_URN = "urn:li:dataset:(urn:li:dataPlatform:postgres,inventory_pricing,PROD)"
JOB_URN = (
    "urn:li:dataJob:(urn:li:dataFlow:(airflow,aftershock_demo,PROD),"
    "purchase_order_generator)"
)
DOCUMENT_URN = "urn:li:document:aftershock-live-contract-test"


def test_live_mcp_discovers_seeded_playbook_and_persists_document() -> None:
    context = build_datahub_context_from_env()
    assert isinstance(context, MCPDataHubContext), (
        "RUN_LIVE_DATAHUB_TESTS=1 requires AFTERSHOCK_DATAHUB_MODE=mcp; "
        "fixture mode is not a live test"
    )

    targets = asyncio.run(BlastRadiusMapper(context).get_targets(DATASET_URN))
    job = next((target for target in targets if target.urn == JOB_URN), None)

    assert job is not None, "seeded DataJob was not discovered through MCP lineage"
    assert job.business_action == "ISSUE_PO"
    assert (
        job.remediation_webhook
        == "http://127.0.0.1:8765/remediate/cancel_po"
    )

    result = asyncio.run(
        context.save_document(
            document_type="Summary",
            title="Aftershock live MCP contract test",
            content=(
                "This deterministic document verifies real MCP save_document "
                "write-back. It does not execute remediation."
            ),
            urn=DOCUMENT_URN,
            related_assets=[DATASET_URN, JOB_URN],
        )
    )

    assert result.get("urn") == DOCUMENT_URN
