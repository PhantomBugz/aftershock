"""Opt-in live contract against a seeded DataHub instance through real MCP."""

from __future__ import annotations

import asyncio
import os
import time
from collections.abc import Awaitable, Callable
from typing import Any

import pytest

from blast_radius_mapper import BlastRadiusMapper, BlastRadiusMappingError
from datahub_context import (
    DataHubMCPError,
    MCPDataHubContext,
    build_datahub_context_from_env,
)
from remediation_models import ActionableTarget


pytestmark = [
    pytest.mark.live_datahub,
    pytest.mark.skipif(
        os.environ.get("RUN_LIVE_DATAHUB_TESTS") != "1",
        reason="set RUN_LIVE_DATAHUB_TESTS=1 to run against a real DataHub MCP server",
    ),
]

DATASET_URN = (
    "urn:li:dataset:(urn:li:dataPlatform:postgres,"
    "aftershock_demo.inventory_pricing,DEV)"
)
JOB_URN = (
    "urn:li:dataJob:(urn:li:dataFlow:(airflow,aftershock_demo,DEV),"
    "purchase_order_generator)"
)
DOCUMENT_URN = "urn:li:document:aftershock-live-contract-test"
LIVE_INDEX_TIMEOUT_SECONDS = 30.0
LIVE_INDEX_POLL_INTERVAL_SECONDS = 1.0
EXPECTED_BUSINESS_ACTION = "ISSUE_PO"
EXPECTED_REMEDIATION_WEBHOOK = (
    "http://127.0.0.1:8765/remediate/cancel_po"
)


def _index_deadline_error(timeout_seconds: float) -> AssertionError:
    return AssertionError(
        f"timed out after {timeout_seconds:g}s waiting for seeded "
        "DataJob and both Aftershock structured properties through MCP"
    )


async def wait_for_seeded_playbook(
    context: Any,
    *,
    timeout_seconds: float = LIVE_INDEX_TIMEOUT_SECONDS,
    poll_interval_seconds: float = LIVE_INDEX_POLL_INTERVAL_SECONDS,
    mapper_factory: Callable[[Any], Any] = BlastRadiusMapper,
    monotonic: Callable[[], float] = time.monotonic,
    sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
) -> ActionableTarget:
    """Poll MCP until the seeded DataJob and both properties are indexed."""

    if timeout_seconds <= 0:
        raise ValueError("timeout_seconds must be positive")
    if poll_interval_seconds <= 0:
        raise ValueError("poll_interval_seconds must be positive")

    mapper = mapper_factory(context)
    deadline = monotonic() + timeout_seconds
    while True:
        remaining = deadline - monotonic()
        if remaining <= 0:
            raise _index_deadline_error(timeout_seconds)

        try:
            targets = await asyncio.wait_for(
                mapper.get_targets(DATASET_URN),
                timeout=remaining,
            )
        except TimeoutError:
            raise _index_deadline_error(timeout_seconds) from None
        except (DataHubMCPError, BlastRadiusMappingError):
            targets = None

        if targets is not None:
            job = next(
                (
                    target
                    for target in targets
                    if target.urn == JOB_URN
                    and target.business_action == EXPECTED_BUSINESS_ACTION
                    and target.remediation_webhook
                    == EXPECTED_REMEDIATION_WEBHOOK
                ),
                None,
            )
            if job is not None:
                return job

        remaining = deadline - monotonic()
        if remaining <= 0:
            raise _index_deadline_error(timeout_seconds)
        await sleep(min(poll_interval_seconds, remaining))


def test_live_mcp_discovers_seeded_playbook_and_persists_document() -> None:
    context = build_datahub_context_from_env()
    assert isinstance(context, MCPDataHubContext), (
        "RUN_LIVE_DATAHUB_TESTS=1 requires AFTERSHOCK_DATAHUB_MODE=mcp; "
        "fixture mode is not a live test"
    )

    job = asyncio.run(wait_for_seeded_playbook(context))

    assert job.business_action == EXPECTED_BUSINESS_ACTION
    assert job.remediation_webhook == EXPECTED_REMEDIATION_WEBHOOK

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
