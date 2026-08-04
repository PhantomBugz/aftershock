"""Fast unit contracts for the opt-in live DataHub indexing poller."""

from __future__ import annotations

import asyncio
from collections.abc import Sequence
from typing import Any

import pytest

import test_live_datahub_mcp as live_contract
from remediation_models import ActionableTarget


class FakeClock:
    def __init__(self) -> None:
        self.now = 0.0
        self.sleeps: list[float] = []

    def monotonic(self) -> float:
        return self.now

    async def sleep(self, delay: float) -> None:
        self.sleeps.append(delay)
        self.now += delay


class SequencedMapper:
    def __init__(
        self, responses: Sequence[Sequence[ActionableTarget]]
    ) -> None:
        self.responses = [list(response) for response in responses]
        self.calls = 0

    async def get_targets(self, dataset_urn: str) -> list[ActionableTarget]:
        assert dataset_urn == live_contract.DATASET_URN
        index = min(self.calls, len(self.responses) - 1)
        self.calls += 1
        return self.responses[index]


def _job(
    *, action: str | None = "ISSUE_PO", webhook: str | None = None
) -> ActionableTarget:
    return ActionableTarget(
        urn=live_contract.JOB_URN,
        entity_type="DATA_JOB",
        business_action=action,
        remediation_webhook=webhook,
    )


def test_live_index_poller_waits_until_job_and_both_properties_are_visible() -> None:
    clock = FakeClock()
    complete = _job(
        webhook="http://127.0.0.1:8765/remediate/cancel_po"
    )
    mapper = SequencedMapper(
        [
            [],
            [_job(webhook=None)],
            [complete],
        ]
    )

    result = asyncio.run(
        live_contract.wait_for_seeded_playbook(
            context=object(),
            timeout_seconds=2.5,
            poll_interval_seconds=1.0,
            mapper_factory=lambda context: mapper,
            monotonic=clock.monotonic,
            sleep=clock.sleep,
        )
    )

    assert result == complete
    assert mapper.calls == 3
    assert clock.sleeps == [1.0, 1.0]


def test_live_index_poller_fails_at_deadline_without_unbounded_retry() -> None:
    clock = FakeClock()
    mapper = SequencedMapper([[_job(webhook=None)]])

    with pytest.raises(
        AssertionError,
        match="timed out after 2.5s waiting for seeded DataJob and both",
    ):
        asyncio.run(
            live_contract.wait_for_seeded_playbook(
                context=object(),
                timeout_seconds=2.5,
                poll_interval_seconds=1.0,
                mapper_factory=lambda context: mapper,
                monotonic=clock.monotonic,
                sleep=clock.sleep,
            )
        )

    assert mapper.calls == 4
    assert clock.sleeps == [1.0, 1.0, 0.5]
