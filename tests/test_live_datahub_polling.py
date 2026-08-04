"""Fast unit contracts for the opt-in live DataHub indexing poller."""

from __future__ import annotations

import asyncio
from collections.abc import Sequence
from typing import Any

import pytest

import test_live_datahub_mcp as live_contract
from blast_radius_mapper import BlastRadiusMappingError
from datahub_context import DataHubMCPError
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
        self,
        responses: Sequence[
            Sequence[ActionableTarget] | BaseException
        ],
    ) -> None:
        self.responses = [
            list(response) if not isinstance(response, BaseException) else response
            for response in responses
        ]
        self.calls = 0

    async def get_targets(self, dataset_urn: str) -> list[ActionableTarget]:
        assert dataset_urn == live_contract.DATASET_URN
        index = min(self.calls, len(self.responses) - 1)
        self.calls += 1
        response = self.responses[index]
        if isinstance(response, BaseException):
            raise response
        return response


def _job(
    *, action: str | None = "ISSUE_PO", webhook: str | None = None
) -> ActionableTarget:
    return ActionableTarget(
        urn=live_contract.JOB_URN,
        entity_type="DATA_JOB",
        business_action=action,
        remediation_webhook=webhook,
    )


def _exact_job() -> ActionableTarget:
    return _job(
        action="ISSUE_PO",
        webhook="http://127.0.0.1:8765/remediate/cancel_po",
    )


def test_live_index_poller_waits_for_exact_seeded_property_values() -> None:
    clock = FakeClock()
    complete = _exact_job()
    mapper = SequencedMapper(
        [
            [
                _job(
                    action="STALE_ACTION",
                    webhook="http://127.0.0.1:8765/remediate/cancel_po",
                )
            ],
            [
                _job(
                    action="ISSUE_PO",
                    webhook="http://127.0.0.1:9999/stale",
                )
            ],
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


@pytest.mark.parametrize(
    "transient_error",
    [
        DataHubMCPError("controlled transient MCP failure"),
        BlastRadiusMappingError("controlled transient mapping failure"),
    ],
    ids=["mcp", "mapping"],
)
def test_live_index_poller_retries_only_controlled_transient_errors(
    transient_error: Exception,
) -> None:
    clock = FakeClock()
    mapper = SequencedMapper([transient_error, [_exact_job()]])

    result = asyncio.run(
        live_contract.wait_for_seeded_playbook(
            context=object(),
            timeout_seconds=2.0,
            poll_interval_seconds=0.5,
            mapper_factory=lambda context: mapper,
            monotonic=clock.monotonic,
            sleep=clock.sleep,
        )
    )

    assert result == _exact_job()
    assert mapper.calls == 2
    assert clock.sleeps == [0.5]


def test_live_index_poller_propagates_arbitrary_mapper_exceptions() -> None:
    clock = FakeClock()
    error = RuntimeError("programmer or authentication error")
    mapper = SequencedMapper([error])

    with pytest.raises(RuntimeError) as exc_info:
        asyncio.run(
            live_contract.wait_for_seeded_playbook(
                context=object(),
                timeout_seconds=2.0,
                poll_interval_seconds=0.5,
                mapper_factory=lambda context: mapper,
                monotonic=clock.monotonic,
                sleep=clock.sleep,
            )
        )

    assert exc_info.value is error
    assert mapper.calls == 1
    assert clock.sleeps == []


def test_live_index_poller_checks_deadline_before_starting_mapper_call() -> None:
    readings = iter([0.0, 2.5])
    mapper = SequencedMapper([[_exact_job()]])

    async def forbidden_sleep(delay: float) -> None:
        pytest.fail(f"must not sleep for {delay}")

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
                monotonic=lambda: next(readings),
                sleep=forbidden_sleep,
            )
        )

    assert mapper.calls == 0


def test_live_index_poller_bounds_and_cancels_a_hanging_mapper_call() -> None:
    class HangingMapper:
        def __init__(self) -> None:
            self.calls = 0
            self.cancelled = False

        async def get_targets(
            self, dataset_urn: str
        ) -> list[ActionableTarget]:
            self.calls += 1
            try:
                await asyncio.Event().wait()
            except asyncio.CancelledError:
                self.cancelled = True
                raise

    mapper = HangingMapper()

    with pytest.raises(
        AssertionError,
        match="timed out after 0.05s waiting for seeded DataJob and both",
    ):
        asyncio.run(
            live_contract.wait_for_seeded_playbook(
                context=object(),
                timeout_seconds=0.05,
                poll_interval_seconds=0.01,
                mapper_factory=lambda context: mapper,
            )
        )

    assert mapper.calls == 1
    assert mapper.cancelled is True


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

    assert mapper.calls == 3
    assert clock.sleeps == [1.0, 1.0, 0.5]
