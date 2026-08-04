"""FastAPI entrypoint for Aftershock-normalized incident envelopes."""

from __future__ import annotations

import logging
from collections.abc import AsyncIterator, Callable
from contextlib import AbstractAsyncContextManager, asynccontextmanager
from typing import Annotated

from fastapi import Depends, FastAPI, HTTPException, status
from pydantic import BaseModel, ConfigDict, Field, field_validator

from blast_radius_mapper import BlastRadiusMappingError
from compensating_action_engine import CompensatingActionEngine
from datahub_context import (
    DataHubConfigurationError,
    DataHubMCPError,
    build_datahub_context_from_env,
)
from incident_processor import AftershockIncidentProcessor
from remediation_models import IncidentReport


logger = logging.getLogger("Aftershock-Listener")
app = FastAPI(title="Aftershock Incident Listener", version="0.2.0")
_DATASET_URN_PREFIX = "urn:li:dataset:"
_PROCESSING_UNAVAILABLE = "Aftershock incident processing unavailable"

ProcessorSession = AbstractAsyncContextManager[AftershockIncidentProcessor]
ProcessorSessionFactory = Callable[[], ProcessorSession]


class AftershockIncidentEnvelope(BaseModel):
    """Normalized Aftershock input; it is not a native DataHub event schema."""

    model_config = ConfigDict(str_strip_whitespace=True, extra="forbid")

    incident_id: str = Field(min_length=1, max_length=128)
    dataset_urn: str = Field(min_length=1, max_length=2048)
    severity: str = Field(min_length=1, max_length=32)

    @field_validator("dataset_urn")
    @classmethod
    def require_dataset_urn(cls, value: str) -> str:
        suffix = value[len(_DATASET_URN_PREFIX) :]
        if not value.startswith(_DATASET_URN_PREFIX) or not suffix.strip():
            raise ValueError("dataset_urn must be a DataHub dataset URN")
        return value


@asynccontextmanager
async def _default_processor_session() -> AsyncIterator[AftershockIncidentProcessor]:
    """Build and close one live or explicitly configured fixture session."""

    context = build_datahub_context_from_env()
    engine = CompensatingActionEngine()
    try:
        yield AftershockIncidentProcessor(context, engine)
    finally:
        await engine.aclose()


def get_processor_session_factory() -> ProcessorSessionFactory:
    """Provide a lazy session factory so ignored envelopes touch no context."""

    return _default_processor_session


def _workflow_completed(processor_report: IncidentReport) -> bool:
    return (
        all(receipt.status == "succeeded" for receipt in processor_report.receipts)
        and processor_report.writeback.status == "succeeded"
    )


@app.post("/webhook/datahub", status_code=status.HTTP_200_OK)
async def receive_aftershock_incident(
    incident: AftershockIncidentEnvelope,
    session_factory: Annotated[
        ProcessorSessionFactory, Depends(get_processor_session_factory)
    ],
) -> dict[str, object]:
    """Process one normalized incident fully, including DataHub write-back."""

    if incident.severity.upper() != "CRITICAL":
        return {
            "status": "ignored",
            "incident_id": incident.incident_id,
            "message": "no action required",
        }

    try:
        async with session_factory() as processor:
            report = await processor.process(
                incident.incident_id, incident.dataset_urn
            )
    except (
        DataHubConfigurationError,
        DataHubMCPError,
        BlastRadiusMappingError,
    ):
        # Tool details can contain server responses or secrets. Log and return
        # only a controlled message, and never switch to fixture data.
        logger.error("Aftershock incident processing failed before completion")
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=_PROCESSING_UNAVAILABLE,
        ) from None

    response = report.to_dict()
    return {
        "status": (
            "completed" if _workflow_completed(report) else "completed_with_issues"
        ),
        **response,
    }
