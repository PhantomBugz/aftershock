"""FastAPI receiver for DataHub incident webhook events."""

from __future__ import annotations

from collections.abc import AsyncIterator
from typing import Annotated

from fastapi import Depends, FastAPI, Response, status
from pydantic import BaseModel, Field

from blast_radius_mapper import BlastRadiusMapper
from compensating_action_engine import CompensatingActionEngine


app = FastAPI(title="Aftershock Data Incident Listener", version="0.1.0")


class DataHubIncident(BaseModel):
    """Minimal incident envelope emitted by the DataHub Action webhook."""

    incident_id: str = Field(min_length=1)
    dataset_urn: str = Field(min_length=1)
    severity: str = Field(min_length=1)


def get_blast_radius_mapper() -> BlastRadiusMapper:
    """Provide the current DataHub graph adapter."""

    return BlastRadiusMapper()


async def get_compensating_action_engine(
) -> AsyncIterator[CompensatingActionEngine]:
    """Provide and close a request-scoped remediation engine."""

    engine = CompensatingActionEngine()
    try:
        yield engine
    finally:
        await engine.aclose()


@app.post("/webhook/datahub", status_code=status.HTTP_202_ACCEPTED)
async def receive_datahub_incident(
    incident: DataHubIncident,
    response: Response,
    mapper: Annotated[BlastRadiusMapper, Depends(get_blast_radius_mapper)],
    engine: Annotated[
        CompensatingActionEngine, Depends(get_compensating_action_engine)
    ],
) -> dict[str, object]:
    """Map a DataHub incident to downstream compensation playbooks."""

    if incident.severity.upper() != "CRITICAL":
        response.status_code = status.HTTP_200_OK
        return {
            "status": "ignored",
            "incident_id": incident.incident_id,
            "message": "no action required",
        }

    targets = await mapper.get_targets(incident.dataset_urn)
    receipts = await engine.process_blast_radius(targets, incident.incident_id)
    return {
        "status": "accepted",
        "incident_id": incident.incident_id,
        "targets_found": len(targets),
        "remediations_triggered": sum(
            receipt.status == "succeeded" for receipt in receipts
        ),
        "results": [receipt.to_dict() for receipt in receipts],
    }
