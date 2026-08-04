import asyncio
import json
from io import StringIO

import httpx
from rich.console import Console

from demo_dashboard import run_demo
from remediation_models import RemediationReceipt


def test_dashboard_renders_lifecycle_and_executes_engine() -> None:
    output = StringIO()
    requests: list[tuple[str, dict]] = []

    async def remediation_api(request: httpx.Request) -> httpx.Response:
        requests.append((request.url.path, json.loads(request.content)))
        return httpx.Response(200, json={"status": "accepted"})

    async def scenario() -> list[RemediationReceipt]:
        transport = httpx.MockTransport(remediation_api)
        async with httpx.AsyncClient(transport=transport) as client:
            console = Console(
                file=output,
                force_terminal=False,
                color_system=None,
                width=120,
            )
            return await run_demo(console=console, http_client=client, delay=0)

    results = asyncio.run(scenario())
    rendered = output.getvalue()

    assert [receipt.status for receipt in results] == ["succeeded", "succeeded"]
    assert "CRITICAL INCIDENT DETECTED" in rendered
    assert "Pricing Decimal Shift" in rendered
    assert "inventory_pricing" in rendered
    assert "Airflow Job" in rendered
    assert "ISSUE_PO" in rendered
    assert "SageMaker Model" in rendered
    assert "ADJUST_PRICE" in rendered
    assert "Executing Compensating Controls..." in rendered
    assert "SUCCESS: Action Debt Neutralized" in rendered
    assert "$120,000 in erroneous orders reversed" in rendered
    assert "Enterprise State Restored" in rendered
    assert {path for path, _ in requests} == {
        "/remediate/cancel_po",
        "/remediate/revert_pricing",
    }
    assert {payload["incident_id"] for _, payload in requests} == {"INC-9942"}
