# Aftershock

Aftershock is a DataHub-backed incident-response agent for the **Agents That Do
Real Work** challenge. It turns downstream lineage and governed metadata into
an operational workflow: discover exposed systems, invoke their configured
compensating controls, capture structured receipts, and write an incident
summary back to DataHub for the next person or agent.

We use **Action Debt** as project terminology for operational follow-up created
when an automated system acts on data that is later found to be unreliable.

## What it does

For a critical, normalized incident envelope, Aftershock:

1. calls the official `mcp-server-datahub==0.6.0` through FastMCP;
2. discovers downstream assets with `get_lineage(upstream=false)`;
3. fetches entity metadata with `get_entities`;
4. reads the exact structured-property qualified names
   `aftershock.businessAction` and `aftershock.remediationWebhook`;
5. concurrently POSTs the configured compensating controls and records a
   success, failure, or skipped receipt for each target; and
6. calls `save_document` through MCP to persist the incident-specific receipt
   summary and related asset URNs in DataHub.

Each control request includes a deterministic `Idempotency-Key`. That gives a
downstream service a stable retry key; it is not, by itself, proof of
exactly-once execution.

## Architecture

```text
DataHub incidentInfo MetadataChangeLogEvent
                    |
        custom DataHub Action adapter       (production integration; not included)
                    |
        authenticated normalized envelope
                    |
             FastAPI listener
                    |
     official DataHub MCP server over FastMCP
        |                  |                 |
 get_lineage         get_entities       save_document
        |                  |                 |
        +---- actionable targets ----+       |
                                     |       |
                         compensating controls
                                     |
                         structured receipts + DataHub record
```

The current API accepts an **Aftershock-normalized incident envelope**. A
production event integration would translate DataHub's `incidentInfo`
MetadataChangeLogEvent with a custom DataHub Action, then send an authenticated
HTTP request to Aftershock. That Action adapter is not part of this repository.
See DataHub's [MetadataChangeLogEvent reference][mcl] and
[custom Action guide][actions].

## Run the deterministic demonstration

Requirements: Python 3.12 and PowerShell.

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
python -m pip install -r requirements.txt

python -m pytest -q
python src\demo_dashboard.py
```

The dashboard is unmistakably labeled **OFFLINE FIXTURE MODE**. It uses local,
deterministic lineage, `httpx.MockTransport` control responses, and an in-memory
write-back recorder. It does not represent a live DataHub run.

The protocol tests use an in-process FastMCP server and genuine MCP/JSON-RPC
exchanges. They verify tool arguments, parsing, failure behavior, and write-back
contracts, but they are not evidence of persistence to a live DataHub instance.

## Run the listener

Set an explicit context mode and a secret used to authenticate critical
requests. This fixture command exercises the HTTP contract, but its configured
example remediation hosts are intentionally non-production and will normally
produce failed control receipts. Use the dashboard above for a deterministic
all-success presentation.

Fixture metadata is explicitly synthetic and uses `PROD` URNs. The separate
live bootstrap uses collision-checked, uniquely namespaced `DEV` assets such as
`aftershock_demo.inventory_pricing`; see the [live setup guide][live-setup] for
the mandatory target confirmation and exact commands.

```powershell
$env:AFTERSHOCK_DATAHUB_MODE = "fixture"
$env:AFTERSHOCK_WEBHOOK_TOKEN = "replace-with-a-long-random-secret"
python -m uvicorn lineage_listener:app --app-dir src --host 127.0.0.1 --port 8000
```

From a second PowerShell window:

```powershell
$headers = @{ Authorization = "Bearer replace-with-a-long-random-secret" }
$body = @{
  incident_id = "INC-DEMO-001"
  dataset_urn = "urn:li:dataset:(urn:li:dataPlatform:postgres,inventory_pricing,PROD)"
  severity = "CRITICAL"
} | ConvertTo-Json

Invoke-RestMethod `
  -Method Post `
  -Uri "http://127.0.0.1:8000/webhook/datahub" `
  -Headers $headers `
  -ContentType "application/json" `
  -Body $body
```

Critical envelopes require `Authorization: Bearer <AFTERSHOCK_WEBHOOK_TOKEN>`.
The endpoint returns HTTP 200 with `completed` only when every discovered
control and the DataHub write-back succeed. Otherwise it returns
`completed_with_issues` and the factual receipts. Invalid input returns 422;
missing or invalid authorization returns 401; missing authentication
configuration or unavailable DataHub processing returns a fixed, secret-safe
503. Noncritical envelopes return `ignored` before opening a DataHub context.

For a real instance, follow [Live DataHub setup](docs/live-datahub-setup.md).

## Repository map

- `src/datahub_context.py` — explicit fixture and MCP context adapters
- `src/blast_radius_mapper.py` — lineage-to-structured-playbook mapping
- `src/compensating_action_engine.py` — concurrent controls and receipts
- `src/incident_processor.py` — read, act, and `save_document` orchestration
- `src/lineage_listener.py` — authenticated normalized-envelope API
- `src/demo_dashboard.py` — receipt-driven offline presentation
- `config/` and `scripts/` — live structured-property and seed bootstrap
- `tests/` — unit, protocol, API, dashboard, and opt-in live tests
- `examples/` — captured sample output, labeled by execution mode

## Current boundary

The MVP operates at the **system-exposure and compensating-control level**. A
lineage edge shows that a downstream asset is exposed; it does not prove which
individual records that system read or which external actions resulted. The
MVP therefore does not claim row-level causality, individual-action recovery,
or exactly-once control execution.

The proposed [Action-Provenance Ledger RFC](docs/RFC-Action-Provenance-Ledger.md)
describes a future path to action IDs, incident windows, and more selective
controls. It is a project proposal, not an upstream contribution.

## Reproducibility and live-proof status

The MCP server and FastMCP protocol dependencies are exact-pinned. Offline
tests and demo inputs are deterministic. Live persistence was not executed in
the development environment because no running DataHub deployment or live
credentials were available. The opt-in live test in the setup guide is the
proof gate; a skipped live test is not a successful live result.

Useful DataHub references:

- [DataHub MCP server guide][mcp]
- [Structured properties tutorial][properties]
- [DataHub Quickstart][quickstart]

## AI-assistance disclosure

Standard AI coding assistants were used during development to help scaffold,
test, review, and document the project. The project concept, acceptance
criteria, architecture decisions, and submission decisions remain under human
direction and review.

## License

Licensed under the [Apache License 2.0](LICENSE).

[mcp]: https://docs.datahub.com/docs/features/feature-guides/mcp
[properties]: https://docs.datahub.com/docs/api/tutorials/structured-properties
[mcl]: https://docs.datahub.com/docs/actions/events/metadata-change-log-event
[actions]: https://docs.datahub.com/docs/actions/guides/developing-an-action
[quickstart]: https://docs.datahub.com/docs/quickstart
[live-setup]: docs/live-datahub-setup.md
