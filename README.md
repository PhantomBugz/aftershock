# Aftershock

Aftershock is a DataHub-backed incident-response agent for the **Agents That Do
Real Work** challenge. It is a deterministic, auditable, policy-driven agent
loop:

```text
observe DataHub context -> decide from governed metadata and policy
                        -> act through an exact remediation grant
                        -> persist receipts back to DataHub
```

Its agency comes from closing this governed observe/decide/act/persist loop.
The listener/API path begins after an authenticated incident trigger; the live
judge command invokes the same processor directly for the fixed seeded
scenario. Every demonstrated decision is reproducible from DataHub metadata
and operator policy.

We use **Action Debt** as project terminology for operational follow-up created
when an automated system acts on data that is later found to be unreliable.
In the live proof, bad pricing has already issued `PO-AFTERSHOCK-001`;
Aftershock finds the exposed job in DataHub, cancels that order, prevents the
next issuance, and writes the PO-bound receipt back.

## What it does

For a critical Aftershock-normalized incident envelope, the workflow:

1. calls the official `mcp-server-datahub==0.6.0` through FastMCP;
2. discovers downstream assets with `get_lineage(upstream=false)`;
3. fetches entity metadata with `get_entities`;
4. reads the exact structured-property qualified names
   `aftershock.businessAction` and `aftershock.remediationWebhook`;
5. checks the target URN, entity type, business action, and endpoint against an
   operator-controlled exact remediation grant;
6. invokes bounded, asynchronous compensating controls and records factual
   receipts; and
7. calls `save_document` through MCP to persist the incident summary and
   related asset URNs for the next operator or agent.

## DataHub is the foundation

Aftershock does not rebuild catalog or lineage capabilities. DataHub supplies
the downstream context, governed playbook metadata, and durable organizational
memory. Live mode never changes to fixture data after a failed MCP call.

For `mcp-server-datahub==0.6.0`, the adapter passes `max_hops=3`; in this pinned
server contract that value means unlimited lineage traversal, not a finite hop
limit. Results are paginated and validated fail-closed before controls run.

```text
DataHub incidentInfo MetadataChangeLogEvent
                    |
        custom DataHub Action adapter       (not implemented)
                    |
        authenticated normalized envelope
                    |
             FastAPI listener
                    |
     official DataHub MCP server over FastMCP
        |                  |                 |
 get_lineage         get_entities       save_document
        |                  |                 |
        +---- governed target policy ----+  |
                                        |  |
                           exact remediation grants
                                        |
                         receipts + DataHub incident record
```

The current API begins after the authenticated normalized trigger. A production
event integration would translate DataHub's `incidentInfo`
MetadataChangeLogEvent with a custom DataHub Action, then POST the normalized
contract to Aftershock. That adapter remains future integration work. See the
[MetadataChangeLogEvent reference][mcl] and [custom Action guide][actions].

## Receipt contract: HTTP is not the outcome

Every target settles into one of five states:

| Status | Meaning |
| --- | --- |
| `succeeded` | An eligible non-202 response contained the required v1 terminal `succeeded` contract and a valid external receipt ID. A 408/5xx is honored only when it carries that terminal contract. |
| `accepted` | The receiver acknowledged work, but no terminal outcome is known. HTTP 202 and accepted/pending contracts on ordinary success-class responses are nonterminal. |
| `failed` | The request failed before dispatch, was rejected with a 4xx other than 408, encountered a disabled redirect, or returned a valid v1 terminal failure receipt. |
| `skipped` | The playbook was incomplete, its exact endpoint was denied, or the workflow deadline expired before dispatch. |
| `outcome_unknown` | Dispatch may have occurred, but a 408/5xx without a valid terminal receipt, transport failure, deadline expiry, or another invalid/missing terminal response left the outcome unproven. |

The success response contract is:

```json
{
  "receipt_version": 1,
  "status": "succeeded",
  "receipt_id": "receiver-generated-stable-id"
}
```

The external receipt ID is shown in the API, dashboard, and DataHub incident
document. Extra response fields are ignored and never establish success. A
plain HTTP 200 is not business-success evidence. An explicit v1 terminal
success/failure receipt is honored on HTTP 408/5xx; without one, those
ambiguous responses remain `outcome_unknown`.

The API returns `completed` only when every discovered control is terminal
`succeeded` **and** the DataHub write-back succeeds. `accepted` and
`outcome_unknown` therefore produce `completed_with_issues`.

Each dispatched request carries a deterministic `Idempotency-Key` derived from
the incident, target, and business action. It is a stable retry key, not proof
of exactly-once execution. The engine uses at most eight workers and a
30-second workflow deadline by default. At the deadline, dispatched work is
conservatively `outcome_unknown`; work not yet dispatched is `skipped`.

## Governed outbound controls

Critical listener sessions require `AFTERSHOCK_REMEDIATION_ALLOWLIST_JSON`, a
nonempty JSON array of exact, four-field grants. An endpoint alone cannot
authorize a different asset or business action:

```powershell
$env:AFTERSHOCK_REMEDIATION_ALLOWLIST_JSON = '[{"target_urn":"urn:li:dataJob:(urn:li:dataFlow:(airflow,aftershock_demo,DEV),purchase_order_generator)","entity_type":"DATA_JOB","business_action":"ISSUE_PO","endpoint":"http://127.0.0.1:8765/remediate/cancel_po"}]'
```

Authorization is exact across `target_urn`, `entity_type`, `business_action`,
and `endpoint`. The endpoint's scheme, host, port, path, and query must also
match. User information and fragments are rejected. Plain HTTP is allowed only
for loopback hosts; nonloopback endpoints require HTTPS. Redirects are
disabled. Query values may participate in authorization but are removed from
logs and persisted receipt endpoints.

Never put credentials in a structured property or URL. DataHub property-write
RBAC, document-mutation permissions, receiver authentication, and network
egress policy remain operator responsibilities.

## Run the live judge demonstration

The primary submission path uses a real local DataHub OSS deployment, the
official DataHub MCP server, a loopback-only stateful receiver, and independent
MCP read-back. First follow the [live setup guide][live-setup] to start and seed
DataHub. Then start the built-in receiver in one PowerShell window:

```powershell
python src\demo_remediation_receiver.py
```

In a second window, select the live MCP context and run the proof gate:

```powershell
$env:AFTERSHOCK_DATAHUB_MODE = "mcp"
$env:DATAHUB_GMS_URL = "http://127.0.0.1:8080"
Remove-Item Env:DATAHUB_GMS_TOKEN -ErrorAction SilentlyContinue
Remove-Item Env:DATAHUB_MCP_URL -ErrorAction SilentlyContinue
Remove-Item Env:DATAHUB_MCP_TOKEN -ErrorAction SilentlyContinue
python src\live_demo.py
```

`live_demo.py` refuses fixture mode. It resets the receiver and displays the
seeded order `PO-AFTERSHOCK-001` as `issued`. It then executes the
OBSERVE/DECIDE/ACT/PERSIST loop, requires a PO-bound terminal v1 success
receipt, confirms the order is `canceled` and further issuance is disabled,
and requires the DataHub write-back to succeed. It then proves persistence
through three independent MCP reads:

1. `search_documents` finds the exact generated document URN and title;
2. `grep_documents` finds the incident ID and external receipt ID in content;
3. `get_entities` finds the saved document in both the dataset and DataJob
   `relatedDocuments` backlinks.

On August 4, 2026, this path passed against local DataHub OSS Quickstart v1.6.0:
the seeded order changed from `issued` to `canceled` exactly once, the same
PO-bound receipt was persisted to DataHub, and all read-back gates passed. The
credential-free evidence is captured in
[`examples/live_demo_proof.txt`](examples/live_demo_proof.txt).

![Local DataHub OSS UI showing the saved Aftershock incident document in MCP context with its PO-bound terminal control receipt](docs/assets/datahub-live-incident.png)

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
deterministic lineage, exact in-memory remediation grants, `httpx.MockTransport`
responses containing valid v1 terminal receipts, and an in-memory write-back
recorder. It does not represent a live DataHub run.

The protocol tests use an in-process FastMCP server and genuine MCP/JSON-RPC
exchanges. They verify tool arguments, parsing, failure behavior, and write-back
contracts, but they are not evidence of persistence to a live DataHub instance.

## Run the listener in fixture context

This command exercises the real API and policy configuration, but the fixture
endpoints are reserved example hosts and normally do not produce successful
receipts. Use the dashboard for a deterministic presentation.

```powershell
$env:AFTERSHOCK_DATAHUB_MODE = "fixture"
$env:AFTERSHOCK_WEBHOOK_TOKEN = "replace-with-a-long-random-secret"
$env:AFTERSHOCK_REMEDIATION_ALLOWLIST_JSON = '[{"target_urn":"urn:li:dataJob:(urn:li:dataFlow:(airflow,aftershock_demo,PROD),purchase_order_generator)","entity_type":"DATA_JOB","business_action":"ISSUE_PO","endpoint":"https://api.internal.example/remediate/cancel_po"},{"target_urn":"urn:li:mlModel:(urn:li:dataPlatform:sagemaker,dynamic_pricing_model,PROD)","entity_type":"ML_MODEL","business_action":"ADJUST_PRICE","endpoint":"https://api.internal.example/remediate/revert_pricing"}]'
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
Invalid input returns 422; missing or invalid authorization returns 401;
missing authentication/remediation configuration or unavailable DataHub
processing returns a fixed, secret-safe 503. Noncritical envelopes return
`ignored` before opening a DataHub context or constructing remediation grants.

Fixture metadata is explicitly synthetic and uses `PROD` URNs. The separate
live bootstrap uses collision-checked, namespaced `DEV` assets. Follow the
[live setup guide][live-setup] for mandatory target confirmation, collision
handling, receiver requirements, and exact commands.

## Repository map

- `src/datahub_context.py` — explicit fixture and MCP context adapters
- `src/blast_radius_mapper.py` — lineage-to-structured-playbook mapping
- `src/compensating_action_engine.py` — governed execution and receipts
- `src/incident_processor.py` — read, act, and `save_document` orchestration
- `src/lineage_listener.py` — authenticated normalized-envelope API
- `src/demo_dashboard.py` — receipt-driven offline presentation
- `src/demo_contract.py` — exact seeded live-demo identities and endpoint
- `src/demo_remediation_receiver.py` — loopback-only observable demo control
- `src/live_demo.py` — live MCP workflow and three-read persistence proof
- `config/` and `scripts/` — safe live metadata bootstrap
- `tests/` — unit, MCP protocol, API, dashboard, and opt-in live tests
- `examples/` — captured outputs labeled by execution mode
- `docs/submission-checklist.md` — human actions still required before entry

## Current boundary

The MVP operates at the **system-exposure and compensating-control level**. A
lineage edge shows downstream exposure; it does not prove which individual
records a system read or which external actions resulted. The MVP therefore
does not claim row-level causality or individual-action recovery.

The proposed [Action-Provenance Ledger RFC](docs/RFC-Action-Provenance-Ledger.md)
describes a future path to action IDs, incident windows, and more selective
controls. It is a project proposal, not an upstream contribution.

## Reproducibility and live-proof status

The MCP server, FastMCP, and DataHub SDK integration dependencies are pinned.
Offline tests and demo inputs are deterministic. On August 4, 2026, the offline
suite passed `309` tests with the one opt-in live test skipped as expected. The
same live test then ran explicitly against local DataHub OSS Quickstart v1.6.0
and passed (`1 passed in 27.59s`). The final live demonstration also proved the
purchase-order transition from `issued` to `canceled`, canonical PO-bound
receipt, MCP `save_document`, searchable content, and both related-asset
backlinks. A future skipped live test still must not be presented as a new
successful live result; use the dated
[proof transcript](examples/live_demo_proof.txt) as the captured evidence.

Useful DataHub references:

- [DataHub MCP server guide][mcp]
- [Structured properties tutorial][properties]
- [DataHub Quickstart][quickstart]

## AI-assistance disclosure

Standard AI coding assistants were used during development to help scaffold,
test, review, and document the project. The project concept, acceptance
criteria, architecture decisions, and submission decisions remain under human
direction and review.

## License and submission status

Licensed under the [Apache License 2.0](LICENSE). The
[public repository](https://github.com/PhantomBugz/aftershock) contains the
reviewed implementation, setup guide, reproducible examples, captured live
proof transcript, and DataHub screenshot. GitHub's Apache-2.0 license detection
was verified on August 4, 2026. Publishing the final project and video URLs,
completing the Devpost form, and checking every judge-facing resource while
signed out remain release steps; see the
[submission checklist](docs/submission-checklist.md).

[mcp]: https://docs.datahub.com/docs/features/feature-guides/mcp
[properties]: https://docs.datahub.com/docs/api/tutorials/structured-properties
[mcl]: https://docs.datahub.com/docs/actions/events/metadata-change-log-event
[actions]: https://docs.datahub.com/docs/actions/guides/developing-an-action
[quickstart]: https://docs.datahub.com/docs/quickstart
[live-setup]: docs/live-datahub-setup.md
