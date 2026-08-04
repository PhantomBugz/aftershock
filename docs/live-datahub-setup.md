# Live DataHub setup

This guide separates the deterministic fixture workflow from the live MCP
workflow. Aftershock never changes from a failed live call to fixture data.

## 1. Install Aftershock

Use Python 3.12 in PowerShell:

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
python -m pip install -r requirements.txt
```

## 2. Start DataHub

Bring up a local DataHub deployment by following the current official
[DataHub Quickstart](https://docs.datahub.com/docs/quickstart). The exact
launcher options can change, so this repository does not duplicate a
version-specific Quickstart command. Confirm that GMS is reachable and note
its base URL and, when authentication is enabled, its token.

The official [DataHub MCP guide](https://docs.datahub.com/docs/features/feature-guides/mcp)
describes server requirements and mutation support. With
`mcp-server-datahub==0.6.0`, `save_document` requires DataHub OSS 1.4.0 or
newer, or DataHub Cloud 0.3.16 or newer. Before bootstrap or the live proof
gate, verify the target version, confirm that `save_document` is available,
and confirm that MCP mutation tools are enabled.

The DataHub operator must also grant the bootstrap identity only the required
asset, lineage, and structured-property write permissions, and grant the live
MCP identity only the reads plus document mutation it needs. Aftershock does
not configure DataHub RBAC.

## 3. Define and seed the demo metadata

Configure the DataHub CLI for the target instance. Set the GMS URL before the
preview so the plan can display the target origin. The preview strips paths,
queries, user information, and credentials; it does not create a client or
make a network call:

```powershell
$env:DATAHUB_GMS_URL = "http://localhost:8080"
Remove-Item Env:DATAHUB_GMS_TOKEN -ErrorAction SilentlyContinue
# If the instance requires authentication, set its real token in this shell:
# $env:DATAHUB_GMS_TOKEN = "replace-with-gms-token"
python scripts\bootstrap_datahub_demo.py --dry-run
```

Review the printed canonical target origin, uniquely namespaced `DEV` URNs,
property values, and lineage edge. Copy that origin exactly into the mandatory
confirmation argument, including its port, and then apply the same plan:

```powershell
python scripts\bootstrap_datahub_demo.py `
  --apply `
  --confirm-target "http://localhost:8080"
```

The bootstrap first checks all three exact asset URNs for collisions, before
applying structured-property definitions or any asset mutation. It fails closed
if one already exists. Only for a deliberate idempotent rerun of these exact
demo assets, add `--allow-existing-demo-assets`. A non-loopback target is also
refused unless its canonical origin is confirmed and
`--allow-remote-target` is supplied explicitly.

Exact collision-override example for the local demo namespace:

```powershell
python scripts\bootstrap_datahub_demo.py `
  --apply `
  --confirm-target "http://localhost:8080" `
  --allow-existing-demo-assets
```

Exact remote-target form (replace the origin with the one printed by your dry
run and review it before proceeding):

```powershell
python scripts\bootstrap_datahub_demo.py `
  --apply `
  --confirm-target "https://datahub.example:443" `
  --allow-remote-target
```

The live bootstrap seeds exactly these namespaced `DEV` assets:

```text
urn:li:dataset:(urn:li:dataPlatform:postgres,aftershock_demo.inventory_pricing,DEV)
urn:li:dataFlow:(airflow,aftershock_demo,DEV)
urn:li:dataJob:(urn:li:dataFlow:(airflow,aftershock_demo,DEV),purchase_order_generator)
```

It assigns both remediation properties to the DataJob and creates one
Dataset-to-DataJob lineage edge. It deliberately does not manufacture
Dataset-to-MLModel lineage. The offline fixture remains an explicitly synthetic
`PROD` graph and includes a labeled MLModel example only to exercise mixed
target rendering and receipt handling; its URNs are not the live seed URNs.

The seeded loopback remediation URL is a placeholder. No receiver is created
by the bootstrap, so a control receipt will not succeed unless an authorized
receiver exists at that exact URL and it is explicitly allowlisted in
`AFTERSHOCK_REMEDIATION_ALLOWLIST_JSON`. Do not store endpoint credentials in
structured-property values; use the downstream service's credential-management
mechanism.

The receiver must return the v1 terminal success contract only after its
business operation is terminal:

```json
{
  "receipt_version": 1,
  "status": "succeeded",
  "receipt_id": "receiver-generated-stable-id"
}
```

An HTTP 200 without that contract is `outcome_unknown`; HTTP 202 and v1
`accepted`/`pending` on ordinary success-class responses are nonterminal
`accepted`. A 4xx response other than 408 is a failed rejection. HTTP 408 or a
5xx response is `outcome_unknown` unless it carries a valid v1 terminal
`succeeded` or `failed` receipt, which Aftershock honors. Network failure or
workflow deadline expiry after dispatch is conservatively `outcome_unknown`;
work that never dispatches before the deadline is `skipped`. The stable
`Idempotency-Key` supports receiver-side deduplication but does not prove
exactly-once execution.

## 4. Choose exactly one MCP transport

### Local stdio MCP server

The stdio path launches the installed `mcp-server-datahub==0.6.0` as a child
process. `DATAHUB_GMS_TOKEN` is optional only when the target permits
unauthenticated access.

```powershell
$env:AFTERSHOCK_DATAHUB_MODE = "mcp"
$env:DATAHUB_GMS_URL = "http://localhost:8080"
Remove-Item Env:DATAHUB_GMS_TOKEN -ErrorAction SilentlyContinue
# If the instance requires authentication:
# $env:DATAHUB_GMS_TOKEN = "replace-with-gms-token"
Remove-Item Env:DATAHUB_MCP_URL -ErrorAction SilentlyContinue
Remove-Item Env:DATAHUB_MCP_TOKEN -ErrorAction SilentlyContinue
```

Aftershock starts this local child with `DATAHUB_SKIP_CONFIG=true` and
`TOOLS_IS_MUTATION_ENABLED=true`. It forwards only the DataHub variables needed
by that child, rather than copying the full parent environment.

### Remote MCP server

For an already-running Streamable HTTP MCP server:

```powershell
$env:AFTERSHOCK_DATAHUB_MODE = "mcp"
$env:DATAHUB_MCP_URL = "https://your-mcp-host.example/mcp"
$env:DATAHUB_MCP_TOKEN = "replace-with-the-mcp-server-token"
Remove-Item Env:DATAHUB_GMS_URL -ErrorAction SilentlyContinue
Remove-Item Env:DATAHUB_GMS_TOKEN -ErrorAction SilentlyContinue
```

The remote server itself must be configured with mutation tools enabled and
with permission to read lineage/entities and save documents. The MCP bearer
token is distinct from a GMS token; Aftershock does not substitute one for the
other.

## 5. Start the authenticated listener

Configure an exact outbound policy. The value must be a nonempty JSON array of
complete URLs. The seeded live endpoint is loopback and has no receiver until
you start one you are authorized to operate:

```powershell
$env:AFTERSHOCK_WEBHOOK_TOKEN = "replace-with-a-long-random-secret"
$env:AFTERSHOCK_REMEDIATION_ALLOWLIST_JSON = '["http://127.0.0.1:8765/remediate/cancel_po"]'
python -m uvicorn lineage_listener:app --app-dir src --host 127.0.0.1 --port 8000
```

The metadata URL and allowlist entry must match exactly, including scheme,
host, port, path, and query. User information and fragments are rejected.
Plain HTTP is accepted only for `localhost` or an IP loopback address;
nonloopback control endpoints require HTTPS. Redirects are never followed.
Never place a credential in the URL or DataHub metadata. Receiver
authentication and firewall/service-mesh policy must be supplied outside that
metadata.

The operator remains responsible for restricting process and network egress so
Aftershock can reach only approved receivers. The application allowlist is one
policy layer; it is not a replacement for infrastructure egress controls.

Send the current normalized envelope from a second PowerShell window:

```powershell
$headers = @{ Authorization = "Bearer replace-with-a-long-random-secret" }
$body = @{
  incident_id = "INC-LIVE-001"
  dataset_urn = "urn:li:dataset:(urn:li:dataPlatform:postgres,aftershock_demo.inventory_pricing,DEV)"
  severity = "CRITICAL"
} | ConvertTo-Json

Invoke-RestMethod `
  -Method Post `
  -Uri "http://127.0.0.1:8000/webhook/datahub" `
  -Headers $headers `
  -ContentType "application/json" `
  -Body $body
```

This request initiates real configured control calls. The engine uses bounded
concurrency (eight workers) and a 30-second workflow deadline by default.
Inspect all five receipt counts (`succeeded`, `accepted`, `failed`, `skipped`,
and `outcome_unknown`), every external receipt ID, and the returned write-back
document URN. Overall `completed` requires every target to have terminal
`succeeded` plus a successful DataHub write-back.

The payload above is an Aftershock contract, not a DataHub event schema. The
production ingress boundary is:

```text
incidentInfo MetadataChangeLogEvent
  -> custom DataHub Action adapter
  -> normalized authenticated POST
  -> Aftershock
```

Implementing that custom adapter remains future integration work. Use the
[event reference](https://docs.datahub.com/docs/actions/events/metadata-change-log-event)
and [custom Action guide](https://docs.datahub.com/docs/actions/guides/developing-an-action)
when building it.

## 6. Run the opt-in live proof gate

Keep the MCP variables from step 4 and enable the test explicitly:

```powershell
$env:RUN_LIVE_DATAHUB_TESTS = "1"
python -m pytest tests\test_live_datahub_mcp.py -q
```

Before treating this as a live success, verify that the test ran rather than
being skipped, that all MCP calls passed, and that the incident document is
visible in the configured DataHub instance. Without those observations, only
the offline and in-process protocol contracts have been verified.

The opt-in live test validates seeded lineage/property discovery and
`save_document`; it does not start the placeholder receiver or prove an
external control outcome.

## Fixture mode reference

Fixture mode is always explicit:

```powershell
$env:AFTERSHOCK_DATAHUB_MODE = "fixture"
python src\demo_dashboard.py
```

The dashboard additionally uses `httpx.MockTransport` and an in-memory
write-back recorder. It is the reproducible presentation path, not a live
persistence claim.
