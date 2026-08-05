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

The submission proof was run successfully on August 4, 2026 against a local
DataHub OSS Quickstart v1.6.0 instance. This is a verified local baseline, not a
claim that a remotely hosted DataHub instance is included with the project.

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
`--allow-remote-target` is supplied explicitly. Non-loopback targets must use
HTTPS; the override never permits remote plain HTTP.

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

The seeded loopback remediation URL is implemented by the repository's
`src/demo_remediation_receiver.py`. It is a stateful, loopback-only demonstration
receiver. Before the control, purchase order `PO-AFTERSHOCK-001` is `issued`
and `issue_po_enabled` is `true`; after one authorized `ISSUE_PO` compensation,
the order is `canceled` and further issuance is disabled. It also records an
application count, incident ID, and canonical PO-bound receipt ID so the
business effect is independently observable. Distinct sequential or concurrent
retry keys return that same receipt; retarget attempts fail. It is not a
production authentication or deployment pattern. Do not store endpoint
credentials in structured-property values; use the downstream service's
credential-management mechanism.

The receiver must return the v1 terminal success contract only after its
business operation is terminal:

```json
{
  "receipt_version": 1,
  "status": "succeeded",
  "receipt_id": "receiver-generated-stable-id"
}
```

An HTTP 200 without that contract is `outcome_unknown`; HTTP 202 without a
terminal v1 contract, and v1 `accepted`/`pending` on ordinary success-class
responses, are nonterminal `accepted`. A 4xx response other than 408 is a failed
rejection. HTTP 408 or a 5xx response is `outcome_unknown` unless it carries a
valid v1 terminal `succeeded` or `failed` receipt, which Aftershock honors.
Network failure or
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

## 5. Start the built-in loopback receiver

Run the stateful demonstration receiver in its own PowerShell window:

```powershell
python src\demo_remediation_receiver.py
```

It binds only to `127.0.0.1:8765` and exposes the seeded remediation endpoint.
`POST /demo/reset` restores the baseline and `GET /demo/state` exposes only the
demonstration state. Replays of the same idempotency key and payload return the
same receipt; a conflicting payload is rejected.

## 6. Run the primary live judge path

Keep the MCP configuration from step 4 and run:

```powershell
python src\live_demo.py
```

The command fails unless `AFTERSHOCK_DATAHUB_MODE=mcp`; it never substitutes
fixture data. It constructs the one exact grant for the seeded DataJob,
business action, entity type, and loopback endpoint. The expected visible
sequence is:

1. **RECEIVER BEFORE:** `PO-AFTERSHOCK-001=issued`,
   `issue_po_enabled=True`, `apply_count=0`.
2. **OBSERVE:** read downstream lineage and entity metadata through MCP.
3. **DECIDE:** resolve the one metadata-backed remediation target.
4. **ACT:** require the exact grant and a terminal v1 receipt.
5. **PERSIST:** create or update the incident document with MCP
   `save_document` after a verified bounded search.
6. **RECEIVER AFTER:** `PO-AFTERSHOCK-001=canceled`,
   `issue_po_enabled=False`, `apply_count=1`.
7. **LIVE DATAHUB READBACK VERIFIED:** all three independent reads pass.

The three read-back checks are intentionally distinct:

- `search_documents` must find the returned document URN and exact
  title;
- `grep_documents` must find the incident ID and external receipt ID in the
  saved content; and
- `get_entities` must show the document in both the dataset and DataJob
  `relatedDocuments` backlinks.

On August 4, 2026, this complete path succeeded against local DataHub OSS
Quickstart v1.6.0. The order changed from `issued` to `canceled` exactly once,
the canonical terminal receipt was
`aftershock-demo-succeeded-PO-AFTERSHOCK-001-ea74bcc907123906e7fbaf52`, and
DataHub returned
`urn:li:document:shared-015c94ed-69c0-40a6-a851-ce76a4920616`. See the
[credential-free proof transcript](../examples/live_demo_proof.txt).

## 7. Run the opt-in live contract test

Keep the MCP variables from step 4 and enable the test explicitly:

```powershell
$env:RUN_LIVE_DATAHUB_TESTS = "1"
python -m pytest tests\test_live_datahub_mcp.py -q
```

The test creates a uniquely marked document and independently polls
`search_documents`, `grep_documents`, and both related-asset projections. It
does not invoke the remediation receiver; `live_demo.py` is the end-to-end
business-action proof. On August 4, 2026, this test ran rather than skipping and
passed against the local v1.6.0 instance (`1 passed`). A later skipped
test is not evidence of a later successful live run.

## Optional: exercise the authenticated listener

The primary judge path calls the same processor directly so its proof is easy
to see. To exercise the normalized-envelope API instead, configure an exact
four-field grant and a webhook token:

```powershell
$env:AFTERSHOCK_WEBHOOK_TOKEN = "replace-with-a-long-random-secret"
$env:AFTERSHOCK_REMEDIATION_ALLOWLIST_JSON = '[{"target_urn":"urn:li:dataJob:(urn:li:dataFlow:(airflow,aftershock_demo,DEV),purchase_order_generator)","entity_type":"DATA_JOB","business_action":"ISSUE_PO","endpoint":"http://127.0.0.1:8765/remediate/cancel_po"}]'
python -m uvicorn lineage_listener:app --app-dir src --host 127.0.0.1 --port 8000
```

Authorization matches all four fields exactly. The endpoint's scheme, host,
port, path, and query must also match. User information and fragments are
rejected, plain HTTP is limited to loopback, nonloopback controls require HTTPS,
and redirects are disabled. The operator remains responsible for DataHub RBAC,
receiver authentication, and network egress policy.

Send the normalized envelope from a second PowerShell window:

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

This payload is an Aftershock contract, not a DataHub event schema. A production
integration would translate an `incidentInfo` MetadataChangeLogEvent through a
custom DataHub Action into this authenticated contract. That adapter remains
future work; see the
[event reference](https://docs.datahub.com/docs/actions/events/metadata-change-log-event)
and [custom Action guide](https://docs.datahub.com/docs/actions/guides/developing-an-action).

## Fixture mode reference

Fixture mode is always explicit:

```powershell
$env:AFTERSHOCK_DATAHUB_MODE = "fixture"
python src\demo_dashboard.py
```

The dashboard additionally uses `httpx.MockTransport` and an in-memory
write-back recorder. It is the reproducible presentation path, not a live
persistence claim.
