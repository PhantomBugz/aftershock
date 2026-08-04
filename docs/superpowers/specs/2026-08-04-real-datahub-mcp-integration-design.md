# Real DataHub MCP Integration Design

**Status:** Approved on 2026-08-04; reconciled with the governed receipt and
safe-bootstrap contracts.

## Goal and category fit

Aftershock is an honest DataHub agent application in the **Agents That Do Real
Work** category. It is a deterministic, policy-driven loop:

```text
observe context -> decide from governed metadata/policy
                -> act through an allowlisted control
                -> persist evidence for later people and agents
```

The project does not claim generative or LLM reasoning. It begins after an
authenticated Aftershock-normalized incident trigger.

## Scope

The implementation:

- reads downstream context through the official DataHub MCP Server;
- calls `get_lineage(upstream=false)` and `get_entities`;
- maps exact DataHub structured-property qualified names into typed targets;
- applies an exact outbound URL policy before dispatch;
- executes controls with bounded concurrency and a workflow deadline;
- classifies five factual receipt states without inferring business success
  from HTTP transport alone;
- calls `save_document` through MCP after controls settle;
- links the incident document to the source and downstream asset URNs;
- provides explicit, labeled fixture mode for deterministic verification; and
- provides a collision-checked, confirmed-target `DEV` bootstrap for a live
  DataHub instance.

The MVP does not claim record-level causality, individual-action recovery, a
DataHub Action ingress adapter, a completed live DataHub run in this development
environment, or exactly-once control execution.

## Architecture

```text
incidentInfo MetadataChangeLogEvent
        |
        +--> custom DataHub Action adapter (production boundary; not implemented)
        |
authenticated Aftershock-normalized envelope
        |
        v
AftershockIncidentProcessor
        |
        +--> BlastRadiusMapper
        |       +--> DataHubContextPort
        |               +--> MCPDataHubContext
        |               |       get_lineage(upstream=false)
        |               |       get_entities(...)
        |               +--> FixtureDataHubContext (explicitly labeled)
        |
        +--> CompensatingActionEngine
        |       +--> exact URL policy
        |       +--> bounded workers + workflow deadline
        |       +--> governed HTTP POSTs, redirects disabled
        |       +--> five-state RemediationReceipt[]
        |
        +--> DataHubContextPort.save_document(...)
                +--> incident summary through MCP
```

`MCPDataHubContext` owns MCP transport and tool normalization.
`BlastRadiusMapper` owns lineage/entity-to-target mapping.
`CompensatingActionEngine` owns outbound policy, execution, and receipt
classification. `AftershockIncidentProcessor` coordinates the loop and writes
one incident document.

## DataHub MCP contract

Live MCP mode uses `mcp-server-datahub==0.6.0` and FastMCP 3.4.5. It supports:

- `DATAHUB_MCP_URL` with a separate optional `DATAHUB_MCP_TOKEN` for a remote
  Streamable HTTP MCP server; or
- a local stdio child launched with the current Python interpreter, explicit
  `DATAHUB_GMS_URL`, optional `DATAHUB_GMS_TOKEN`,
  `DATAHUB_SKIP_CONFIG=true`, and `TOOLS_IS_MUTATION_ENABLED=true`.

GMS credentials are never forwarded to an arbitrary remote MCP URL. MCP
connection, tool, payload, or pagination errors fail closed; live mode never
changes to fixture data.

The adapter calls `get_lineage` with `upstream=false`, `max_results=100`, and
`max_hops=3`. For the pinned server, `max_hops=3` is the sentinel for unlimited
lineage traversal; it must not be described as a finite hop cap. The adapter
validates offsets, totals, returned counts, continuation state, repeated pages,
and global safety limits before accepting a blast radius.

`save_document` with this server version requires DataHub OSS 1.4.0 or newer,
or DataHub Cloud 0.3.16 or newer, plus enabled mutation tools and appropriate
document-write permission.

## Metadata contract

Actionable DataJob or MLModel entities use two single-string structured
properties, matched by exact qualified name:

- `aftershock.businessAction` — stable action identifier such as `ISSUE_PO`;
- `aftershock.remediationWebhook` — complete HTTP(S) control endpoint.

The mapper retains a lineage target even when either property is absent. The
engine then emits a `skipped` receipt; missing metadata is never interpreted as
success.

## Governed endpoint policy

The listener requires `AFTERSHOCK_REMEDIATION_ALLOWLIST_JSON`, a nonempty JSON
array of exact URL strings. Policy rules are:

- metadata URL and allowlist entry must match exactly, including query/path;
- user information, fragments, whitespace, and control characters are denied;
- nonloopback endpoints require HTTPS; loopback HTTP is allowed for local
  development;
- redirects are disabled even if the supplied HTTP client would follow them;
- query information is removed from logs and persisted endpoint fields; and
- a missing, malformed, or empty allowlist fails before context creation.

Credentials must not be placed in structured properties or URLs. DataHub
property/document RBAC, receiver authentication, and network egress controls
remain operator responsibilities.

## Request and idempotency contract

The control request body is:

```json
{
  "incident_id": "INC-9942",
  "target_urn": "urn:li:dataJob:...",
  "action": "REVERT_STATE",
  "business_action": "ISSUE_PO"
}
```

Each dispatch includes a deterministic opaque `Idempotency-Key` derived from
the incident ID, target URN, and business action. It is a stable retry key for
the receiver, not proof of exactly-once execution.

## Receipt contract

Each immutable receipt contains incident ID, target URN/type, business action,
sanitized endpoint, status, HTTP status when observed, external receipt ID when
valid, and a controlled error summary.

The five statuses are:

- `succeeded` — only an eligible non-202 response containing the required v1
  terminal success contract with a valid external receipt ID; 408/5xx are
  eligible only when that terminal contract is present;
- `accepted` — HTTP 202 or an accepted/pending acknowledgment on an ordinary
  success-class response; it is nonterminal;
- `failed` — pre-dispatch failure, 4xx rejection other than 408, disabled
  redirect, or a valid v1 terminal failure receipt;
- `skipped` — incomplete/denied policy or deadline expiry before dispatch;
- `outcome_unknown` — 408/5xx without a valid terminal success/failure receipt,
  transport failure after dispatch, deadline expiry after dispatch, or a
  success-class response without a valid terminal receipt.

Terminal success requires:

```json
{
  "receipt_version": 1,
  "status": "succeeded",
  "receipt_id": "receiver-generated-stable-id"
}
```

HTTP status alone is never terminal business-success evidence. A valid v1
terminal success/failure receipt on 408/5xx is authoritative; otherwise these
ambiguous statuses remain unknown. Extra response fields do not change
classification and are not logged.

## Bounded execution

The engine defaults to at most eight workers and one 30-second deadline across
the control phase. It preserves input order. When the deadline expires, a
dispatched target is `outcome_unknown`; a target not yet dispatched is
`skipped`. Process-control cancellation propagates after worker cleanup.

## DataHub write-back and completion

After controls settle, the processor calls `save_document` once. The Summary
contains the incident/source/mode/timestamp and every receipt, including the
external receipt ID. `related_assets` contains the unique source and target
URNs. A deterministic incident/source-derived document URN makes retries update
the same logical record.

Write-back failure is separate from control status. Overall API/dashboard
`completed` requires all discovered receipts to be `succeeded` and the
write-back to be `succeeded`; any accepted, failed, skipped, unknown, or
write-back failure yields `completed_with_issues`.

## Ingress contract

The FastAPI route accepts an authenticated normalized envelope containing
`incident_id`, Dataset URN, and severity. Critical requests require a bearer
value matching `AFTERSHOCK_WEBHOOK_TOKEN`. Noncritical input is ignored before
authentication, context construction, or allowlist parsing.

Production integration is:

```text
DataHub incidentInfo MetadataChangeLogEvent
  -> custom DataHub Action adapter
  -> authenticated normalized POST
  -> Aftershock
```

The custom Action adapter is not part of the MVP.

## Safe live bootstrap

The bootstrap creates exactly these synthetic, namespaced assets:

- `urn:li:dataset:(urn:li:dataPlatform:postgres,aftershock_demo.inventory_pricing,DEV)`;
- `urn:li:dataFlow:(airflow,aftershock_demo,DEV)`;
- `urn:li:dataJob:(urn:li:dataFlow:(airflow,aftershock_demo,DEV),purchase_order_generator)`.

Dry-run creates no client and prints only a sanitized canonical target origin.
Apply requires `--confirm-target` equal to that origin. A nonloopback origin
also requires `--allow-remote-target`. All three exact URNs are checked for
collisions before any mutation; only a deliberate rerun may use
`--allow-existing-demo-assets`.

The bootstrap seeds Dataset-to-DataJob lineage only. Its loopback control URL
is a non-running placeholder. An authorized receiver with the v1 receipt
contract and an exact allowlist entry is required before that control can
succeed.

## Testing and truthfulness

Tests use real in-process MCP initialization and `tools/call` exchanges through
FastMCP plus deterministic DataHub-shaped responses. They cover pagination,
structured properties, exact endpoint policy, redirects, concurrency,
deadlines, all receipt states, write-back, listener authentication, and safe
bootstrap behavior. These tests do not represent a live DataHub deployment.

The Rich dashboard is labeled `OFFLINE FIXTURE MODE`. Its HTTP test doubles
return valid terminal v1 receipts with external receipt IDs, and its DataHub
write-back is an in-memory recorder. Live persistence remains pending until a
fresh, non-skipped live gate succeeds and the document is independently seen in
the configured DataHub instance.
