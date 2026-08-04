# Real DataHub MCP Integration Design

**Status:** Approved on 2026-08-04

## Goal

Turn Aftershock from an offline GraphQL-and-webhook demonstration into an honest DataHub agent application that reads downstream context through the official DataHub MCP Server, executes compensating controls, and persists an incident-specific remediation document through MCP for later people and agents.

## Scope

The implementation will:

- call the official `get_lineage` MCP tool with `upstream=false`;
- call `get_entities` to read Aftershock playbook configuration from DataHub structured properties;
- issue real asynchronous HTTP requests to configured remediation endpoints;
- return structured receipts for succeeded, failed, and skipped controls;
- call `save_document` through MCP after the controls finish;
- link the remediation document to the corrupted source and every affected downstream asset;
- provide an explicit offline fixture mode for deterministic tests and video rehearsal;
- fail visibly in live MCP mode instead of falling back to fixtures;
- remove unsupported monetary and transaction-level claims;
- provide reproducible local-DataHub instructions, sample output, an RFC proposal, and feedback copy.

The implementation will not claim that the MVP identifies individual transactions, proves a native DataHub Actions event envelope, or has reversed production money. Those remain separate future integrations.

## Architecture

```text
DataHub incident envelope
        |
        v
AftershockIncidentProcessor
        |
        +--> BlastRadiusMapper
        |       |
        |       +--> DataHubContextPort
        |               +--> MCPDataHubContext (live)
        |               |       get_lineage(upstream=false)
        |               |       get_entities(...)
        |               |
        |               +--> FixtureDataHubContext (offline, labeled)
        |
        +--> CompensatingActionEngine
        |       +--> concurrent remediation HTTP calls
        |       +--> RemediationReceipt[]
        |
        +--> DataHubContextPort.save_remediation_document(...)
                +--> save_document via MCP
```

`MCPDataHubContext` owns only DataHub MCP concerns. `BlastRadiusMapper` converts DataHub entities into actionable targets. `CompensatingActionEngine` owns only external control execution. `AftershockIncidentProcessor` coordinates the transaction and writes the final evidence document.

## Configuration and Modes

`AFTERSHOCK_DATAHUB_MODE` accepts exactly:

- `mcp`: connect to a real MCP server. Any connection, tool, or parsing error is returned as an error; no fallback occurs.
- `fixture`: read `mock-data/datahub_lineage.json` and write the generated document to an in-memory fixture recorder. Every API response and dashboard screen identifies this as `OFFLINE FIXTURE MODE`.

Live MCP transport supports:

- `DATAHUB_MCP_URL`: streamable HTTP MCP endpoint, with the separate `DATAHUB_MCP_TOKEN` sent as a bearer token when present;
- otherwise, a stdio child process launched with the current Python interpreter as `python -m mcp_server_datahub`, receiving `DATAHUB_GMS_URL`, `DATAHUB_GMS_TOKEN`, and `TOOLS_IS_MUTATION_ENABLED=true` in its environment.

Secrets must never be printed or committed.

The live bootstrap is bounded: the structured-property CLI subprocess has a
hard 30-second timeout, while DataHub SDK calls use a 10-second request timeout
and at most one retry for the configured transient HTTP statuses. Every apply
stage converts third-party exceptions into fixed, secret-safe errors while
allowing process-control exceptions to propagate. The opt-in live proof polls
for the seeded DataJob and both playbook properties only within a fixed
monotonic deadline; it never falls back to fixture data.

## DataHub Metadata Contract

Actionable entities use two structured properties:

- `aftershock.businessAction`: stable action identifier such as `ISSUE_PO`;
- `aftershock.remediationWebhook`: absolute HTTP or HTTPS remediation endpoint.

The mapper will accept the official MCP entity response shapes and normalize them into:

```json
{
  "urn": "urn:li:dataJob:(urn:li:dataFlow:(airflow,aftershock_demo,PROD),purchase_order_generator)",
  "entity_type": "DATA_JOB",
  "business_action": "ISSUE_PO",
  "remediation_webhook": "https://api.internal.example/remediate/cancel_po"
}
```

Entities missing either required property are excluded from control execution and represented by skipped receipts rather than silently treated as successful.

## Remediation Receipts

Each attempted control returns a serializable receipt containing:

- incident ID;
- target URN and entity type;
- business action;
- endpoint host/path without credentials;
- status: `succeeded`, `failed`, or `skipped`;
- HTTP status when available;
- error summary when applicable.

The HTTP request body remains:

```json
{
  "incident_id": "INC-9942",
  "target_urn": "...",
  "action": "REVERT_STATE",
  "business_action": "ISSUE_PO"
}
```

Each request also includes an opaque, deterministic `Idempotency-Key` header
derived from the incident ID, target URN, and business action. Downstream
services can therefore deduplicate retries without Aftershock exposing those
values in the header itself.

## MCP Write-Back

After all controls settle, the processor calls `save_document` once. The document contains the incident ID, source dataset, execution mode, timestamp, and a table of all receipts. `related_assets` contains the source dataset and downstream target URNs. A deterministic document URN is reused for the same incident so retries update the record instead of creating ambiguous duplicates.

Write-back failure is reported separately from remediation failure. A successful HTTP control is never relabeled as failed merely because DataHub persistence failed, and the overall response never claims the incident is fully recorded unless `save_document` succeeds.

## Testing Strategy

Tests use a real in-process MCP transport from the same FastMCP 3.x stack used by the official DataHub MCP Server. The test server records tool names and JSON arguments while returning deterministic DataHub-shaped payloads. This verifies MCP initialization and `tools/call` behavior without claiming that Docker or a live DataHub instance was used.

TDD cycles cover:

1. `get_lineage(upstream=false)` and `get_entities` calls;
2. structured-property normalization;
3. explicit fixture/live behavior and fail-closed errors;
4. structured remediation receipts for HTTP success and failure;
5. `save_document` arguments and incident-document content;
6. listener response semantics;
7. dashboard wording and removal of unsupported numbers;
8. an opt-in live test guarded by environment variables.

## Demo Truthfulness

The default dashboard uses fixture mode and says so. It will show each deterministic HTTP-test receipt and the fixture recorder write-back receipt. A separate live command is documented for a running DataHub Quickstart and official MCP Server. The demo says “system-level compensating controls accepted” rather than “transactions reversed” or “enterprise state restored.”

## Repository Deliverables

- Apache-2.0 license retained unchanged;
- complete README and live setup guide;
- `examples/` output captured from a verified demo run;
- formal Action-Provenance Ledger RFC, clearly labeled as a project proposal rather than an accepted upstream contribution;
- feedback-survey draft with actionable DataHub developer-experience feedback;
- optional upstream contribution handled separately after this branch is finalized.
