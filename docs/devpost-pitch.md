# Aftershock: From Data Lineage to Governed Operational Recovery

**Primary challenge category:** Agents That Do Real Work

## Inspiration

Data-quality tools can identify an unreliable dataset, and lineage can show
which systems are downstream. But an automated job or model may already be
acting on that data. Repairing the dataset does not settle the operational
follow-up. We call that follow-up **Action Debt**.

## What it does

Aftershock is a deterministic, policy-driven agent loop built on DataHub:

1. **Observe:** read downstream lineage and entity context through DataHub MCP.
2. **Decide:** resolve structured playbook metadata and enforce an exact
   operator-provided endpoint policy.
3. **Act:** invoke bounded compensating controls.
4. **Persist memory:** write the factual receipt summary back with
   `save_document` so the next operator or agent inherits it.

This is an agent because it closes that governed loop after an authenticated
trigger. It does not claim generative or LLM reasoning.

## How DataHub is foundational

- `get_lineage(upstream=false)` discovers downstream exposure. In the pinned
  server contract, `max_hops=3` requests unlimited traversal rather than a
  finite hop boundary.
- `get_entities` resolves exposed assets and their structured properties.
- `aftershock.businessAction` and `aftershock.remediationWebhook` provide the
  governed control mapping.
- `save_document` persists receipt evidence and related asset URNs back into
  DataHub.

The implementation uses `mcp-server-datahub==0.6.0` over FastMCP. A failed
live MCP call fails closed and never changes to fixture data.

## Receipts, not inferred outcomes

Aftershock reports five states: `succeeded`, `accepted`, `failed`, `skipped`,
and `outcome_unknown`. A control is `succeeded` only when an eligible non-202
response contains this terminal v1 contract with a valid external receipt ID:

```json
{"receipt_version": 1, "status": "succeeded", "receipt_id": "..."}
```

HTTP 202 and accepted/pending receipts on ordinary success-class responses are
nonterminal. A 4xx other than 408 is a failed rejection. HTTP 408 and 5xx are
`outcome_unknown` unless they carry a valid v1 terminal success/failure receipt,
which is honored. Transport failure, deadline expiry after dispatch, or a 2xx
without the terminal contract is also `outcome_unknown`; undispatched work is
`skipped`. HTTP transport success alone is never presented as business success.

Overall completion requires every discovered target to return terminal success
and the DataHub write-back to succeed. The dashboard and saved document include
external receipt IDs and preserve unresolved states.

## Governed execution

The listener requires a nonempty exact URL allowlist. User information and
fragments are forbidden, nonloopback endpoints require HTTPS, paths and queries
are authorized exactly, and redirects are disabled. The engine bounds worker
concurrency and the workflow deadline. A deterministic idempotency key supports
receiver-side deduplication but is not exactly-once proof.

DataHub RBAC, network egress, receiver authentication, and credential handling
remain operator responsibilities. Credentials do not belong in DataHub
properties or endpoint URLs.

## Why it is different

Aftershock composes catalog context with an operational control loop. It goes
beyond displaying impact: governed DataHub metadata determines which controls
may be attempted, and DataHub receives the resulting evidence. The project
extends DataHub rather than rebuilding its catalog or lineage.

## Demo and technical execution

The terminal demonstration is explicitly labeled **OFFLINE FIXTURE MODE**. It
uses deterministic lineage, an in-memory exact allowlist, mocked HTTP responses
with valid terminal receipt IDs, and an in-memory document recorder. The test
suite separately performs genuine MCP/JSON-RPC exchanges against an in-process
FastMCP server and tests pagination, policy denial, redirects, deadlines,
receipt classification, API authentication, and `save_document` arguments.

A live MCP adapter, safe `DEV` bootstrap, and opt-in live test are included.
Live persistence was not run in the development environment because no live
DataHub deployment was available; a skipped live test is not evidence of a
live result.

## Event integration boundary

The FastAPI route begins after an authenticated Aftershock-normalized incident
trigger. In production, a custom DataHub Action would translate an
`incidentInfo` MetadataChangeLogEvent into that contract. That adapter is not
implemented in this MVP.

## Current limit and next step

The MVP proves system-level exposure discovery, governed control attempts, and
factual receipts. Lineage alone does not establish which individual records a
downstream system read or which external actions followed. The proposed
Action-Provenance Ledger would add stable action IDs, incident windows,
coverage evidence, and append-oriented control receipts for more selective
recovery.

## Built with

Python 3.12, DataHub MCP Server, FastMCP, FastAPI, HTTPX, Rich, and pytest.
