# Aftershock: From Data Lineage to Governed Operational Recovery

**Primary challenge category:** Agents That Do Real Work

**Project/test URL:** <https://aftershock.phantombugz.com>

**Source repository:** <https://github.com/PhantomBugz/aftershock>

**Demo video:** <https://youtu.be/yopSGs_kx5s>

## Inspiration

Data-quality tools can identify an unreliable dataset, and lineage can show
which systems are downstream. But an automated job may already be acting on
that data. In the demonstrated scenario, bad pricing has already issued a
purchase order. Repairing the table does not cancel it or stop the next one. We
call that unresolved operational follow-up **Action Debt**.

## What it does

Aftershock is a deterministic, auditable agent loop built on DataHub:

1. **Observe:** read downstream lineage and entity context through DataHub MCP.
2. **Decide:** resolve structured playbook metadata and require an exact
   operator grant for the target URN, entity type, business action, and endpoint.
3. **Act:** invoke a bounded compensating control and require a factual terminal
   receipt rather than inferring success from HTTP alone.
4. **Persist:** write the incident, action receipt, and related assets back to
   DataHub, then independently read them back.

The demonstrated decision loop is policy-driven, autonomous after its trigger,
and reproducible from governed context and policy.

## How DataHub is foundational

Aftershock extends DataHub instead of rebuilding its catalog:

- `get_lineage(upstream=false)` discovers downstream exposure.
- `get_entities` resolves assets plus the governed structured properties
  `aftershock.businessAction` and `aftershock.remediationWebhook`.
- `search_documents` locates a verified prior incident record for
  duplicate-resistant replay.
- `save_document` creates or updates durable incident memory related to the
  source dataset and downstream job.
- post-write `search_documents` proves the returned document URN and exact title.
- `grep_documents` proves the incident and external receipt IDs were persisted.
- a second `get_entities` read proves both related assets link back to the saved
  document.

The implementation uses the official `mcp-server-datahub==0.6.0` through
FastMCP. Live failures fail closed and never switch to fixture data.

## Functioning live demonstration

The primary demo runs against a local DataHub OSS Quickstart v1.6.0 instance
and a built-in loopback-only receiver. The receiver starts with purchase order
`PO-AFTERSHOCK-001` already `issued` and further issuance enabled. Aftershock
reads the seeded lineage and playbook metadata, authorizes exactly one
`ISSUE_PO` compensation, and sends the cancellation control.

The receiver returns one canonical, PO-bound terminal v1 receipt, changes the
order from `issued` to `canceled`, disables further issuance, and increments
`apply_count` from zero to one. Sequential and concurrent retries resolve to
the same receipt rather than manufacturing additional success. Aftershock then
saves that receipt summary to DataHub and requires all three independent
read-back checks to pass before displaying
`LIVE DATAHUB READBACK VERIFIED`.

On August 4, 2026, the complete OBSERVE/DECIDE/ACT/PERSIST path succeeded. The
saved record was visible in the local DataHub UI and linked to both the dataset
and DataJob. A separate opt-in live MCP contract test also passed (`1 passed`).
The deterministic offline suite passed `309` tests with the one live
test skipped only when its explicit opt-in was absent.

The August 5 release hardening passes `320` tests with that same opt-in test
skipped. Two consecutive real MCP runs reused one canonical incident Document,
kept the existing exact-title record count unchanged, and passed all three
read-back gates. The receiver idempotency key and DataHub record key are stable
replay controls, not a claim of globally atomic exactly-once execution.

## Receipts, not inferred outcomes

A control succeeds only when the receiver returns the strict terminal contract:

```json
{"receipt_version": 1, "status": "succeeded", "receipt_id": "..."}
```

Aftershock distinguishes `succeeded`, `accepted`, `failed`, `skipped`, and
`outcome_unknown`. HTTP 202 is nonterminal unless its body carries a valid v1
terminal receipt. Ambiguous timeouts and 5xx responses remain unknown unless a
valid terminal receipt proves the outcome. A stable
idempotency key supports safe receiver-side replay, while bounded concurrency,
a workflow deadline, disabled redirects, and a 64 KiB response limit constrain
execution.

Overall completion requires every discovered target to succeed and the DataHub
write-back to succeed.

## Why it is different

Most lineage tools stop at “what might be affected?” Aftershock turns governed
catalog context into a bounded operational response and returns the evidence to
the catalog. **Action Debt** names the real-world gap between finding unreliable
data and settling the downstream actions it already triggered.

The purchase-order scenario makes that value concrete: a bad pricing feed can
be discovered in DataHub, the exposed ordering job can be identified, an
already-issued order can be canceled, further issuance can be stopped through
an exact grant, and the resulting PO-bound receipt can be preserved for the
next operator or agent.

## Technical execution and safety

- Exact immutable grants bind asset, entity type, business action, and endpoint.
- Plain HTTP is permitted only on loopback; nonloopback controls require HTTPS.
- User information and fragments are rejected and redirects are disabled.
- The receiver exposes an observable before/after state and deterministic replay
  behavior.
- Distinct sequential or concurrent retry keys return the one canonical
  cancellation receipt; retarget attempts fail.
- MCP payloads and document read-back are strictly validated and deadline-bound.
- Credentials never belong in DataHub properties, URLs, screenshots, or proof
  transcripts.

DataHub RBAC, production receiver authentication, and infrastructure egress
policy remain operator responsibilities.

## Current boundary and next step

The FastAPI endpoint begins after an authenticated, normalized incident trigger.
A production deployment would translate a DataHub `incidentInfo`
MetadataChangeLogEvent through a custom DataHub Action; that adapter is future
integration work.

The MVP proves system-level exposure and compensating-control receipts, not
row-level causality. The proposed Action-Provenance Ledger would add stable
action IDs, incident windows, and coverage evidence for more selective recovery.

## Built with

Python 3.12, DataHub OSS, DataHub MCP Server, FastMCP, FastAPI, HTTPX, Rich, and
pytest. The project is open source under Apache License 2.0.
