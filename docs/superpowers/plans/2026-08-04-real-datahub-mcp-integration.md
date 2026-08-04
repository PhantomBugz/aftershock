# Real DataHub MCP Integration Implementation Plan

**Status:** Implemented in an isolated feature branch; reconciled with the
governed receipt, allowlist, deadline, and safe-bootstrap contracts. Public
submission steps remain separate human work.

## Objective

Build Aftershock as a deterministic DataHub agent loop for **Agents That Do
Real Work**:

```text
observe DataHub -> decide from metadata and policy -> act -> save evidence
```

The system must not infer business success from HTTP transport, silently change
from live MCP to fixtures, or present fixture/protocol tests as a live DataHub
run.

## Pinned stack

- Python 3.12
- `mcp-server-datahub==0.6.0`
- `fastmcp==3.4.5`
- `acryl-datahub==1.6.0.17`
- FastAPI, HTTPX, Rich, and pytest

## Task 1 — MCP context and explicit modes

**Files:** `src/datahub_context.py`, `tests/mcp_test_server.py`,
`tests/test_datahub_context.py`, dependency manifests.

- [x] Implement a `DataHubContextPort` with MCP and fixture adapters.
- [x] Use genuine FastMCP initialization and `tools/call` exchanges in protocol
  tests.
- [x] Support stdio MCP with explicit GMS configuration and remote Streamable
  HTTP MCP with a separate MCP token.
- [x] Parse DataHub tool results from decoded data, structured content, or one
  JSON text block; reject errors and ambiguous shapes with secret-safe output.
- [x] Make `AFTERSHOCK_DATAHUB_MODE` accept only `mcp` or `fixture` and never
  change modes after a live failure.
- [x] Paginate `get_lineage` fail-closed with offset/total/continuation and
  repeated-page validation.

The adapter sends `upstream=false`, `max_results=100`, and `max_hops=3`. For
the pinned server, `max_hops=3` means unlimited traversal. Tests and documents
must not describe it as a finite hop limit.

## Task 2 — Typed blast-radius mapping

**Files:** `src/remediation_models.py`, `src/blast_radius_mapper.py`,
`mock-data/datahub_lineage.json`, mapper tests.

- [x] Discover downstream URNs through `get_lineage`.
- [x] Fetch unique entity details in one `get_entities` batch.
- [x] Read `aftershock.businessAction` and
  `aftershock.remediationWebhook` by exact qualified name.
- [x] Preserve lineage order and ambiguous/missing targets rather than
  selecting an arbitrary response.
- [x] Make missing playbook data produce a later `skipped` receipt.
- [x] Label the synthetic fixture, including its MLModel example, as offline
  data rather than evidence of a live lineage run.

## Task 3 — Governed outbound execution

**Files:** `src/compensating_action_engine.py`, `src/remediation_models.py`,
engine tests.

- [x] Require a nonempty exact URL policy from
  `AFTERSHOCK_REMEDIATION_ALLOWLIST_JSON` in listener sessions.
- [x] Authorize scheme/host/port/path/query exactly; do not prefix-match.
- [x] Reject user information, fragments, whitespace, and controls.
- [x] Allow plain HTTP only for loopback; require HTTPS for nonloopback.
- [x] Disable redirects at dispatch.
- [x] Strip user/query/fragment information from logs and persisted endpoints.
- [x] Preserve operator responsibility for DataHub RBAC, receiver
  authentication, and infrastructure egress controls.
- [x] Never instruct operators to store credentials in metadata or URLs.

## Task 4 — Five-state terminal receipt contract

**Files:** engine, shared models, processor, listener, dashboard, and their
focused tests.

- [x] Add `external_receipt_id` and statuses `succeeded`, `accepted`, `failed`,
  `skipped`, and `outcome_unknown`.
- [x] Classify `succeeded` only for an eligible non-202 response with the v1
  terminal contract; 408/5xx are eligible only when that contract is present:

```json
{
  "receipt_version": 1,
  "status": "succeeded",
  "receipt_id": "receiver-generated-stable-id"
}
```

- [x] Treat HTTP 202 and v1 accepted/pending as nonterminal `accepted`.
- [x] Treat 4xx other than 408, disabled redirect, pre-dispatch failure, or a
  valid v1 terminal failure as `failed`.
- [x] Treat 408/5xx without a valid v1 terminal success/failure receipt,
  post-dispatch transport error, deadline expiry after dispatch, or
  missing/invalid terminal response as `outcome_unknown`.
- [x] Treat policy denial, missing playbook data, and deadline expiry before
  dispatch as `skipped`.
- [x] Preserve observed HTTP status and valid external receipt ID without
  recording arbitrary response content.
- [x] Include a deterministic `Idempotency-Key` while stating that it is not
  proof of exactly-once execution.

## Task 5 — Bounded processor and write-back

**Files:** `src/incident_processor.py`, processor tests.

- [x] Bound engine workers (default eight) and the control-phase deadline
  (default 30 seconds), preserving input order.
- [x] Cancel/clean workers on deadline while classifying dispatched vs
  undispatched targets conservatively.
- [x] Render incident Markdown containing all receipt fields, including the
  external receipt ID.
- [x] Call `save_document` exactly once after controls settle with a
  deterministic source/incident-derived document URN.
- [x] Include unique source/target URNs in `related_assets`.
- [x] Keep write-back failure separate from already observed control states.
- [x] Define overall `completed` as all receipts terminal `succeeded` plus a
  successful DataHub write-back. Every other combination is
  `completed_with_issues`.

## Task 6 — Authenticated ingress and truthful presentation

**Files:** `src/lineage_listener.py`, `src/demo_dashboard.py`, API/dashboard
tests.

- [x] Accept an Aftershock-normalized envelope and require bearer
  `AFTERSHOCK_WEBHOOK_TOKEN` for critical input.
- [x] Ignore noncritical input before authentication, allowlist parsing, or
  context construction.
- [x] Return fixed, secret-safe errors for auth/config/MCP/mapping failures.
- [x] Return counts for all five receipt states and include external IDs.
- [x] Display the same receipt and completion semantics in Rich.
- [x] Mark the default presentation `OFFLINE FIXTURE MODE`.
- [x] Make fixture endpoints exactly allowlisted and return deterministic v1
  terminal test receipts through `httpx.MockTransport`.

Production ingress remains:

```text
incidentInfo MetadataChangeLogEvent
  -> custom DataHub Action adapter
  -> authenticated normalized POST
  -> Aftershock
```

The custom Action adapter is not implemented in this plan.

## Task 7 — Safe live metadata bootstrap

**Files:** `config/aftershock_structured_properties.yaml`,
`scripts/bootstrap_datahub_demo.py`, bootstrap/live tests.

- [x] Pin the DataHub SDK and define the two SINGLE string properties for
  DataJob/MLModel targets.
- [x] Seed namespaced `DEV` Dataset, DataFlow, and DataJob assets and only a
  supported Dataset-to-DataJob lineage edge.
- [x] Keep the remediation endpoint loopback and clearly non-running until the
  operator supplies an authorized receiver.
- [x] Make dry-run network-free and print only a canonical, sanitized target
  origin plus the exact plan.
- [x] Require exact `--confirm-target` for apply.
- [x] Require `--allow-remote-target` for nonloopback apply.
- [x] Check all exact seed URNs for collisions before any mutation; permit a
  deliberate rerun only with `--allow-existing-demo-assets`.
- [x] Bound structured-property CLI and SDK operations and return secret-safe
  stage errors.
- [x] Gate the real MCP lineage/`save_document` test behind
  `RUN_LIVE_DATAHUB_TESTS=1` and a bounded propagation poll.

Compatibility floor: with `mcp-server-datahub==0.6.0`, `save_document` needs
DataHub OSS 1.4.0 or newer, or DataHub Cloud 0.3.16 or newer, with mutation
tools and document-write permission enabled.

## Task 8 — Submission artifacts and verification

**Files:** README, live setup, pitch, video script, RFC, feedback draft,
examples, and submission checklist.

- [x] Explain category fit as deterministic agency without a generative claim.
- [x] Document the five receipt states, allowlist, deadlines, bootstrap gates,
  and operator responsibility boundaries.
- [x] Keep fixture, in-process MCP protocol, and live proof claims distinct.
- [x] Provide captured fixture examples, setup commands, Apache-2.0 license,
  RFC proposal, feedback draft, and under-three-minute video script.
- [x] Add a checklist that leaves merge/push/public access/license detection,
  project URL, video publication, and Devpost submission as human actions.

Final technical verification commands:

```powershell
python -m pytest -q
python -m compileall -q src scripts
python src\demo_dashboard.py
$env:DATAHUB_GMS_URL = "http://localhost:8080"
python scripts\bootstrap_datahub_demo.py --dry-run
git diff --check main...HEAD
```

The live gate must remain reported as pending unless it runs without a skip
against a named instance and the saved document is independently verified.

## Human submission boundary

The following are intentionally not marked complete by implementation work:

- [ ] merge/integrate and push the reviewed branch;
- [ ] make the repository public;
- [ ] verify Apache-2.0 detection and all links while signed out;
- [ ] publish an accessible project URL;
- [ ] record and publish the under-three-minute video;
- [ ] complete and submit the Devpost entry.

Use `docs/submission-checklist.md` for the full handoff.
