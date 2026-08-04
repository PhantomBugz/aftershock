# RFC: Action-Provenance Ledger

**Status: Project Proposal — Not Submitted or Accepted Upstream**

## Summary

This project-level RFC proposes a DataHub-compatible Action-Provenance Ledger:
an append-oriented record linking governed data assets and bounded consumption
windows to stable IDs for actions emitted by automated systems. Its purpose is
to let an incident-response agent select compensating controls more precisely
than system-level lineage permits.

This document records future design work for Aftershock. It is not evidence of
an upstream patch, review, endorsement, or contribution.

## Motivation

DataHub lineage can identify a downstream job or model exposed to an upstream
dataset. That is the correct foundation for system-level impact discovery, but
an edge alone does not answer:

- which run consumed data during the incident window;
- which externally visible actions that run emitted;
- which stable action IDs a compensating service should receive; or
- whether a prior compensation attempt already settled.

Aftershock's current MVP therefore invokes governed controls at the
exposed-system level and stores normalized, secret-safe receipts. It does not
persist arbitrary response bodies. The proposed ledger adds the missing
provenance needed for selective action handling.

## Goals

- Assign a stable, producer-supplied ID to each material automated action.
- Associate that action with source asset URNs and a bounded data-consumption
  window or watermark.
- Represent the producing job/model/deployment and its run identity.
- Support time-window and asset-based lookup during an incident.
- Record compensation attempts and outcomes without rewriting history.
- Make retry and uncertainty semantics explicit.

## Non-goals

- Inferring record-level causality from lineage alone.
- Storing payment details, customer payloads, or endpoint credentials.
- Promising exactly-once behavior across independent services.
- Replacing workflow-engine, payment, or order-system audit logs.
- Automatically authorizing a high-impact control.

## Proposed record

An `ActionExecution` record would contain:

| Field | Purpose |
| --- | --- |
| `action_id` | Stable ID assigned by the producing system |
| `action_type` | Governed action name, such as `ISSUE_PO` |
| `producer_urn` | DataJob, MLModel, deployment, or agent asset URN |
| `producer_run_id` | Workflow/model/agent execution identifier |
| `source_asset_urns` | DataHub assets consulted by that run |
| `consumption_start` / `consumption_end` | UTC interval or watermark bounds |
| `occurred_at` | Producer-observed action timestamp |
| `target_reference` | Opaque reference usable by the owning system |
| `idempotency_key` | Stable retry key, not exactly-once evidence |
| `status` | Observed producer state |
| `retention_class` | Policy controlling expiry and redaction |

The opaque target reference should be the minimum identifier required by the
owning service. Sensitive business payloads remain in that service's audit
store.

## Proposed DataHub relationships and tools

The following labels are conceptual additions, not claims about relationships
that DataHub currently ships:

```text
Dataset/Feature --CONSUMED_BY_ACTION--> ActionExecution
DataJob/MLModel/Agent --EMITTED_ACTION--> ActionExecution
Incident Document --AFFECTS_ACTION--> ActionExecution
CompensationAttempt --COMPENSATES--> ActionExecution
```

A first implementation could use governed Documents plus related asset URNs
while the metadata model is evaluated. A mature implementation could define a
dedicated entity/aspect and expose narrowly scoped MCP tools:

- `record_action_provenance` — append one producer-authenticated action record;
- `query_actions_by_asset_window` — return IDs matching source URNs and an
  incident interval, including confidence/coverage metadata;
- `record_compensation_result` — append one attempt and its external receipt;
- `get_action_provenance_coverage` — report producers or time ranges that did
  not emit sufficient evidence.

Tools should use typed inputs, return stable schemas, enforce authorization,
and surface partial coverage rather than silently treating missing evidence as
an empty match.

## Incident-window semantics

An incident window should be expressed as inclusive/exclusive UTC bounds plus
the clock or watermark source. The query response must distinguish:

- a confirmed match;
- a possible match caused by coarse watermarks or clock uncertainty;
- a confirmed non-match; and
- unknown coverage because a producer did not report.

Selection policy belongs in the incident playbook. High-impact controls may
require a human approval step for possible matches.

## Idempotency and failure semantics

Every compensation attempt receives a deterministic key derived from the
incident ID, action ID, and control version. The owning service decides how to
enforce that key. The ledger records attempts as append-only state transitions:

```text
requested -> accepted -> succeeded
                    \-> failed
                    \-> outcome_unknown
```

Timeouts are `outcome_unknown`, not failures safe to repeat blindly. A retry
reuses the same key. Conflicting terminal receipts are retained and escalated;
they are not collapsed into a synthetic success. Ledger persistence failure
does not relabel an already observed external control result.

The current Aftershock v1 receiver contract supplies a concrete baseline for
the ledger. A terminal success requires an eligible non-202 response containing
`receipt_version: 1`, `status: succeeded`, and a valid external `receipt_id`.
`accepted` and `pending` are nonterminal on ordinary success-class responses.
A 4xx other than 408 is a failed rejection. HTTP 408 and 5xx are
`outcome_unknown` unless they carry a valid v1 terminal success/failure receipt,
which is honored. Transport failure after dispatch, deadline expiry after
dispatch, or a missing terminal contract is also `outcome_unknown`. Work denied
by policy or not dispatched before the deadline is `skipped`. The ledger must
preserve all five states rather than converting transport signals into inferred
business outcomes.

Endpoint authorization remains separate from action provenance. The current
engine requires exact URL allowlisting, rejects user information/fragments,
requires HTTPS away from loopback, and disables redirects. A future ledger
should reference a governed playbook identity, not copy credentials or broaden
network authorization. DataHub RBAC and infrastructure egress enforcement
remain operator-controlled layers.

## Privacy, security, and retention

- Store opaque IDs and metadata, not customer or financial payloads.
- Apply ownership, domain, and policy metadata to the ledger entity/aspect.
- Separate read, append, and compensation permissions.
- Encrypt transport and backing stores; never store control credentials in
  structured properties, action records, or endpoint URLs.
- Define retention by action class and jurisdiction, with deletion/redaction
  events that preserve aggregate coverage evidence.
- Audit every query and mutation made by a remediation agent.

## Staged rollout

1. **Shadow capture:** instrument one noncritical producer; compare ledger
   records with its authoritative audit log.
2. **Read-only incident lookup:** show candidate action IDs and coverage without
   invoking controls.
3. **Approval-gated controls:** require an owner to approve selected IDs.
4. **Policy-gated automation:** automate only well-covered, low-risk action
   types with idempotent downstream APIs.
5. **Broader metadata proposal:** use measured results to decide whether a
   dedicated DataHub entity/aspect and MCP tools merit upstream discussion.

## Evaluation

Measure provenance coverage, lookup precision/recall against authoritative
logs, time to identify affected actions, duplicate-attempt rate, unknown
outcomes, write latency, storage cost, and operator override frequency. A
rollout advances only when coverage and downstream idempotency meet the policy
for that action class.

## Open questions

- Should action records be first-class entities, time-partitioned aspects, or
  external records referenced by DataHub Documents?
- How should field-level lineage and data-quality windows refine matching?
- Which ownership policy may approve an automated compensation?
- How should retention and deletion propagate across related incident records?
