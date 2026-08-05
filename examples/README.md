# Aftershock evidence artifacts

This directory keeps deterministic fixture artifacts separate from the genuine
local DataHub MCP proof. None of these files contains a credential or bearer
token.

## Deterministic offline fixture pair

[`execution_log.txt`](execution_log.txt) and
[`remediation_report.json`](remediation_report.json) were generated together
from one actual `run_demo` execution with:

- an explicit `FixtureDataHubContext`;
- the dashboard's deterministic `httpx.MockTransport` path;
- a fixed timezone-aware UTC clock of `2026-08-04T12:00:00Z`;
- `delay=0`; and
- a plain-text Rich console with a fixed width of 120 columns.

`execution_log.txt` is the exported plain-text console recording with
terminal-only right padding removed and its final newline preserved.
`remediation_report.json` is the sorted, indented serialization of the exact
`IncidentReport` returned by the same execution.

These are **OFFLINE FIXTURE MODE** artifacts. Their terminal `succeeded`
statuses and external receipt IDs come from local `httpx.MockTransport`
responses. Their write-back receipt comes from an in-memory document recorder.
They do not prove that DataHub or an external business system completed an
action. The final release checklist requires comparison or regeneration after
the code freeze so the tracked output stays synchronized with the presentation.

## Genuine local DataHub MCP proof

[`live_demo_proof.txt`](live_demo_proof.txt) is a credential-free transcript of
the dated, verified end-to-end demonstration run on August 4, 2026 against a
local DataHub OSS Quickstart v1.6.0 instance. It records:

- `LIVE DATAHUB MCP MODE`;
- receiver BEFORE state: `PO-AFTERSHOCK-001=issued`,
  `issue_po_enabled=True`, `apply_count=0`;
- ordered OBSERVE/DECIDE/ACT/PERSIST milestones;
- one canonical, PO-bound terminal v1 `succeeded` receipt from the loopback
  receiver;
- a successful MCP `save_document` result with its generated document URN;
- receiver AFTER state: `PO-AFTERSHOCK-001=canceled`,
  `issue_po_enabled=False`, `apply_count=1`; and
- independent `search_documents`, `grep_documents`, and dataset/DataJob
  `relatedDocuments` read-back proof.

This file proves the named local run; it does not claim a hosted DataHub service
or a future rerun. `src/live_demo.py` remains the executable proof gate.

[`replay_hardening_proof.txt`](replay_hardening_proof.txt) records the August 5,
2026 release verification after duplicate-resistant write-back was added. It
captures the current test count, the canonical Document URN returned by two
consecutive real MCP runs, and the unchanged exact-title document count.
