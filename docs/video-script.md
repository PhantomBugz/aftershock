# Aftershock demo video script (target: 2:40)

## 0:00–0:25 — The problem

**On screen:** project title, then the architecture diagram in the README.

**Voiceover:**

“When a data incident is detected, repairing the table is only half the job.
Downstream jobs and models may already be acting on unreliable data. We call
the operational follow-up Action Debt. Aftershock uses DataHub context to find
exposed systems, invoke their configured controls, and leave the receipts in
DataHub for the next person or agent.”

## 0:25–0:55 — DataHub foundation

**On screen:** briefly highlight `src/datahub_context.py` and the four tool or
property names in the README.

**Voiceover:**

“This is the official DataHub MCP server over FastMCP. Aftershock calls
`get_lineage` with upstream set to false, fetches entity details, and reads the
exact structured properties for business action and remediation endpoint. It
then executes the controls and calls `save_document` with the incident summary.
Live MCP errors fail closed; they never fall back to demo data.”

## 0:55–1:05 — Label the demonstration

**On screen:** run:

```powershell
python src\demo_dashboard.py
```

**Voiceover:**

“For a reproducible recording, this run is explicitly OFFLINE FIXTURE MODE.
The lineage is deterministic, HTTP responses use a mock transport, and the
write-back is an in-memory recorder. I am not presenting this as live DataHub
persistence.”

## 1:05–1:50 — Read, act, and record

**On screen:** let the alert, workflow progress, lineage tree, and receipt table
render. Pause on the receipt table.

**Voiceover:**

“A critical normalized envelope identifies the source dataset. The same
processor used by the API maps downstream targets, executes each control
concurrently, and records structured receipts. Here we can inspect the target,
business action, sanitized endpoint, HTTP status, and any error. No outcome is
inferred: failed and skipped controls stay failed or skipped.”

## 1:50–2:10 — Write-back

**On screen:** pause on the recorded write-back status and document URN.

**Voiceover:**

“Completion also requires the write-back receipt. In live mode, this same step
uses MCP `save_document` to persist a deterministic incident record related to
the source and target assets. That is how later operators and agents inherit
what Aftershock observed and attempted.”

## 2:10–2:27 — Technical evidence

**On screen:** run or show the final line from:

```powershell
python -m pytest -q
```

Then show `tests/test_live_datahub_mcp.py` without claiming it ran live.

**Voiceover:**

“The automated suite includes real in-process MCP and JSON-RPC exchanges, API
authentication, failure isolation, idempotency-key stability, and write-back
contract tests. An opt-in live test is included as the live proof gate; it was
not run here because this environment had no live DataHub deployment.”

## 2:27–2:40 — Honest boundary and vision

**On screen:** `docs/RFC-Action-Provenance-Ledger.md`.

**Voiceover:**

“Today, Aftershock proves system-level exposure and control receipts. It does
not claim record-level causality. The next step is an Action-Provenance Ledger
with stable action IDs and incident windows, enabling more selective recovery.
Aftershock turns DataHub context into an operational loop: read, act, and write
the evidence back.”

## Recording checklist

- Keep the final upload under three minutes.
- Show the **OFFLINE FIXTURE MODE** label before any result.
- Do not describe the in-process MCP tests as a live deployment.
- Use no copyrighted music or third-party footage.
- Stop or edit the recording if any token, credential, or private URL appears.
