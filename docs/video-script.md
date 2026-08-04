# Aftershock demo video script (target: 2:45)

## 0:00–0:25 — The problem

**On screen:** project title and README architecture.

**Voiceover:**

“When a data incident is detected, repairing the table is only half the job.
Downstream jobs and models may already be acting on unreliable data. We call
that follow-up Action Debt. Aftershock uses DataHub context to find exposed
systems, apply governed controls, and leave factual receipts for the next
operator or agent.”

## 0:25–0:52 — Why this is an agent and why DataHub matters

**On screen:** highlight `get_lineage`, `get_entities`, the structured-property
names, and `save_document` in the README.

**Voiceover:**

“Aftershock is a deterministic policy-driven agent: observe DataHub context,
decide from governed metadata and an exact endpoint policy, act, then persist
memory back to DataHub. It does not claim language-model reasoning. The
official DataHub MCP server provides lineage and entity context, while
`save_document` records what the workflow observed and attempted.”

## 0:52–1:03 — Label the demonstration

**On screen:** run:

```powershell
python src\demo_dashboard.py
```

**Voiceover:**

“This reproducible run is explicitly OFFLINE FIXTURE MODE. It uses deterministic
lineage, an in-memory exact allowlist, mocked HTTP responses, and an in-memory
document recorder. I am not presenting it as live DataHub persistence.”

## 1:03–1:45 — Read, decide, and act

**On screen:** let the alert, workflow progress, lineage tree, and receipt table
render. Pause on the table columns: Status, HTTP, External receipt ID, Endpoint,
and Error.

**Voiceover:**

“The same processor used by the API maps downstream targets, checks the exact
allowlist, and executes controls with bounded concurrency and a workflow
deadline. Success is not inferred from HTTP. It requires a terminal v1 JSON
receipt with status succeeded and a receiver-generated receipt ID. The five
states are succeeded, accepted, failed, skipped, and outcome unknown.”

## 1:45–2:07 — Explain honest outcome handling

**On screen:** keep the receipt table and external receipt IDs visible.

**Voiceover:**

“Accepted is nonterminal. Most 4xx responses are failed rejections. HTTP 408
and every 5xx are outcome unknown unless they carry a valid terminal success or
failure receipt. Transport failure and deadline expiry after dispatch are also
unknown; undispatched work is skipped. Redirects are disabled, and the stable
idempotency key is not proof of exactly-once execution.”

## 2:07–2:25 — Write-back

**On screen:** pause on write-back status and document URN.

**Voiceover:**

“Overall completion requires every target to have terminal success and the
DataHub write-back to succeed. In live mode this step calls MCP
`save_document`, relating the incident record to the source and downstream
assets so later people and agents inherit the receipts.”

## 2:25–2:37 — Technical evidence

**On screen:** show the final line from a fresh:

```powershell
python -m pytest -q
```

**Voiceover:**

“Tests cover genuine in-process MCP exchanges, pagination, exact endpoint
policy, redirects, deadlines, all receipt states, authentication, and
write-back. The opt-in live test is included but was not run here because this
environment had no live DataHub deployment.”

## 2:37–2:45 — Boundary and vision

**On screen:** `docs/RFC-Action-Provenance-Ledger.md`.

**Voiceover:**

“Today, Aftershock proves system-level exposure and receipts, not record-level
causality. The next step is an Action-Provenance Ledger for more selective
recovery. Aftershock turns DataHub context into a governed operational loop.”

## Recording checklist

- Keep the final upload under three minutes.
- Show **OFFLINE FIXTURE MODE** before any result.
- Display external receipt IDs in the table.
- Do not describe in-process MCP tests as a live deployment.
- Use no copyrighted music or third-party footage.
- Stop or edit the recording if any token, credential, or private URL appears.
