# Aftershock live demo video script (target: 2:48)

## 0:00–0:16 — The problem: Action Debt

**On screen:** Aftershock title, then the seeded pricing dataset in DataHub.

**Voiceover:**

“Bad pricing has already issued a purchase order. Finding the bad data is only
half the incident. Aftershock uses DataHub to find the exposed job, cancel that
order, stop further issuance, and preserve the receipt. We call that unresolved
operational follow-up Action Debt.”

## 0:16–0:32 — The agent loop

**On screen:** README loop: OBSERVE → DECIDE → ACT → PERSIST.

**Voiceover:**

“Aftershock is a deterministic, auditable action agent. It observes DataHub
lineage and metadata, decides from exact policy, acts through a bounded control,
and persists the receipt back to DataHub for the next operator or agent.”

## 0:32–0:50 — DataHub is foundational

**On screen:** local DataHub UI showing the `aftershock_demo.inventory_pricing`
dataset and downstream purchase-order DataJob. Then show the two governed
structured-property names in the clean terminal or README. Keep all credentials
and unrelated browser chrome out of frame.

**Voiceover:**

“The official DataHub MCP server supplies the downstream lineage and the
governed business action and webhook properties. The grant binds this exact job,
entity type, ISSUE_PO action, and loopback endpoint—an endpoint alone is not
authority.”

## 0:50–1:02 — Establish observable state

**On screen:** the receiver is already running in one terminal. In the clean
demo terminal, run:

```powershell
python src\live_demo.py
```

Pause on `LIVE DATAHUB MCP MODE` and `RECEIVER BEFORE`, showing
`PO-AFTERSHOCK-001`, `purchase_order_status=issued`,
`issue_po_enabled=True`, and `apply_count=0`.

**Voiceover:**

“This is live MCP mode, not the fixture demo. Purchase order
PO-AFTERSHOCK-001 is already issued, further issuance is enabled, and no
control has been applied.”

## 1:02–1:43 — Observe, decide, act, persist

**On screen:** let the ordered OBSERVE, DECIDE, ACT, and PERSIST milestones
render. Pause briefly on the lineage tree and control receipt table.

**Voiceover:**

“Observe reads the real seeded lineage and entity metadata. Decide resolves one
metadata-backed target. Act requires the exact four-field grant and sends the
control with a stable idempotency key. Success is not inferred from HTTP—it
requires a terminal v1 receipt with a receiver-generated receipt ID. Persist
then creates the incident document through MCP save_document.”

## 1:43–2:08 — Prove the business action

**On screen:** show the terminal `succeeded` receipt and PO-bound external
receipt ID, then `RECEIVER AFTER`: the same order is `canceled`,
`issue_po_enabled=False`, `apply_count=1`, and the same receipt ID.

**Voiceover:**

“The receiver returned terminal success and changed the observable business
state exactly once: the already-issued order is canceled and further issuance
is disabled. The matching PO-bound receipt ties that action to the DataHub
incident record.”

## 2:08–2:25 — Prove durable DataHub memory

**On screen:** show `LIVE DATAHUB READBACK VERIFIED`, then the saved incident
document in the local DataHub UI.

**Voiceover:**

“Aftershock does not trust the write response alone. Search confirms the exact
generated document and title, grep confirms the incident and receipt IDs in its
content, and both related assets link back to it. The evidence is now durable
DataHub context.”

## 2:25–2:39 — Technical quality

**On screen:** show captured terminal results from the final verification:
`309 passed, 1 skipped` for the offline suite and `1 passed in 27.59s` for the
separately enabled live MCP test. Briefly show the public repository and
Apache-2.0 license badge after the final code has been pushed.

**Voiceover:**

“The offline suite passes 309 tests, and the separately enabled live MCP
contract also passes. The public Apache-2.0 repository includes the receiver,
bootstrap, setup guide, strict contracts, and reproducible fixture path.”

## 2:39–2:48 — Close

**On screen:** Aftershock title and the four-stage loop.

**Voiceover:**

“Aftershock turns DataHub context into governed operational recovery—closing
Action Debt with evidence, not assumptions.”

## Recording setup

Before recording:

1. Start and seed the local DataHub OSS v1.6.0 instance.
2. Start `python src\demo_remediation_receiver.py` in a separate terminal.
3. In the clean demo terminal set `AFTERSHOCK_DATAHUB_MODE=mcp` and
   `DATAHUB_GMS_URL=http://127.0.0.1:8080`; remove token variables when the local
   instance does not require them.
4. Run `python src\live_demo.py` once off-camera to confirm the environment,
   then reset and record a fresh uninterrupted proof sequence. Trim only idle
   waiting time; do not reorder the before/action/after evidence.

## Recording checklist

- Keep the final upload under three minutes.
- Show `LIVE DATAHUB MCP MODE`, not fixture output, as the primary demonstration.
- Keep the receiver BEFORE and AFTER order states, PO-bound external receipt ID,
  saved document URN, and `LIVE DATAHUB READBACK VERIFIED` visible long enough
  to read.
- Show the local DataHub UI accurately; do not imply that a hosted DataHub
  deployment is included.
- Stop or edit the recording if any token, credential, private URL, notification,
  or unrelated personal information appears.
- Use no copyrighted music or third-party footage.
- Upload the finished video and make it publicly visible on YouTube, Vimeo, or
  Youku, then verify it while signed out.
