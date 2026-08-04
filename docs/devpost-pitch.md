# Aftershock: Operationalizing Data Lineage to Eradicate "Action Debt"

## The Problem

When data pipelines break silently, current observability tools send an alert to a dashboard. But what about downstream AI agents, marketing engines, and automated purchasing bots actively consuming that corrupted data? Fixing the pipeline does not stop a misinformed system from executing real-world actions. We call this unattended operational fallout **Action Debt**.

## The Solution

Aftershock transforms DataHub from a passive metadata catalog into an active incident-response platform. By receiving critical DataHub incident webhooks, Aftershock identifies operational systems exposed through DataHub lineage and automatically invokes their predefined compensating controls. It maps the blast radius of a data incident to downstream business applications and fires state-reversing API playbooks to isolate or halt affected systems before the operational damage scales.

## What Is Next: The Action-Provenance Ledger

Our MVP proves system-level remediation: identifying exposed downstream systems and triggering their macro-remediation webhooks. The strongest next architectural upgrade is an **Action-Provenance Ledger** linking incident time windows to exact transaction IDs. This would allow Aftershock to pass affected order numbers into remediation payloads for surgical, row-level transaction reversal without taking an entire downstream agent offline.

## Judge Q&A

**Is this just data observability or impact analysis?**

No. Observability tells you data is broken, and impact analysis shows which analytical assets may be affected. Aftershock is the operational recovery layer: it uses DataHub lineage to identify exposed business systems and invokes their registered compensating controls.
