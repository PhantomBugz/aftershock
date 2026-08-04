# Aftershock Three-Minute Video Script

Run `python src\demo_dashboard.py` immediately before recording.

## 0:00–0:45 — The Hook

“When a pricing pipeline silently shifts a decimal, monitoring tools alert a data engineer. But an automated purchasing system may already be acting on that bad data. Fixing the table does not stop or reverse those actions. That operational fallout is what we call Action Debt.”

## 0:45–1:30 — The Architecture

“Aftershock turns DataHub lineage into an operational incident-response layer. A FastAPI listener receives the critical incident. The blast-radius mapper queries DataHub’s downstream lineage graph to identify exposed operational systems. The compensating-action engine then invokes each system’s registered remediation API concurrently.”

“This MVP is deliberately system-level: it proves corrupted dataset to exposed system to compensating API. It does not overclaim row-level transaction causality.”

## 1:30–2:30 — The Demo

Start the terminal demonstration.

“Here, Aftershock intercepts a critical pricing incident. DataHub lineage identifies an Airflow purchasing job and a SageMaker pricing model. The actual asynchronous engine now sends rollback requests to both deterministic demo endpoints concurrently. Both controls return HTTP 200, and the exposed enterprise state is contained.”

## 2:30–3:00 — The Vision

“Our next step is the Action-Provenance Ledger. It will connect incident time windows to exact transaction IDs, allowing Aftershock to move from system-level containment to surgical reversal of individual orders. We are not only observing broken data—we are building the recovery layer for the automated enterprise.”
