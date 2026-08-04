"""Shared constants for Aftershock's seeded live DataHub demo."""

from __future__ import annotations


DEMO_DATASET_URN = (
    "urn:li:dataset:(urn:li:dataPlatform:postgres,"
    "aftershock_demo.inventory_pricing,DEV)"
)
DEMO_JOB_URN = (
    "urn:li:dataJob:(urn:li:dataFlow:(airflow,aftershock_demo,DEV),"
    "purchase_order_generator)"
)
DEMO_INCIDENT_ID = "INC-LIVE-001"
DEMO_PURCHASE_ORDER_ID = "PO-AFTERSHOCK-001"
DEMO_ACTION = "REVERT_STATE"
DEMO_BUSINESS_ACTION = "ISSUE_PO"
DEMO_RECEIVER_HOST = "127.0.0.1"
DEMO_RECEIVER_PORT = 8765
DEMO_REMEDIATION_PATH = "/remediate/cancel_po"
DEMO_REMEDIATION_ENDPOINT = (
    f"http://{DEMO_RECEIVER_HOST}:{DEMO_RECEIVER_PORT}"
    f"{DEMO_REMEDIATION_PATH}"
)
