import asyncio
from io import StringIO
from typing import Any

import httpx
import pytest
from rich.console import Console

from demo_contract import (
    DEMO_BUSINESS_ACTION,
    DEMO_DATASET_URN,
    DEMO_INCIDENT_ID,
    DEMO_JOB_URN,
    DEMO_PURCHASE_ORDER_ID,
)
from demo_remediation_receiver import create_app
from live_demo import (
    LiveDemoError,
    _receiver_state,
    run_live_demo,
    wait_for_document_readback,
)


DOCUMENT_URN = "urn:li:document:shared-aftershock-live-proof"
DOCUMENT_TITLE = f"Aftershock incident {DEMO_INCIDENT_ID}"
RECEIPT_ID = "aftershock-demo-succeeded-proof"


def _property(qualified_name: str, value: str) -> dict[str, Any]:
    return {
        "structuredProperty": {
            "urn": f"urn:li:structuredProperty:{qualified_name}",
            "definition": {"qualifiedName": qualified_name},
        },
        "values": [{"stringValue": value}],
    }


def _related_entity(urn: str, *, document_urn: str = DOCUMENT_URN) -> dict[str, Any]:
    return {
        "urn": urn,
        "relatedDocuments": {
            "documents": [
                {
                    "urn": document_urn,
                    "type": "DOCUMENT",
                    "info": {"title": DOCUMENT_TITLE},
                }
            ]
        },
    }


class _ReadbackContext:
    mode = "mcp"

    def __init__(
        self,
        *,
        search_urn: str = DOCUMENT_URN,
        search_title: str = DOCUMENT_TITLE,
        grep_excerpt: str = f"{DEMO_INCIDENT_ID} {RECEIPT_ID}",
        dataset_document_urn: str = DOCUMENT_URN,
        job_document_urn: str = DOCUMENT_URN,
    ) -> None:
        self.search_urn = search_urn
        self.search_title = search_title
        self.grep_excerpt = grep_excerpt
        self.dataset_document_urn = dataset_document_urn
        self.job_document_urn = job_document_urn
        self.calls: list[tuple[str, dict[str, Any]]] = []

    async def search_documents(self, **kwargs: Any) -> dict[str, Any]:
        self.calls.append(("search_documents", kwargs))
        return {
            "searchResults": [
                {
                    "entity": {
                        "urn": self.search_urn,
                        "info": {"title": self.search_title},
                    }
                }
            ],
            "total": 1,
            "count": 1,
        }

    async def grep_documents(self, **kwargs: Any) -> dict[str, Any]:
        self.calls.append(("grep_documents", kwargs))
        return {
            "results": [
                {
                    "urn": DOCUMENT_URN,
                    "title": DOCUMENT_TITLE,
                    "matches": [
                        {"excerpt": self.grep_excerpt, "position": 0}
                    ],
                    "total_matches": 2,
                }
            ],
            "total_matches": 2,
            "documents_with_matches": 1,
        }

    async def get_entities(self, urns: list[str]) -> list[dict[str, Any]]:
        self.calls.append(("get_entities", {"urns": urns}))
        return [
            _related_entity(
                DEMO_DATASET_URN,
                document_urn=self.dataset_document_urn,
            ),
            _related_entity(
                DEMO_JOB_URN,
                document_urn=self.job_document_urn,
            ),
        ]


def _readback(context: Any, *, timeout_seconds: float = 0.05):
    return asyncio.run(
        wait_for_document_readback(
            context,
            document_urn=DOCUMENT_URN,
            expected_title=DOCUMENT_TITLE,
            incident_id=DEMO_INCIDENT_ID,
            receipt_ids=(RECEIPT_ID,),
            related_asset_urns=(DEMO_DATASET_URN, DEMO_JOB_URN),
            timeout_seconds=timeout_seconds,
            poll_interval_seconds=0.001,
        )
    )


def test_document_readback_uses_three_independent_mcp_proofs() -> None:
    context = _ReadbackContext()

    proof = _readback(context)

    assert proof.document_urn == DOCUMENT_URN
    assert proof.title == DOCUMENT_TITLE
    assert proof.markers == (DEMO_INCIDENT_ID, RECEIPT_ID)
    assert proof.related_asset_urns == (DEMO_DATASET_URN, DEMO_JOB_URN)
    assert [name for name, _ in context.calls] == [
        "search_documents",
        "grep_documents",
        "get_entities",
    ]
    assert context.calls[0][1] == {
        "query": DOCUMENT_TITLE,
        "num_results": 50,
        "offset": 0,
    }
    assert context.calls[1][1]["urns"] == [DOCUMENT_URN]
    assert context.calls[1][1]["max_matches_per_doc"] == 5
    assert context.calls[2][1] == {
        "urns": [DEMO_DATASET_URN, DEMO_JOB_URN]
    }


@pytest.mark.parametrize(
    "context",
    [
        _ReadbackContext(search_urn="urn:li:document:different"),
        _ReadbackContext(search_title="Aftershock incident someone-else"),
        _ReadbackContext(grep_excerpt=DEMO_INCIDENT_ID),
        _ReadbackContext(
            dataset_document_urn="urn:li:document:different"
        ),
        _ReadbackContext(job_document_urn="urn:li:document:different"),
    ],
    ids=[
        "wrong-search-urn",
        "wrong-search-title",
        "missing-receipt-marker",
        "missing-dataset-backlink",
        "missing-job-backlink",
    ],
)
def test_document_readback_never_accepts_wrong_or_missing_evidence(
    context: _ReadbackContext,
) -> None:
    with pytest.raises(LiveDemoError, match="timed out"):
        _readback(context, timeout_seconds=0.01)


def test_document_readback_bounds_and_cancels_a_hanging_mcp_call() -> None:
    class HangingContext(_ReadbackContext):
        async def search_documents(self, **kwargs: Any) -> dict[str, Any]:
            self.calls.append(("search_documents", kwargs))
            await asyncio.Event().wait()
            raise AssertionError("unreachable")

    context = HangingContext()

    with pytest.raises(LiveDemoError, match="timed out"):
        _readback(context, timeout_seconds=0.02)

    assert [name for name, _ in context.calls] == ["search_documents"]


class _LiveContext(_ReadbackContext):
    def __init__(self) -> None:
        super().__init__()
        self.saved: dict[str, Any] | None = None

    async def get_lineage(self, dataset_urn: str) -> dict[str, Any]:
        assert dataset_urn == DEMO_DATASET_URN
        return {
            "downstreams": {
                "searchResults": [
                    {"entity": {"urn": DEMO_JOB_URN, "type": "DATA_JOB"}}
                ],
                "total": 1,
                "offset": 0,
                "returned": 1,
                "hasMore": False,
            }
        }

    async def get_entities(self, urns: list[str]) -> list[dict[str, Any]]:
        if urns == [DEMO_JOB_URN]:
            return [
                {
                    "urn": DEMO_JOB_URN,
                    "type": "DATA_JOB",
                    "structuredProperties": {
                        "properties": [
                            _property(
                                "aftershock.businessAction",
                                DEMO_BUSINESS_ACTION,
                            ),
                            _property(
                                "aftershock.remediationWebhook",
                                "http://127.0.0.1:8765/remediate/cancel_po",
                            ),
                        ]
                    },
                }
            ]
        return await super().get_entities(urns)

    async def save_document(self, **kwargs: Any) -> dict[str, Any]:
        self.saved = kwargs
        return {"success": True, "urn": DOCUMENT_URN}

    async def search_documents(self, **kwargs: Any) -> dict[str, Any]:
        assert self.saved is not None
        self.search_title = self.saved["title"]
        return await super().search_documents(**kwargs)

    async def grep_documents(self, **kwargs: Any) -> dict[str, Any]:
        assert self.saved is not None
        self.grep_excerpt = self.saved["content"]
        return await super().grep_documents(**kwargs)


def _console(output: StringIO) -> Console:
    return Console(
        file=output,
        force_terminal=False,
        color_system=None,
        width=160,
    )


def test_live_demo_resets_receiver_runs_mcp_and_proves_after_state() -> None:
    output = StringIO()
    context = _LiveContext()
    transport = httpx.ASGITransport(
        app=create_app(),
        client=("127.0.0.1", 43123),
    )

    async def scenario():
        async with httpx.AsyncClient(
            transport=transport,
            base_url="http://127.0.0.1:8765",
            timeout=10.0,
        ) as client:
            return await run_live_demo(
                console=_console(output),
                context=context,
                http_client=client,
                delay=0,
                readback_timeout_seconds=0.1,
            )

    report = asyncio.run(scenario())
    rendered = output.getvalue()

    assert report.context_mode == "mcp"
    assert report.incident_id == DEMO_INCIDENT_ID
    assert report.dataset_urn == DEMO_DATASET_URN
    assert report.receipts[0].status == "succeeded"
    assert report.writeback.document_urn == DOCUMENT_URN
    assert context.saved is not None
    assert context.saved["related_assets"] == [DEMO_DATASET_URN, DEMO_JOB_URN]
    assert DEMO_PURCHASE_ORDER_ID in context.saved["content"]
    assert "RECEIVER BEFORE" in rendered
    assert DEMO_PURCHASE_ORDER_ID in rendered
    assert "purchase_order_status" in rendered
    assert "issued" in rendered
    assert "issue_po_enabled" in rendered
    assert "True" in rendered
    assert "LIVE DATAHUB MCP MODE" in rendered
    assert "OBSERVE  //" in rendered
    assert "DECIDE  //" in rendered
    assert "ACT  //" in rendered
    assert "PERSIST  //" in rendered
    receipt_id = report.receipts[0].external_receipt_id
    assert receipt_id is not None
    assert DEMO_PURCHASE_ORDER_ID in receipt_id
    assert receipt_id in rendered
    assert "RECEIVER AFTER" in rendered
    assert "canceled" in rendered
    assert "False" in rendered
    assert "LIVE DATAHUB READBACK VERIFIED" in rendered
    assert DOCUMENT_URN in rendered


def test_live_demo_rejects_success_receipt_not_bound_to_purchase_order() -> None:
    context = _LiveContext()
    arbitrary_receipt = "receiver-success-without-business-identity"

    def state(*, issued: bool) -> dict[str, object]:
        return {
            "dataset_urn": DEMO_DATASET_URN,
            "target_urn": DEMO_JOB_URN,
            "business_action": DEMO_BUSINESS_ACTION,
            "purchase_order_id": DEMO_PURCHASE_ORDER_ID,
            "purchase_order_status": "issued" if issued else "canceled",
            "issue_po_enabled": issued,
            "apply_count": 0 if issued else 1,
            "last_incident_id": None if issued else DEMO_INCIDENT_ID,
            "last_receipt_id": None if issued else arbitrary_receipt,
        }

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/demo/reset":
            return httpx.Response(200, json=state(issued=True))
        if request.url.path == "/remediate/cancel_po":
            return httpx.Response(
                200,
                json={
                    "receipt_version": 1,
                    "status": "succeeded",
                    "receipt_id": arbitrary_receipt,
                },
            )
        if request.url.path == "/demo/state":
            return httpx.Response(200, json=state(issued=False))
        raise AssertionError(f"unexpected demo request: {request.url.path}")

    async def scenario() -> None:
        async with httpx.AsyncClient(
            transport=httpx.MockTransport(handler),
            timeout=10.0,
        ) as client:
            with pytest.raises(
                LiveDemoError,
                match="PO-bound terminal proof",
            ):
                await run_live_demo(
                    console=_console(StringIO()),
                    context=context,
                    http_client=client,
                    delay=0,
                    readback_timeout_seconds=0.1,
                )

    asyncio.run(scenario())
    assert context.calls == []


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("purchase_order_id", "PO-WRONG-999"),
        ("purchase_order_status", "pending"),
        ("purchase_order_status", None),
    ],
)
def test_live_demo_rejects_invalid_purchase_order_state(
    field: str, value: object
) -> None:
    payload: dict[str, object] = {
        "dataset_urn": DEMO_DATASET_URN,
        "target_urn": DEMO_JOB_URN,
        "business_action": DEMO_BUSINESS_ACTION,
        "purchase_order_id": DEMO_PURCHASE_ORDER_ID,
        "purchase_order_status": "issued",
        "issue_po_enabled": True,
        "apply_count": 0,
        "last_incident_id": None,
        "last_receipt_id": None,
    }
    payload[field] = value

    with pytest.raises(LiveDemoError, match="invalid state"):
        _receiver_state(payload)


def test_live_demo_refuses_fixture_mode_before_touching_the_receiver() -> None:
    class FixtureContext:
        mode = "fixture"

    calls: list[httpx.Request] = []

    def forbidden(request: httpx.Request) -> httpx.Response:
        calls.append(request)
        return httpx.Response(500)

    async def scenario() -> None:
        async with httpx.AsyncClient(
            transport=httpx.MockTransport(forbidden),
            timeout=10.0,
        ) as client:
            with pytest.raises(
                LiveDemoError,
                match="requires AFTERSHOCK_DATAHUB_MODE=mcp",
            ):
                await run_live_demo(
                    context=FixtureContext(),  # type: ignore[arg-type]
                    http_client=client,
                    delay=0,
                )

    asyncio.run(scenario())
    assert calls == []
