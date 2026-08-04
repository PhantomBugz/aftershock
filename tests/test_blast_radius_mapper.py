import asyncio
from pathlib import Path
from typing import Any

import pytest

from blast_radius_mapper import BlastRadiusMapper
from datahub_context import (
    DataHubConfigurationError,
    FixtureDataHubContext,
    MCPDataHubContext,
)
from remediation_models import ActionableTarget
from mcp_test_server import (
    DATASET_URN,
    DATA_JOB_URN,
    MODEL_URN,
    MCPCallRecorder,
    make_client_factory,
)


FIXTURE_PATH = (
    Path(__file__).resolve().parents[1] / "mock-data" / "datahub_lineage.json"
)
UNCONFIGURED_URN = (
    "urn:li:dataJob:(urn:li:dataFlow:(airflow,aftershock_demo,PROD),audit_sink)"
)


def _property(qualified_name: str, *values: Any) -> dict[str, Any]:
    return {
        "structuredProperty": {
            "urn": f"urn:li:structuredProperty:{qualified_name}",
            "definition": {"qualifiedName": qualified_name},
        },
        "values": list(values),
    }


def _complete_entity(
    urn: str,
    entity_type: str,
    action: str,
    webhook: str,
) -> dict[str, Any]:
    return {
        "urn": urn,
        "type": entity_type,
        "customProperties": {
            "business_action": "LEGACY_VALUE_MUST_NOT_BE_USED",
            "remediation_webhook": "https://legacy.invalid/ignored",
        },
        "structuredProperties": {
            "properties": [
                _property(
                    "aftershock.businessAction",
                    {"stringValue": action},
                ),
                _property(
                    "aftershock.remediationWebhook",
                    {"stringValue": webhook},
                ),
            ]
        },
    }


def _single_lineage_page(
    urn: str = DATA_JOB_URN,
    entity_type: str = "DATA_JOB",
) -> dict[int, dict[str, Any]]:
    return {
        0: {
            "downstreams": {
                "searchResults": [
                    {"entity": {"urn": urn, "type": entity_type}}
                ],
                "total": 1,
                "offset": 0,
                "returned": 1,
                "hasMore": False,
            }
        }
    }


def test_maps_batched_mcp_entities_by_urn_and_preserves_lineage_order() -> None:
    recorder = MCPCallRecorder(
        lineage_pages={
            0: {
                "downstreams": {
                    "searchResults": [
                        {"entity": {"urn": MODEL_URN, "type": "ML_MODEL"}},
                        {"entity": {"urn": DATA_JOB_URN, "type": "DATA_JOB"}},
                        {"entity": {"urn": MODEL_URN, "type": "ML_MODEL"}},
                    ],
                    "total": 3,
                    "offset": 0,
                    "returned": 3,
                    "hasMore": False,
                }
            }
        },
        # Deliberately reverse the detail order. The mapper must join by URN.
        entities_payload=[
            _complete_entity(
                DATA_JOB_URN,
                "DATA_JOB",
                "ISSUE_PO",
                "https://api.internal.example/remediate/cancel_po",
            ),
            {
                "urn": MODEL_URN,
                "type": "ML_MODEL",
                "structuredProperties": {
                    "properties": [
                        _property(
                            "aftershock.businessAction",
                            {"stringValue": "ADJUST_PRICE"},
                        ),
                        # Malformed values do not make a target disappear.
                        _property(
                            "aftershock.remediationWebhook",
                            {"stringValue": ""},
                            {"numberValue": 7},
                        ),
                    ]
                },
            },
        ],
    )
    context = MCPDataHubContext(client_factory=make_client_factory(recorder))

    targets = asyncio.run(BlastRadiusMapper(context).get_targets(DATASET_URN))

    assert targets == [
        ActionableTarget(MODEL_URN, "ML_MODEL", "ADJUST_PRICE", None),
        ActionableTarget(
            DATA_JOB_URN,
            "DATA_JOB",
            "ISSUE_PO",
            "https://api.internal.example/remediate/cancel_po",
        ),
        ActionableTarget(MODEL_URN, "ML_MODEL", "ADJUST_PRICE", None),
    ]
    assert recorder.calls == [
        (
            "get_lineage",
            {
                "urn": DATASET_URN,
                "upstream": False,
                "max_hops": 3,
                "max_results": 100,
                "offset": 0,
            },
        ),
        ("get_entities", {"urns": [MODEL_URN, DATA_JOB_URN]}),
    ]
    assert targets[1].to_dict() == {
        "urn": DATA_JOB_URN,
        "entity_type": "DATA_JOB",
        "business_action": "ISSUE_PO",
        "remediation_webhook": "https://api.internal.example/remediate/cancel_po",
    }


def test_preserves_nodes_when_details_are_missing_or_report_an_error() -> None:
    recorder = MCPCallRecorder(
        lineage_pages={
            0: {
                "downstreams": {
                    "searchResults": [
                        {"entity": {"urn": DATA_JOB_URN, "type": "DATA_JOB"}},
                        {
                            "entity": {
                                "urn": UNCONFIGURED_URN,
                                "type": "DATA_PROCESS_INSTANCE",
                            }
                        },
                    ],
                    "total": 2,
                    "offset": 0,
                    "returned": 2,
                    "hasMore": False,
                }
            }
        },
        entities_payload=[
            {
                "urn": DATA_JOB_URN,
                "type": "DATA_FLOW",
                "error": "detail lookup failed",
                "structuredProperties": {
                    "properties": [
                        _property(
                            "aftershock.businessAction",
                            {"stringValue": "MUST_NOT_BE_TRUSTED"},
                        )
                    ]
                },
            }
        ],
    )
    context = MCPDataHubContext(client_factory=make_client_factory(recorder))

    targets = asyncio.run(BlastRadiusMapper(context).get_targets(DATASET_URN))

    assert targets == [
        ActionableTarget(DATA_JOB_URN, "DATA_JOB", None, None),
        ActionableTarget(
            UNCONFIGURED_URN,
            "DATA_PROCESS_INSTANCE",
            None,
            None,
        ),
    ]


@pytest.mark.parametrize("error_first", [False, True])
def test_duplicate_valid_and_error_details_fail_closed_regardless_of_order(
    error_first: bool,
) -> None:
    valid = _complete_entity(
        DATA_JOB_URN,
        "DATA_JOB",
        "ISSUE_PO",
        "https://api.internal.example/remediate/cancel_po",
    )
    error = {
        "urn": DATA_JOB_URN,
        "type": "DATA_FLOW",
        "error": "detail lookup failed",
    }
    recorder = MCPCallRecorder(
        lineage_pages=_single_lineage_page(),
        entities_payload=[error, valid] if error_first else [valid, error]
    )
    context = MCPDataHubContext(client_factory=make_client_factory(recorder))

    targets = asyncio.run(BlastRadiusMapper(context).get_targets(DATASET_URN))

    assert targets == [
        ActionableTarget(DATA_JOB_URN, "DATA_JOB", None, None),
    ]


@pytest.mark.parametrize("reverse_details", [False, True])
def test_conflicting_duplicate_details_fail_closed_regardless_of_order(
    reverse_details: bool,
) -> None:
    first = _complete_entity(
        DATA_JOB_URN,
        "DATA_JOB",
        "ISSUE_PO",
        "https://api.internal.example/remediate/cancel_po",
    )
    second = _complete_entity(
        DATA_JOB_URN,
        "DATA_FLOW",
        "PAUSE_PIPELINE",
        "https://api.internal.example/remediate/pause_pipeline",
    )
    recorder = MCPCallRecorder(
        lineage_pages=_single_lineage_page(),
        entities_payload=[second, first] if reverse_details else [first, second]
    )
    context = MCPDataHubContext(client_factory=make_client_factory(recorder))

    targets = asyncio.run(BlastRadiusMapper(context).get_targets(DATASET_URN))

    assert targets == [
        ActionableTarget(DATA_JOB_URN, "DATA_JOB", None, None),
    ]


def test_empty_lineage_does_not_fetch_entity_details() -> None:
    recorder = MCPCallRecorder(
        lineage_pages={
            0: {
                "downstreams": {
                    "searchResults": [],
                    "total": 0,
                    "offset": 0,
                    "returned": 0,
                    "hasMore": False,
                }
            }
        }
    )
    context = MCPDataHubContext(client_factory=make_client_factory(recorder))

    targets = asyncio.run(BlastRadiusMapper(context).get_targets(DATASET_URN))

    assert targets == []
    assert [name for name, _ in recorder.calls] == ["get_lineage"]


def test_explicit_fixture_mode_uses_the_same_structured_property_contract() -> None:
    context = FixtureDataHubContext(FIXTURE_PATH)

    targets = asyncio.run(BlastRadiusMapper(context).get_targets(DATASET_URN))

    assert [target.to_dict() for target in targets] == [
        {
            "urn": DATA_JOB_URN,
            "entity_type": "DATA_JOB",
            "business_action": "ISSUE_PO",
            "remediation_webhook": "https://api.internal.example/remediate/cancel_po",
        },
        {
            "urn": MODEL_URN,
            "entity_type": "ML_MODEL",
            "business_action": "ADJUST_PRICE",
            "remediation_webhook": "https://api.internal.example/remediate/revert_pricing",
        },
    ]


def test_default_mapper_requires_an_explicit_context_mode(monkeypatch) -> None:
    monkeypatch.delenv("AFTERSHOCK_DATAHUB_MODE", raising=False)

    with pytest.raises(DataHubConfigurationError):
        BlastRadiusMapper()


def test_deprecated_wrapper_is_derived_from_typed_targets() -> None:
    context = FixtureDataHubContext(FIXTURE_PATH)

    wrapped = asyncio.run(
        BlastRadiusMapper(context).get_actionable_targets(DATASET_URN)
    )

    assert wrapped[0] == {
        "entity": {
            "urn": DATA_JOB_URN,
            "type": "DATA_JOB",
            "customProperties": {
                "business_action": "ISSUE_PO",
                "remediation_webhook": "https://api.internal.example/remediate/cancel_po",
            },
        }
    }
