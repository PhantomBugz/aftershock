# Aftershock: From Data Lineage to Operational Recovery

**Challenge category:** Agents That Do Real Work

## Inspiration

Data-quality tools can tell a team that a pricing dataset is unreliable, and
lineage can show which systems are downstream. But an automated job or model
may already be acting on that data. Repairing the dataset does not settle the
operational follow-up. We call that follow-up **Action Debt**.

## What it does

Aftershock is an incident-response agent built on DataHub. Given a critical,
normalized incident envelope, it reads the downstream context graph through
the official DataHub MCP server, resolves governed remediation metadata,
invokes each configured compensating control, returns factual per-target
receipts, and writes an incident summary back with `save_document`. The next
operator or agent can inherit both the affected assets and the observed
outcomes.

## How DataHub is foundational

- `get_lineage(upstream=false)` discovers downstream exposure.
- `get_entities` resolves the exposed assets and their structured properties.
- `aftershock.businessAction` and `aftershock.remediationWebhook` provide the
  governed control mapping.
- `save_document` persists the incident-specific receipt summary and related
  asset URNs back into DataHub.

This is implemented with `mcp-server-datahub==0.6.0` over FastMCP. A failed MCP
call fails closed; live mode never silently switches to fixtures.

## Why it is different

Aftershock composes catalog context with an operational control loop. It does
not stop at displaying impact: it turns DataHub metadata into explicit control
attempts and then contributes the results back to the context available to the
organization. The project extends DataHub rather than rebuilding its lineage or
catalog capabilities.

## Demo and technical execution

The recorded terminal demonstration is explicitly labeled **OFFLINE FIXTURE
MODE**. It uses deterministic local lineage, mocked HTTP transport, and an
in-memory document recorder so every displayed receipt is reproducible. The
test suite separately exercises genuine MCP/JSON-RPC exchanges against an
in-process FastMCP server, including pagination, structured-property mapping,
control failures, authentication, and `save_document` arguments.

A live MCP adapter and opt-in live test are included. Live persistence was not
run in the development environment because no live DataHub deployment was
available; a skipped live test is not presented as proof.

## Event integration boundary

The current FastAPI route accepts an authenticated Aftershock-normalized
envelope. In production, a custom DataHub Action would translate an
`incidentInfo` MetadataChangeLogEvent into that contract. The adapter is a
documented next integration step and is not included in the current MVP.

## Current limit and next step

The MVP proves system-level exposure discovery and compensating-control
receipts. Lineage does not establish which individual records a downstream
system read or which external actions followed. The proposed Action-Provenance
Ledger would add stable action IDs, incident windows, coverage evidence, and
append-only compensation receipts for more selective recovery.

## Built with

Python 3.12, DataHub MCP Server, FastMCP, FastAPI, HTTPX, Rich, and pytest.
