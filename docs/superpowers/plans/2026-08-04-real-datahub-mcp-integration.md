# Real DataHub MCP Integration Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make Aftershock perform genuine DataHub MCP lineage reads and `save_document` write-back, with structured remediation receipts and explicitly labeled offline fixtures.

**Architecture:** A `DataHubContextPort` separates live MCP transport from fixtures. `BlastRadiusMapper` normalizes lineage plus structured properties, `CompensatingActionEngine` returns immutable receipts, and `AftershockIncidentProcessor` coordinates discovery, remediation, and one incident-document write-back. The FastAPI listener and Rich dashboard consume the same processor.

**Tech Stack:** Python 3.12, FastMCP 3.4.5 (the official DataHub server's MCP stack), official `mcp-server-datahub` 0.6.0, DataHub 1.6, FastAPI, httpx, Rich, pytest

---

## File Structure

- Create `src/datahub_context.py`: MCP session transport, tool-result parsing, live/fixture adapters, and environment factory.
- Create `src/remediation_models.py`: actionable-target, remediation-receipt, write-back, and incident-report dataclasses.
- Create `src/incident_processor.py`: end-to-end incident orchestration and document rendering.
- Replace `src/blast_radius_mapper.py`: normalize MCP lineage and DataHub structured properties.
- Modify `src/compensating_action_engine.py`: return structured receipts.
- Modify `src/lineage_listener.py`: use the processor and return honest structured status.
- Modify `src/demo_dashboard.py`: explicit fixture mode and receipt-driven output.
- Create `scripts/bootstrap_datahub_demo.py`: seed demo assets and structured-property values after definitions exist.
- Create `config/aftershock_structured_properties.yaml`: DataHub CLI property definitions.
- Create `tests/mcp_test_server.py`: in-process official MCP server fixture.
- Add focused tests for each component plus an opt-in live integration test.
- Update `README.md`, `docs/devpost-pitch.md`, `docs/video-script.md`; add live setup, RFC, feedback draft, and `examples/` artifacts.

### Task 1: Official MCP client and explicit context modes

**Files:**
- Modify: `pyproject.toml`
- Modify: `requirements.txt`
- Create: `src/datahub_context.py`
- Create: `tests/mcp_test_server.py`
- Create: `tests/test_datahub_context.py`

- [ ] **Step 1: Add the failing MCP protocol test**

Create an in-process `fastmcp.FastMCP` server with tools named `get_lineage`, `get_entities`, and `save_document`. Connect with `fastmcp.Client(FastMCPTransport(server, raise_exceptions=True))` so the same stack used by the official DataHub server performs MCP initialization and `tools/call` exchanges. Assert that `MCPDataHubContext.get_lineage()` calls:

```python
{
    "urn": DATASET_URN,
    "upstream": False,
    "max_hops": 3,
    "max_results": 100,
}
```

Assert that `save_document()` calls `document_type="Summary"`, passes the deterministic document URN, and includes all `related_assets`.

- [ ] **Step 2: Run the focused test and verify RED**

Run: `python -m pytest tests/test_datahub_context.py -q`

Expected: collection fails because `datahub_context` does not exist.

- [ ] **Step 3: Add the official dependencies**

Add both dependency ranges to `pyproject.toml` and `requirements.txt`:

```text
mcp-server-datahub==0.6.0
fastmcp==3.4.5
```

Install with `python -m pip install -r requirements.txt`.

- [ ] **Step 4: Implement the minimum MCP adapter**

Implement a `DataHubContextPort` protocol and `MCPDataHubContext`. Its injected `client_factory` returns a `fastmcp.Client`. `_call_tool()` must reject `is_error`, prefer `data`, then `structured_content`, and JSON-decode a single text content block as a fallback.

Production session factories must support:

```python
StdioTransport(
    command=sys.executable,
    args=["-m", "mcp_server_datahub"],
    env={
        **safe_environment,
        "TOOLS_IS_MUTATION_ENABLED": "true",
    },
)
```

and `StreamableHttpTransport(url=DATAHUB_MCP_URL, headers=authenticated_headers)`. The remote MCP client uses only `DATAHUB_MCP_TOKEN`; it must never forward `DATAHUB_GMS_TOKEN` to an arbitrary MCP URL.

Add `FixtureDataHubContext` with `mode="fixture"`; add `build_datahub_context_from_env()` accepting only `fixture` and `mcp`. Never catch a live MCP error and return the fixture.

- [ ] **Step 5: Run the focused test and verify GREEN**

Run: `python -m pytest tests/test_datahub_context.py -q`

Expected: MCP protocol and mode tests pass.

- [ ] **Step 6: Commit**

```powershell
git add pyproject.toml requirements.txt src/datahub_context.py tests/mcp_test_server.py tests/test_datahub_context.py
git commit -m "feat: add genuine DataHub MCP context adapter"
```

### Task 2: MCP blast-radius mapping and structured playbook properties

**Files:**
- Create: `src/remediation_models.py`
- Replace: `src/blast_radius_mapper.py`
- Replace: `tests/test_blast_radius_mapper.py`
- Modify: `mock-data/datahub_lineage.json`

- [ ] **Step 1: Write failing mapper tests**

Make the MCP test server return `downstreams.searchResults[].entity` from `get_lineage` and detailed entities from `get_entities`. Use this exact structured-property shape:

```python
"structuredProperties": {
    "properties": [
        {
            "structuredProperty": {
                "urn": "urn:li:structuredProperty:aftershock.businessAction",
                "definition": {"qualifiedName": "aftershock.businessAction"},
            },
            "values": [{"stringValue": "ISSUE_PO"}],
        },
        {
            "structuredProperty": {
                "urn": "urn:li:structuredProperty:aftershock.remediationWebhook",
                "definition": {"qualifiedName": "aftershock.remediationWebhook"},
            },
            "values": [
                {"stringValue": "https://api.internal.example/remediate/cancel_po"}
            ],
        },
    ]
}
```

Assert `ActionableTarget` values and verify an entity missing a playbook remains present with missing fields so the engine can issue a skipped receipt.

- [ ] **Step 2: Run the mapper test and verify RED**

Run: `python -m pytest tests/test_blast_radius_mapper.py -q`

Expected: imports or assertions fail because the typed MCP mapper is absent.

- [ ] **Step 3: Implement models and mapper**

Create frozen dataclasses with `to_dict()` methods:

```python
@dataclass(frozen=True)
class ActionableTarget:
    urn: str
    entity_type: str
    business_action: str | None
    remediation_webhook: str | None
```

`BlastRadiusMapper.get_targets()` calls the context port for downstream lineage, batches all URNs into one `get_entities` call, then extracts properties by `definition.qualifiedName`. Preserve lineage order and never use the removed GraphQL route.

Update the fixture to use the same `structuredProperties` representation returned by MCP.

- [ ] **Step 4: Run focused and regression tests**

Run: `python -m pytest tests/test_blast_radius_mapper.py tests/test_datahub_context.py -q`

Expected: all focused tests pass.

- [ ] **Step 5: Commit**

```powershell
git add src/remediation_models.py src/blast_radius_mapper.py tests/test_blast_radius_mapper.py mock-data/datahub_lineage.json
git commit -m "feat: map DataHub structured playbooks from MCP lineage"
```

### Task 3: Structured compensating-control receipts

**Files:**
- Modify: `src/remediation_models.py`
- Replace: `src/compensating_action_engine.py`
- Replace: `tests/test_compensating_action_engine.py`

- [ ] **Step 1: Write failing receipt tests**

Test one HTTP 200 target, one HTTP 503 target, and one target without a webhook. Assert `RemediationReceipt.status` equals `succeeded`, `failed`, and `skipped`; assert HTTP status/error fields and the request body’s `business_action`.

- [ ] **Step 2: Run the engine test and verify RED**

Run: `python -m pytest tests/test_compensating_action_engine.py -q`

Expected: old `list[bool]` output does not satisfy receipt assertions.

- [ ] **Step 3: Implement receipt-based execution**

`execute_rollback()` must catch HTTP errors per target and return a receipt without cancelling sibling tasks. `process_blast_radius()` must use `asyncio.gather` and preserve input order. Redact URL user-info/query/fragment in stored receipts.

- [ ] **Step 4: Run the focused test and verify GREEN**

Run: `python -m pytest tests/test_compensating_action_engine.py -q`

Expected: receipt tests pass.

- [ ] **Step 5: Commit**

```powershell
git add src/remediation_models.py src/compensating_action_engine.py tests/test_compensating_action_engine.py
git commit -m "feat: emit structured remediation receipts"
```

### Task 4: Incident processor and real `save_document` write-back

**Files:**
- Create: `src/incident_processor.py`
- Create: `tests/test_incident_processor.py`
- Modify: `src/remediation_models.py`

- [ ] **Step 1: Write failing end-to-end processor tests**

Use the real in-process MCP server and `httpx.MockTransport`. Assert order:

1. `get_lineage`;
2. `get_entities`;
3. remediation HTTP calls;
4. `save_document`.

Assert the document Markdown includes every receipt, the execution mode, and the source dataset. Assert `related_assets` contains the source and all targets. Add a write-back-error test proving successful control receipts remain successful while `writeback.status="failed"`.

- [ ] **Step 2: Run processor tests and verify RED**

Run: `python -m pytest tests/test_incident_processor.py -q`

Expected: `incident_processor` does not exist.

- [ ] **Step 3: Implement the coordinator**

Create `AftershockIncidentProcessor.process(incident_id, dataset_urn) -> IncidentReport`. Derive a safe deterministic URN such as `urn:li:document:aftershock-inc-9942`; render Markdown without claims beyond the receipts; call `save_document` with `document_type="Summary"` and no unseeded topic tags.

- [ ] **Step 4: Run processor and full tests**

Run: `python -m pytest tests/test_incident_processor.py -q`

Expected: all processor tests pass.

- [ ] **Step 5: Commit**

```powershell
git add src/incident_processor.py src/remediation_models.py tests/test_incident_processor.py
git commit -m "feat: persist remediation summaries through DataHub MCP"
```

### Task 5: Listener and truthful Rich demo

**Files:**
- Replace: `src/lineage_listener.py`
- Replace: `src/demo_dashboard.py`
- Replace: `tests/test_lineage_listener.py`
- Replace: `tests/test_demo_dashboard.py`

- [ ] **Step 1: Write failing API and dashboard tests**

The API test must assert `context_mode`, counts by receipt status, and write-back URN/status. The dashboard test must assert `OFFLINE FIXTURE MODE`, `get_lineage(upstream=false)`, individual receipt statuses, and the saved document URN. Assert the output does not contain `$120,000`, `transactions reversed`, or `Enterprise State Restored`.

- [ ] **Step 2: Run UI tests and verify RED**

Run: `python -m pytest tests/test_lineage_listener.py tests/test_demo_dashboard.py -q`

Expected: old Boolean response and marketing text fail assertions.

- [ ] **Step 3: Wire the processor into FastAPI and Rich**

Use FastAPI dependency injection for `AftershockIncidentProcessor`. Keep the request envelope labeled as the Aftershock normalized incident envelope, not a proven native DataHub Action payload. Render the dashboard from returned receipts and only show overall success when all required controls and write-back succeed.

- [ ] **Step 4: Run UI tests and full regression suite**

Run: `python -m pytest -q`

Expected: all tests pass.

- [ ] **Step 5: Commit**

```powershell
git add src/lineage_listener.py src/demo_dashboard.py tests/test_lineage_listener.py tests/test_demo_dashboard.py
git commit -m "feat: expose honest MCP incident workflow"
```

### Task 6: Reproducible DataHub setup and submission artifacts

**Files:**
- Create: `config/aftershock_structured_properties.yaml`
- Create: `scripts/bootstrap_datahub_demo.py`
- Create: `tests/test_bootstrap_datahub_demo.py`
- Create: `tests/test_live_datahub_mcp.py`
- Replace: `README.md`
- Create: `docs/live-datahub-setup.md`
- Create: `docs/RFC-Action-Provenance-Ledger.md`
- Create: `docs/FEEDBACK_SURVEY_DRAFT.md`
- Modify: `docs/devpost-pitch.md`
- Modify: `docs/video-script.md`
- Create: `examples/execution_log.txt`
- Create: `examples/remediation_report.json`

- [ ] **Step 1: Write failing bootstrap contract tests**

Assert the structured-property YAML declares `aftershock.businessAction` and `aftershock.remediationWebhook` as single string properties for DataJob and MLModel entity types. Dataset is deliberately excluded because it is the incident source, not a compensating-control target. Assert bootstrap dry-run output includes all demo URNs and no token values.

- [ ] **Step 2: Run the bootstrap test and verify RED**

Run: `python -m pytest tests/test_bootstrap_datahub_demo.py -q`

Expected: setup artifacts do not exist.

- [ ] **Step 3: Implement setup artifacts**

The script must support `--dry-run` and `--apply`, create demo assets using `acryl-datahub`, attach the two structured properties, and add supported lineage. It must require `DATAHUB_GMS_URL` for `--apply` and never print `DATAHUB_GMS_TOKEN`. Bound the definition CLI subprocess with a hard timeout, configure the SDK's request timeout and retry limit directly, and convert every third-party apply-stage exception into fixed secret-safe output without catching process-control exceptions.

Add an integration test marked `live_datahub` and skipped unless `RUN_LIVE_DATAHUB_TESTS=1`; it must execute real MCP lineage and `save_document` and assert a returned document URN. Its index-propagation poll must use a bounded monotonic deadline and wait for the seeded DataJob plus both structured properties without any fixture fallback.

- [ ] **Step 4: Write rule-accurate documentation**

README must distinguish verified offline MCP protocol tests from an optional live DataHub run. Keep Apache-2.0 information, category, architecture, exact setup commands, limitations, and AI-assistance disclosure. The RFC must say “proposal,” the feedback prize must not be called guaranteed, and the pitch must avoid “native webhook” unless the adapter is implemented.

- [ ] **Step 5: Capture real examples**

Run:

```powershell
python src\demo_dashboard.py
python -m pytest -q
```

Copy the actual verified fixture-mode output and serialized report into `examples/`; label both as deterministic offline examples.

- [ ] **Step 6: Commit**

```powershell
git add config scripts tests README.md docs examples
git commit -m "docs: add reproducible DataHub MCP submission package"
```

### Task 7: Verification and review

**Files:**
- Review every changed file

- [ ] **Step 1: Run complete automated verification**

```powershell
python -m pytest -q
python -m compileall -q src scripts
git diff --check main...HEAD
```

- [ ] **Step 2: Run both demos**

Run the fixture dashboard and bootstrap dry run. Verify every displayed claim is backed by receipts or is explicitly labeled simulated/offline.

- [ ] **Step 3: Independent spec and quality review**

Dispatch independent reviewers against the approved design and this plan. Fix every Critical or Important finding and rerun the full verification commands.

- [ ] **Step 4: Record exact live boundary**

If Docker remains unavailable, report that the real MCP protocol is verified in process but a live DataHub persistence run remains pending. Do not claim live DataHub completion without a fresh successful live test.
