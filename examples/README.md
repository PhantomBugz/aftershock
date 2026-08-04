# Deterministic offline examples

These two artifacts were generated together from one actual `run_demo` execution with:

- an explicit `FixtureDataHubContext`;
- the dashboard's deterministic `httpx.MockTransport` path;
- a fixed timezone-aware UTC clock of `2026-08-04T12:00:00Z`;
- `delay=0`; and
- a plain-text Rich console with a fixed width of 120 columns.

`execution_log.txt` is the console's exported plain-text recording with terminal-only right padding removed from each line and its final newline preserved. `remediation_report.json` is the sorted, indented serialization of the exact `IncidentReport` returned by that same execution.

These are **OFFLINE FIXTURE MODE** examples. The v1 terminal `succeeded` statuses and external receipt IDs are deterministic reports returned by local `httpx.MockTransport` endpoints; they are not proof that a real business system completed an action. The successful write-back receipt comes from the fixture's in-memory document recorder and does not demonstrate persistence to a running DataHub deployment. No external compensating-control endpoint is called.
