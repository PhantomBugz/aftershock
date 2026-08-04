# DataHub developer feedback draft

I used DataHub's context graph and MCP tools to build an incident workflow that
reads downstream lineage, resolves structured properties, invokes external
controls, and writes a receipt document back to the graph. Four improvements
would make this style of stateful agent workflow easier to build safely.

First, I would value a more explicit pagination contract for `get_lineage`,
with one documented response schema for `total`, `offset`, returned count, and
continuation state, plus a reference loop that demonstrates the terminal page.
I added fail-closed checks because an agent should not interpret an incomplete
page as a complete blast radius.

Second, structured-property reads would benefit from a typed helper that
normalizes the nested definition, qualified name, cardinality, and typed value
array. In this project I deliberately match
`aftershock.businessAction` and `aftershock.remediationWebhook` by exact
qualified name; a generated model or official flattening utility would reduce
shape-specific parsing and make cardinality errors easier to report.

Third, mutation and `save_document` enablement could be more discoverable. A
single preflight capability check showing server version, mutation-tool state,
required permissions, and document-write availability would turn several
runtime failure modes into an actionable setup result. The MCP guide could also
show local stdio and remote-server examples side by side and state which process
owns each token and mutation flag.

Finally, I would like a typed remediation-playbook metadata concept rather than
assembling operational configuration from unrelated strings. A useful schema
could include the governed action identifier, endpoint reference, owning team,
environment, credential reference (never the credential), timeout/retry policy,
approval policy, dry-run support, and expected receipt schema. Exposing that
metadata through MCP would help agents validate a playbook before taking action
and leave a consistent result for later operators and agents.
