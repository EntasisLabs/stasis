# Workflow Builder Starting Object

## Document Metadata

- Document Type: Cookbook Recipe
- Audience: Engineer, Operator
- Stability: Stable
- Last Verified: 2026-06-04
- Verified Against:
  - src/dashboard/handlers.rs
  - src/dashboard/service.rs

## Outcome

Create a deterministic workflow draft or saved revision that seeds initial $current state through the Starting Object control.

## Why this matters

Starting Object lets you seed stable context for each run without hardcoding context set blocks into source templates.

For end-to-end production loop and orchestration examples, see [Production Agentic Workflows](./production-agentic-workflows.md).

## Recipe

### 1. Build a simple graph in the canvas

1. Open Workflows view in dashboard.
2. Drag at least one module tile into canvas.
3. Optionally connect nodes with edges for linear flow.

### 2. Add Starting Object JSON

1. Click Starting Object in the canvas toolbar.
2. Paste a JSON object, for example:

```json
{
  "query": "rust async runtime",
  "attempt": 1,
  "filters": {
    "region": "us-east"
  }
}
```

3. Save or Test Run.

Validation rules:

1. Empty value is allowed.
2. Value must be valid JSON object when provided.
3. Non-object JSON is rejected by the UI parser.

### 3. Understand serialized graph_state

The frontend stores Starting Object under graph_state.initial_state.

Topology-shaped example:

```json
{
  "version": 1,
  "nodes": [
    { "id": "node-fn-core-echo-1" },
    { "id": "node-fn-websearch-search-2" }
  ],
  "edges": [
    { "from": "node-fn-core-echo-1", "to": "node-fn-websearch-search-2" }
  ],
  "initial_state": {
    "query": "rust async runtime",
    "attempt": 1,
    "filters": { "region": "us-east" }
  }
}
```

### 4. Verify compile behavior

When initial_state exists, compiled query includes a set block before pipeline steps.

When initial_state is absent, no set block is emitted.

This keeps seeded state optional and explicit.

## API-first variant

Action endpoints expect graph_state as a JSON string field.

```bash
curl -sS -X POST http://127.0.0.1:3007/action/workflows/run-draft \
  -H 'content-type: application/json' \
  -H 'authorization: Bearer replace-me' \
  -H 'x-stasis-role: scheduler.admin' \
  -d '{
    "workflow_id": "wf-search",
    "queue": "default",
    "graph_state": "{\"version\":1,\"nodes\":[{\"id\":\"node-fn-core-echo-1\"}],\"edges\":[],\"initial_state\":{\"query\":\"rust async runtime\",\"attempt\":1}}"
  }'
```

## Troubleshooting

| Symptom | Cause | Fix |
|---|---|---|
| Starting Object status shows error | Invalid JSON | Provide valid JSON object |
| Save returns graph_state contract error | Missing required nodes/edges or invalid query shape | Ensure graph_state includes required arrays and valid node ids |
| Compiled output has no set block | initial_state missing or invalid | Confirm graph_state.initial_state is an object |
