# Agent Coordination

## Document Metadata

- Document Type: Reference Standard
- Audience: Engineer, Architect
- Stability: Evolving
- Last Verified: 2026-05-15
- Verified Against:
  - src/application/orchestration/agent_session_pipeline.rs
  - src/application/orchestration/tool_loop_pipeline.rs
  - src/application/orchestration/agent_session_payload.rs
  - src/application/runtime/agent_session_job_handler.rs
  - src/application/runtime/agent_turn_job_handler.rs
  - tests/runtime_backend_parity.rs

## Purpose

Document the agent coordination layer: `AgentSessionPipeline`, `AgentSessionCoordinator`, selection and termination strategy contracts, the `ToolLoopPipeline` underpinning, and the two job handler entry points (`workflow.stasis.agent_turn` and `workflow.stasis.agent_session`).

## Invariants

1. Every agent turn executes through `ToolLoopPipeline` — tool invocation, round management, and `ToolCallMode` enforcement are applied uniformly.
2. `AgentSessionCoordinator` enforces `max_turns_cap.max(1)` — a session of zero turns is not permitted.
3. An empty `participants` list or empty `initial_user_prompt` returns `FatalFailure` immediately with `guardrail_code: POLICY_VIOLATION`.
4. Turn prompts after the first are constructed from the running transcript: `"Session transcript so far:\n...\n\nContinue the collaboration from your role."` — the full transcript is carried forward.
5. `RoundRobinSelectionStrategy` uses an atomic cursor — it is safe for concurrent sessions but the cursor does not reset between sessions.

---

## Architecture

```
workflow.stasis.agent_session (job)
    └── AgentSessionJobHandler
            └── AgentSessionCoordinator.run_session()
                    ├── AgentSelectionStrategy.select_next_agent()
                    ├── AgentSessionPipeline.execute_turn()
                    │       └── ToolLoopPipeline.execute()
                    │               └── PromptExecutionPipeline + ToolRegistry
                    └── AgentTerminationStrategy.should_terminate()

workflow.stasis.agent_turn (job)
    └── AgentTurnJobHandler
            └── AgentSessionPipeline.execute_turn()
                    └── ToolLoopPipeline.execute()
```

---

## Job Handler: `workflow.stasis.agent_turn`

Executes a single agent turn — one tool-augmented prompt/response cycle for a named agent.

### Payload: `AgentTurnJobPayload`

| Field | Type | Description |
|---|---|---|
| `agent_id` | `String` | Identity of the agent executing this turn |
| `thread_id` | `Option<String>` | Thread to associate the turn with |
| `user_prompt` | `String` | Input prompt for this turn |
| `system_prompt` | `Option<String>` | System prompt for the agent |
| `policy_profile` | `Option<String>` | Policy profile identifier |
| `model_hint` | `Option<String>` | Model preference hint |
| `tool_name` | `String` | Tool the agent should use |
| `tool_input` | `Option<Value>` | Initial tool input |
| `tool_call_mode` | `Option<AgentToolCallMode>` | `auto` or `strict` |
| `memory_policy` | `Option<MemoryPolicyPayload>` | Memory recall/store policy |

---

## Job Handler: `workflow.stasis.agent_session`

Executes a coordinated multi-turn session across one or more participants.

### Payload: `AgentSessionJobPayload`

| Field | Type | Description |
|---|---|---|
| `thread_id` | `Option<String>` | Shared thread for the session |
| `initial_user_prompt` | `String` | Starting prompt for the session — must be non-empty |
| `participants` | `Vec<AgentSessionParticipantPayload>` | Ordered list of participants |
| `policy_profile` | `Option<String>` | Default policy profile |
| `model_hint` | `Option<String>` | Default model hint |
| `max_turns` | `Option<usize>` | Turn cap (defaults to `1` if zero or absent) |
| `tool_call_mode` | `Option<AgentToolCallMode>` | Applied to all turns |
| `memory_policy` | `Option<MemoryPolicyPayload>` | Memory policy for all turns |

### AgentSessionParticipantPayload

| Field | Type | Description |
|---|---|---|
| `agent_id` | `String` | Participant identity |
| `system_prompt` | `Option<String>` | System prompt for this participant |
| `tool_name` | `String` | Tool this participant invokes |
| `tool_input` | `Option<Value>` | Tool input for this participant |

---

## AgentToolCallMode

| Variant | Behavior |
|---|---|
| `auto` (default) | Model decides whether to call tools |
| `strict` | Model is required to call a tool on every round |

---

## Tool Loop

Each agent turn executes through `ToolLoopPipeline`, which manages the prompt → tool call → prompt cycle:

- Up to `DEFAULT_MAX_TOOL_ROUNDS = 4` rounds per turn.
- If `tool_name` is non-empty, the registry is filtered to expose only that tool to the model.
- `ToolInvocation` records are collected per round and returned in the turn response.
- `termination_reason` describes why the loop stopped: `max_rounds_reached`, `no_tool_calls`, `completed`, etc.

### ToolLoopExecutionResponse fields

| Field | Type | Description |
|---|---|---|
| `text` | `String` | Final model response text |
| `tool_name` | `String` | Tool that was active |
| `tool_output` | `Value` | Final tool output |
| `tool_invocations` | `Vec<ToolInvocation>` | All tool calls made during the loop |
| `rounds_executed` | `usize` | Number of prompt/response rounds |
| `termination_reason` | `String` | Why the loop stopped |

---

## Selection Strategies

`AgentSelectionStrategy` determines which participant runs each turn.

```rust
pub trait AgentSelectionStrategy: Send + Sync {
    fn select_next_agent(
        &self,
        participants: &[String],
        thread_id: Option<&str>,
        transcript: &[String],
    ) -> Result<String>;
}
```

### RoundRobinSelectionStrategy

Selects participants in round-robin order using an atomic cursor.

```rust
let strategy = RoundRobinSelectionStrategy::new();
```

The cursor is global to the strategy instance — it does not reset between `run_session` calls. For isolated sessions, create a new strategy instance per session.

### Custom selection

Implement `AgentSelectionStrategy` to select based on transcript content, thread state, or external routing logic:

```rust
pub struct ContentBasedStrategy;

impl AgentSelectionStrategy for ContentBasedStrategy {
    fn select_next_agent(
        &self,
        participants: &[String],
        _thread_id: Option<&str>,
        transcript: &[String],
    ) -> Result<String> {
        // Route based on the last turn's content
        let last = transcript.last().map(|s| s.as_str()).unwrap_or("");
        if last.contains("error") {
            participants.iter().find(|id| id.contains("debug"))
                .cloned()
                .ok_or_else(|| StasisError::PortFailure("no debug agent".into()))
        } else {
            Ok(participants[0].clone())
        }
    }
}
```

---

## Termination Strategies

`AgentTerminationStrategy` decides when a session ends early.

```rust
pub trait AgentTerminationStrategy: Send + Sync {
    fn should_terminate(&self, turn_count: usize, last_response: &str) -> Result<bool>;
}
```

### MaxTurnsTerminationStrategy

Terminates when `turn_count >= max_turns` or when the last response contains an optional `done_token`:

```rust
// Terminate after 5 turns
let strategy = MaxTurnsTerminationStrategy::new(5);

// Terminate after 5 turns OR when response contains "[DONE]"
let strategy = MaxTurnsTerminationStrategy::new(5)
    .with_done_token("[DONE]");
```

### Custom termination

```rust
pub struct PolicyViolationTermination;

impl AgentTerminationStrategy for PolicyViolationTermination {
    fn should_terminate(&self, _turn_count: usize, last_response: &str) -> Result<bool> {
        Ok(last_response.contains("POLICY_VIOLATION"))
    }
}
```

---

## AgentSessionCoordinator

`AgentSessionCoordinator` composes the pipeline with selection and termination strategies for direct programmatic use outside the job runtime:

```rust
let coordinator = AgentSessionCoordinator::new(
    pipeline,
    Arc::new(RoundRobinSelectionStrategy::new()),
    Arc::new(MaxTurnsTerminationStrategy::new(5)),
);

let response = coordinator.run_session(AgentSessionRunRequest {
    thread_id: Some("thread-001".to_string()),
    initial_user_prompt: "Analyze the quarterly report.".to_string(),
    participants: vec![analyst, reviewer],
    context: PromptExecutionContext::default(),
    max_turns_cap: 5,
    policy: AgentTurnExecutionPolicy::default(),
}).await?;
```

### AgentSessionRunResponse

| Field | Type | Description |
|---|---|---|
| `thread_id` | `Option<String>` | Thread ID for the session |
| `turns` | `Vec<AgentTurnRecord>` | Per-turn records |
| `transcript` | `Vec<String>` | Full session transcript as `"actor: text"` lines |
| `terminated` | `bool` | Whether the termination strategy ended the session early |

### AgentTurnRecord

| Field | Type | Description |
|---|---|---|
| `turn_number` | `usize` | 1-based turn index |
| `agent_id` | `String` | Agent that executed this turn |
| `response_text` | `String` | Agent's final response |
| `tool_name` | `String` | Tool used |
| `rounds_executed` | `usize` | Tool loop rounds within this turn |
| `termination_reason` | `String` | How the tool loop ended |

---

## Non-Goals

- `AgentSessionCoordinator` does not manage memory. Memory recall/write is handled by the job handlers (`AgentSessionJobHandler`, `AgentTurnJobHandler`) when a `MemoryContextReader`/`Writer` is wired.
- Strategy implementations do not have access to job metadata. Context must be encoded in the transcript or passed via `thread_id`.
- The `RoundRobinSelectionStrategy` cursor is not persistent — it resets on process restart.
