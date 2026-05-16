/// Events emitted by cognition tools and background agent tasks back to the TUI event loop.
#[derive(Debug, Clone)]
pub enum TuiEvent {
    /// A cognition tool was invoked during the tool loop.
    ToolInvoked {
        tool_name: String,
        input_summary: String,
    },
    /// A job was enqueued into the Stasis runtime.
    JobEnqueued {
        job_id: String,
        job_type: String,
    },
    /// A job was processed (synchronously executed inside a tool invocation).
    JobProcessed {
        job_id: String,
        succeeded: bool,
        execution_id: Option<String>,
    },
    /// The tool loop returned a final agent response.
    AgentResponse {
        text: String,
        tool_names: Vec<String>,
    },
    /// Partial assistant output chunk streamed from the model.
    AgentChunk {
        delta: String,
    },
    /// The tool loop failed with an error.
    AgentError(String),
}
