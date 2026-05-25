use chrono::{DateTime, Utc};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WorkflowDefinition {
    pub workflow_id: String,
    pub queue: String,
    pub latest_revision_id: String,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WorkflowRevision {
    pub workflow_id: String,
    pub revision_id: String,
    pub source: String,
    pub graph_modules_csv: String,
    pub graph_function_steps_csv: String,
    pub graph_function_inputs_json: String,
    pub reflected_at_utc: DateTime<Utc>,
    pub executable_count: usize,
    pub reflection_receipt_json: String,
}
