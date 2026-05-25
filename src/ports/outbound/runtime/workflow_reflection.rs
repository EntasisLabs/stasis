use serde::{Deserialize, Serialize};

use crate::domain::errors::Result;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum WorkflowExecutableKind {
    Query,
    Mutation,
    Subscription,
    Iterator,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkflowExecutableReflection {
    pub name: String,
    pub kind: WorkflowExecutableKind,
    pub input_type: Option<String>,
    pub output_type: Option<String>,
    pub loop_directive_count: usize,
    pub recursive_directive_count: usize,
    pub retry_directive_count: usize,
    pub timeout_directive_count: usize,
    pub pipeline_count: usize,
    pub step_count: usize,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkflowSourceReflection {
    pub count: usize,
    pub executables: Vec<WorkflowExecutableReflection>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct WorkflowModuleSearchReflection {
    pub query: String,
    pub count: usize,
    pub matches: Vec<WorkflowModuleSearchMatchReflection>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct WorkflowModuleSearchMatchReflection {
    pub module_id: String,
    pub score: Option<f64>,
    pub summary: String,
    pub matching_ops: Vec<String>,
    pub related_examples: Vec<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkflowModuleOperationReflection {
    pub op: String,
    pub stability: String,
    pub effect: String,
    pub input_schema_ref: Option<String>,
    pub output_schema_ref: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkflowModuleInfoReflection {
    pub module_id: String,
    pub version: String,
    pub entrypoint: String,
    pub required_capabilities: Vec<String>,
    pub total_ops: usize,
    pub exported_ops: Vec<WorkflowModuleOperationReflection>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkflowModuleTypesReflection {
    pub module_id: String,
    pub total_types: usize,
    pub types: Vec<WorkflowModuleOperationReflection>,
}

pub trait WorkflowReflectionPort: Send + Sync {
    fn reflect_executables_from_source(&self, source: &str) -> Result<WorkflowSourceReflection>;
    fn modules_search(&self, query: &str) -> Result<WorkflowModuleSearchReflection>;
    fn module_info(&self, module_id: &str) -> Result<Option<WorkflowModuleInfoReflection>>;
    fn module_types(&self, module_id: &str) -> Result<Option<WorkflowModuleTypesReflection>>;
}
