use std::path::PathBuf;

use serde::{Deserialize, Serialize};
use serde_json::Value;

pub const API_VERSION: &str = "stasisd/v1";
pub const MAX_PAYLOAD_BYTES: usize = 64 * 1024;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OnRemovePolicy {
    Drain,
    Orphan,
    Cancel,
}

impl Default for OnRemovePolicy {
    fn default() -> Self {
        Self::Drain
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct StasisdSchedule {
    pub id: String,
    #[serde(default = "default_enabled")]
    pub enabled: bool,
    pub queue: String,
    pub job_type: String,
    pub cron: String,
    #[serde(default = "default_timezone")]
    pub timezone: String,
    #[serde(default)]
    pub jitter_seconds: i64,
    #[serde(default = "default_max_attempts")]
    pub max_attempts: u32,
    #[serde(default)]
    pub on_remove: OnRemovePolicy,
    #[serde(default = "default_payload")]
    pub payload: Value,
}

fn default_enabled() -> bool {
    true
}

fn default_timezone() -> String {
    "UTC".into()
}

fn default_max_attempts() -> u32 {
    3
}

fn default_payload() -> Value {
    Value::Object(Default::default())
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct StasisdFileDocument {
    pub api_version: String,
    #[serde(default)]
    pub schedule: Vec<StasisdSchedule>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct StasisdDocument {
    pub api_version: String,
    pub source_path: PathBuf,
    pub content_hash: String,
    pub schedules: Vec<StasisdSchedule>,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct DesiredState {
    pub sources: Vec<PathBuf>,
    pub documents: Vec<StasisdDocument>,
    pub schedules: Vec<StasisdSchedule>,
    pub diagnostics: Vec<String>,
}

impl DesiredState {
}
