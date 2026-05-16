use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::io::{BufRead, Write};
use std::path::PathBuf;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConversationTurn {
    pub role: String,
    pub content: String,
    pub timestamp: DateTime<Utc>,
    pub tool_names: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct TuiDefaults {
    pub backend: Option<String>,
    pub provider: Option<String>,
    pub model: Option<String>,
    pub base_url: Option<String>,
    pub tool_call_mode: Option<String>,
    pub max_tool_rounds: Option<usize>,
    pub thinking_capture: Option<bool>,
    pub thinking_max_lines: Option<usize>,
}

#[derive(Debug, Clone)]
pub struct SessionHistorySummary {
    pub session_id: String,
    pub turns: usize,
    pub last_timestamp: Option<DateTime<Utc>>,
    pub preview: String,
}

fn medousa_data_dir() -> PathBuf {
    dirs::data_local_dir()
        .unwrap_or_else(|| PathBuf::from("."))
        .join("medousa")
}

pub fn history_path(session_id: &str) -> PathBuf {
    medousa_data_dir().join("history").join(format!("{session_id}.jsonl"))
}

fn last_session_path() -> PathBuf {
    medousa_data_dir().join("last_session")
}

fn tui_defaults_path() -> PathBuf {
    medousa_data_dir().join("tui_defaults.json")
}

pub fn load_last_session_id() -> Option<String> {
    std::fs::read_to_string(last_session_path())
        .ok()
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
}

pub fn save_last_session_id(session_id: &str) {
    let path = last_session_path();
    if let Some(parent) = path.parent() {
        let _ = std::fs::create_dir_all(parent);
    }
    let _ = std::fs::write(path, session_id);
}

pub fn load_tui_defaults() -> TuiDefaults {
    let path = tui_defaults_path();
    std::fs::read_to_string(path)
        .ok()
        .and_then(|raw| serde_json::from_str::<TuiDefaults>(&raw).ok())
        .unwrap_or_default()
}

pub fn save_tui_defaults(defaults: &TuiDefaults) {
    let path = tui_defaults_path();
    if let Some(parent) = path.parent() {
        let _ = std::fs::create_dir_all(parent);
    }

    if let Ok(json) = serde_json::to_string_pretty(defaults) {
        let _ = std::fs::write(path, json);
    }
}

pub fn load_history(session_id: &str) -> Vec<ConversationTurn> {
    let path = history_path(session_id);
    let Ok(file) = std::fs::File::open(&path) else {
        return Vec::new();
    };
    std::io::BufReader::new(file)
        .lines()
        .filter_map(|line| line.ok())
        .filter(|line| !line.trim().is_empty())
        .filter_map(|line| serde_json::from_str(&line).ok())
        .collect()
}

pub fn append_turn(session_id: &str, turn: &ConversationTurn) {
    let path = history_path(session_id);
    if let Some(parent) = path.parent() {
        let _ = std::fs::create_dir_all(parent);
    }
    let Ok(mut file) = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&path)
    else {
        return;
    };
    if let Ok(line) = serde_json::to_string(turn) {
        let _ = writeln!(file, "{line}");
    }
}

pub fn list_history_sessions(limit: usize) -> Vec<SessionHistorySummary> {
    let history_dir = medousa_data_dir().join("history");
    let Ok(entries) = std::fs::read_dir(history_dir) else {
        return Vec::new();
    };

    let mut sessions = entries
        .filter_map(|entry| entry.ok())
        .filter_map(|entry| {
            let path = entry.path();
            let ext = path.extension().and_then(|e| e.to_str());
            if ext != Some("jsonl") {
                return None;
            }

            let session_id = path.file_stem()?.to_string_lossy().to_string();
            let metadata = entry.metadata().ok();
            let modified = metadata.and_then(|m| m.modified().ok());
            Some((session_id, modified))
        })
        .collect::<Vec<_>>();

    sessions.sort_by(|a, b| b.1.cmp(&a.1));

    sessions
        .into_iter()
        .take(limit)
        .map(|(session_id, _)| {
            let turns = load_history(&session_id);
            let last_timestamp = turns.last().map(|t| t.timestamp);
            let preview = turns
                .iter()
                .rev()
                .find(|t| !t.content.trim().is_empty())
                .and_then(|t| t.content.lines().next())
                .unwrap_or("(empty session)")
                .chars()
                .take(72)
                .collect::<String>();

            SessionHistorySummary {
                session_id,
                turns: turns.len(),
                last_timestamp,
                preview,
            }
        })
        .collect()
}
