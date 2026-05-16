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
