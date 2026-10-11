use chrono::{DateTime, Utc};
use serde::de::DeserializeOwned;
use serde::Serialize;

use crate::domain::errors::{Result, StasisError};

pub fn sql_err(prefix: &str, err: impl std::fmt::Display) -> StasisError {
    StasisError::PortFailure(format!("{prefix}: {err}"))
}

pub fn encode_time(value: DateTime<Utc>) -> i64 {
    value.timestamp_nanos_opt().unwrap_or_else(|| {
        value
            .timestamp()
            .saturating_mul(1_000_000_000)
            .saturating_add(i64::from(value.timestamp_subsec_nanos()))
    })
}

pub fn decode_time(nanos: i64) -> Result<DateTime<Utc>> {
    let secs = nanos.div_euclid(1_000_000_000);
    let nsec = nanos.rem_euclid(1_000_000_000) as u32;
    DateTime::<Utc>::from_timestamp(secs, nsec)
        .ok_or_else(|| sql_err("decode timestamp", format!("out of range: {nanos}")))
}

pub fn encode_json<T: Serialize>(value: &T) -> Result<String> {
    serde_json::to_string(value).map_err(|err| sql_err("encode json", err))
}

pub fn decode_json<T: DeserializeOwned>(raw: &str) -> Result<T> {
    serde_json::from_str(raw).map_err(|err| sql_err("decode json", err))
}

pub fn encode_string_list(values: &[String]) -> Result<String> {
    encode_json(&values)
}

pub fn decode_string_list(raw: &str) -> Result<Vec<String>> {
    decode_json(raw)
}
