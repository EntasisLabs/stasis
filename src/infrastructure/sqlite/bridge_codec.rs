//! JSON codec for the JavaScript Durable Object SQL bridge.
//!
//! Integer parameters travel as decimal strings (`{"i":"..."}`). Hosts bind
//! them as integers (BigInt or text that SQLite affinity stores as INTEGER).
//! Reads of integer columns use `CAST(col AS TEXT)` so a JavaScript number
//! never rounds a nanosecond timestamp.

use serde_json::{Value, json};

use crate::domain::errors::Result;

use super::codec::sql_err;
use super::executor::{SqlRow, SqlValue};

#[derive(Clone, Debug, PartialEq)]
pub struct ExecResponse {
    pub rows_written: u64,
    pub rows: Vec<SqlRow>,
}

pub fn encode_params(params: &[SqlValue]) -> Result<String> {
    let cells: Vec<Value> = params
        .iter()
        .map(|value| match value {
            SqlValue::Null => Value::Null,
            SqlValue::Text(text) => Value::String(text.clone()),
            SqlValue::Integer(value) => json!({ "i": value.to_string() }),
        })
        .collect();
    serde_json::to_string(&cells).map_err(|err| sql_err("encode sql params", err))
}

pub fn decode_response(raw: &str) -> Result<ExecResponse> {
    let value: Value =
        serde_json::from_str(raw).map_err(|err| sql_err("decode sql response", err))?;
    let rows_written = value
        .get("rowsWritten")
        .and_then(|cell| cell.as_u64())
        .unwrap_or(0);
    let rows_json = value
        .get("rows")
        .and_then(|cell| cell.as_array())
        .ok_or_else(|| sql_err("decode sql response", "missing rows"))?;
    let mut rows = Vec::with_capacity(rows_json.len());
    for row in rows_json {
        let cells = row
            .as_array()
            .ok_or_else(|| sql_err("decode sql response", "row is not an array"))?;
        let mut values = Vec::with_capacity(cells.len());
        for cell in cells {
            values.push(decode_cell(cell)?);
        }
        rows.push(SqlRow { values });
    }
    Ok(ExecResponse { rows_written, rows })
}

fn decode_cell(cell: &Value) -> Result<SqlValue> {
    match cell {
        Value::Null => Ok(SqlValue::Null),
        Value::String(text) => Ok(SqlValue::Text(text.clone())),
        Value::Number(number) => number.as_i64().map(SqlValue::Integer).ok_or_else(|| {
            sql_err(
                "decode sql cell",
                format!("number out of i64 range: {number}"),
            )
        }),
        Value::Object(map) => {
            let raw = map
                .get("i")
                .and_then(|value| value.as_str())
                .ok_or_else(|| sql_err("decode sql cell", "integer object missing i"))?;
            let value = raw
                .parse::<i64>()
                .map_err(|err| sql_err("decode sql cell", err))?;
            Ok(SqlValue::Integer(value))
        }
        other => Err(sql_err(
            "decode sql cell",
            format!("unsupported JSON value {other}"),
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn integers_round_trip_as_decimal_strings() {
        let nanos = 1_763_000_000_000_000_000_i64;
        let encoded = encode_params(&[
            SqlValue::text("job-1"),
            SqlValue::Integer(nanos),
            SqlValue::Null,
        ])
        .expect("encode");
        assert!(encoded.contains("1763000000000000000"));
        assert!(!encoded.contains("1.763e"));

        let decoded = decode_response(
            r#"{"rowsWritten":1,"rows":[["job-1",{"i":"1763000000000000000"},null]]}"#,
        )
        .expect("decode");
        assert_eq!(decoded.rows_written, 1);
        assert_eq!(decoded.rows[0].text(0).unwrap(), "job-1");
        assert_eq!(decoded.rows[0].i64(1).unwrap(), nanos);
        assert!(decoded.rows[0].text_opt(2).unwrap().is_none());
    }

    #[test]
    fn cast_text_integers_parse() {
        let decoded = decode_response(r#"{"rowsWritten":0,"rows":[["42"]]}"#).expect("decode");
        assert_eq!(decoded.rows[0].i64(0).unwrap(), 42);
    }
}
