use axum::http::HeaderMap;

use crate::error::ApiError;

/// Header the public site should send on proxied chat completions.
///
/// Value is the lowercase hex encoding of the 32-byte session public key from
/// `@urspace/client` `generateSessionKey()`. Urspace does not inject identity
/// into loopback HTTP, so the browser must supply this.
pub const SUBJECT_HEADER: &str = "x-urspace-subject";
pub const SUBJECT_HEADER_ALT: &str = "x-stasis-subject";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Subject([u8; 32]);

impl Subject {
    pub fn from_bytes(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    pub fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }

    pub fn to_hex(&self) -> String {
        hex::encode(self.0)
    }
}

pub fn subject_hex(bytes: &[u8; 32]) -> String {
    hex::encode(bytes)
}

pub fn parse_public_key_bytes(bytes: &[u8]) -> Result<[u8; 32], ApiError> {
    <[u8; 32]>::try_from(bytes).map_err(|_| {
        ApiError::bad_request("public_key must be exactly 32 bytes (0–255)")
            .with_param("public_key")
    })
}

pub fn parse_subject_hex(value: &str) -> Result<[u8; 32], ApiError> {
    let trimmed = value.trim();
    let decoded = hex::decode(trimmed).map_err(|_| {
        ApiError::bad_request(format!(
            "{SUBJECT_HEADER} must be 64 hex characters (32-byte session public key)"
        ))
        .with_param(SUBJECT_HEADER)
    })?;
    parse_public_key_bytes(&decoded).map_err(|_| {
        ApiError::bad_request(format!(
            "{SUBJECT_HEADER} must be 64 hex characters (32-byte session public key)"
        ))
        .with_param(SUBJECT_HEADER)
    })
}

pub fn subject_from_headers(headers: &HeaderMap) -> Result<Subject, ApiError> {
    let raw = headers
        .get(SUBJECT_HEADER)
        .or_else(|| headers.get(SUBJECT_HEADER_ALT))
        .and_then(|value| value.to_str().ok())
        .ok_or_else(|| {
            ApiError::bad_request(format!(
                "missing {SUBJECT_HEADER} header (hex of the session public key from generateSessionKey)"
            ))
            .with_param(SUBJECT_HEADER)
        })?;
    Ok(Subject::from_bytes(parse_subject_hex(raw)?))
}

pub fn utc_date_yyyy_mm_dd() -> String {
    chrono::Utc::now().format("%Y-%m-%d").to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::{HeaderMap, HeaderValue};

    #[test]
    fn rejects_short_public_key() {
        let err = parse_public_key_bytes(&[1, 2, 3]).unwrap_err();
        assert_eq!(err.status, axum::http::StatusCode::BAD_REQUEST);
    }

    #[test]
    fn accepts_32_bytes() {
        let key = [7u8; 32];
        assert_eq!(parse_public_key_bytes(&key).unwrap(), key);
        assert_eq!(parse_subject_hex(&hex::encode(key)).unwrap(), key);
    }

    #[test]
    fn rejects_non_hex_subject() {
        assert!(parse_subject_hex("not-hex").is_err());
        assert!(parse_subject_hex("aa").is_err());
    }

    #[test]
    fn reads_subject_header() {
        let mut headers = HeaderMap::new();
        let key = [9u8; 32];
        headers.insert(
            SUBJECT_HEADER,
            HeaderValue::from_str(&hex::encode(key)).unwrap(),
        );
        assert_eq!(subject_from_headers(&headers).unwrap().as_bytes(), &key);
    }

    #[test]
    fn missing_subject_header_is_bad_request() {
        let err = subject_from_headers(&HeaderMap::new()).unwrap_err();
        assert_eq!(err.status, axum::http::StatusCode::BAD_REQUEST);
    }
}
