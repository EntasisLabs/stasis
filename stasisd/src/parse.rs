use std::path::Path;

use sha2::{Digest, Sha256};

use crate::error::StasisdError;
use crate::model::{
    StasisdDocument, StasisdEndpoint, StasisdFileDocument, StasisdMcpGateway, StasisdSchedule,
    API_VERSION,
};

pub fn parse_config_bytes(
    source_path: &Path,
    bytes: &[u8],
) -> Result<StasisdDocument, StasisdError> {
    let ext = source_path
        .extension()
        .and_then(|ext| ext.to_str())
        .unwrap_or_default()
        .to_ascii_lowercase();

    let file_doc: StasisdFileDocument = match ext.as_str() {
        "toml" => {
            let text = std::str::from_utf8(bytes).map_err(|err| {
                StasisdError::Validation(format!(
                    "{}: invalid utf-8: {err}",
                    source_path.display()
                ))
            })?;
            toml::from_str(text).map_err(|err| {
                StasisdError::Validation(format!(
                    "{}: failed to parse toml: {err}",
                    source_path.display()
                ))
            })?
        }
        "yaml" | "yml" => serde_yaml::from_slice(bytes).map_err(|err| {
            StasisdError::Validation(format!(
                "{}: failed to parse yaml: {err}",
                source_path.display()
            ))
        })?,
        other => {
            return Err(StasisdError::Validation(format!(
                "{}: unsupported extension '{other}'",
                source_path.display()
            )));
        }
    };

    if file_doc.api_version != API_VERSION {
        return Err(StasisdError::Validation(format!(
            "{}: unsupported api_version='{}' (expected '{API_VERSION}')",
            source_path.display(),
            file_doc.api_version
        )));
    }

    let content_hash = hash_document_resources(
        &file_doc.schedule,
        &file_doc.endpoint,
        &file_doc.mcp_gateway,
    );
    Ok(StasisdDocument {
        api_version: file_doc.api_version,
        source_path: source_path.to_path_buf(),
        content_hash,
        schedules: file_doc.schedule,
        endpoints: file_doc.endpoint,
        mcp_gateways: file_doc.mcp_gateway,
    })
}

pub fn hash_document_resources(
    schedules: &[StasisdSchedule],
    endpoints: &[StasisdEndpoint],
    mcp_gateways: &[StasisdMcpGateway],
) -> String {
    let encoded = serde_json::to_vec(&(schedules, endpoints, mcp_gateways)).unwrap_or_default();
    let digest = Sha256::digest(encoded);
    hex::encode(digest)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    #[test]
    fn parses_toml_schedule() {
        let toml = r#"
api_version = "stasisd/v1"

[[schedule]]
id = "nightly"
queue = "agents"
job_type = "workflow.stasis.agent_session"
cron = "0 0 2 * * * *"
"#;
        let doc = parse_config_bytes(Path::new("agents.toml"), toml.as_bytes()).unwrap();
        assert_eq!(doc.schedules.len(), 1);
        assert_eq!(doc.schedules[0].id, "nightly");
        assert_eq!(doc.schedules[0].timezone, "UTC");
        assert!(!doc.content_hash.is_empty());
    }

    #[test]
    fn parses_endpoint_and_mcp_gateway() {
        let toml = r#"
api_version = "stasisd/v1"

[[endpoint]]
id = "fake-external"
name = "Fake external"
protocol = "http_webhook"
target = "http://127.0.0.1:39001/agent"

[[mcp_gateway]]
id = "local-mcp"
transport = "command"
command = "fake-mcp-gateway"
args = ["--stdio"]
export_allowlist = ["summarize"]

[[schedule]]
id = "external-turn"
queue = "agents"
job_type = "workflow.stasis.agent_turn.waitable"
cron = "0/5 * * * * * *"
payload = { agent_id = "external-reviewer", session_id = "s1", turn_id = "t1", user_prompt = "review", endpoint_ref = "fake-external", timeout_seconds = 30, poll_interval_seconds = 1 }
"#;
        let doc = parse_config_bytes(Path::new("join.toml"), toml.as_bytes()).unwrap();
        assert_eq!(doc.endpoints.len(), 1);
        assert_eq!(doc.endpoints[0].id, "fake-external");
        assert_eq!(doc.mcp_gateways.len(), 1);
        assert_eq!(doc.mcp_gateways[0].command.as_deref(), Some("fake-mcp-gateway"));
        assert_eq!(
            doc.schedules[0]
                .payload
                .get("endpoint_ref")
                .and_then(|v| v.as_str()),
            Some("fake-external")
        );
    }

    #[test]
    fn parses_yaml_schedule() {
        let yaml = r#"
api_version: stasisd/v1
schedule:
  - id: hourly
    queue: agents
    job_type: workflow.stasis.prompt
    cron: "0 0 * * * * *"
    payload:
      user_prompt: hello
"#;
        let doc = parse_config_bytes(Path::new("agents.yaml"), yaml.as_bytes()).unwrap();
        assert_eq!(doc.schedules[0].id, "hourly");
        assert_eq!(
            doc.schedules[0].payload.get("user_prompt").and_then(|v| v.as_str()),
            Some("hello")
        );
    }

    #[test]
    fn rejects_wrong_api_version() {
        let toml = r#"
api_version = "stasisd/v0"
[[schedule]]
id = "x"
queue = "q"
job_type = "workflow.stasis.prompt"
cron = "0 0 * * * * *"
"#;
        let err = parse_config_bytes(Path::new("bad.toml"), toml.as_bytes()).unwrap_err();
        assert!(err.to_string().contains("api_version"));
    }

    #[test]
    fn rejects_malformed_toml() {
        let err = parse_config_bytes(Path::new("bad.toml"), b"[[schedule]\n").unwrap_err();
        assert!(err.to_string().contains("parse toml"));
    }

    #[test]
    fn rejects_malformed_yaml() {
        let err = parse_config_bytes(Path::new("bad.yaml"), b"api_version: [\n").unwrap_err();
        assert!(err.to_string().contains("parse yaml"));
    }

    #[test]
    fn rejects_unsupported_extension() {
        let err = parse_config_bytes(&PathBuf::from("x.json"), b"{}").unwrap_err();
        assert!(err.to_string().contains("unsupported extension"));
    }
}
