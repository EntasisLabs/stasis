use std::collections::HashSet;
use std::str::FromStr;

use chrono_tz::Tz;
use cron::Schedule;

use crate::error::StasisdError;
use crate::job_types::is_known_job_type;
use crate::model::{
    DesiredState, StasisdDocument, StasisdEndpoint, StasisdMcpGateway, StasisdMcpGatewayTransport,
    StasisdSchedule, MAX_PAYLOAD_BYTES,
};

pub fn validate_document(document: &StasisdDocument) -> Result<(), StasisdError> {
    let mut seen_schedules = HashSet::new();
    for schedule in &document.schedules {
        validate_schedule(schedule)?;
        if !seen_schedules.insert(schedule.id.clone()) {
            return Err(StasisdError::Validation(format!(
                "{}: duplicate schedule id '{}' within file",
                document.source_path.display(),
                schedule.id
            )));
        }
    }

    let mut seen_endpoints = HashSet::new();
    for endpoint in &document.endpoints {
        validate_endpoint(endpoint)?;
        if !seen_endpoints.insert(endpoint.id.clone()) {
            return Err(StasisdError::Validation(format!(
                "{}: duplicate endpoint id '{}' within file",
                document.source_path.display(),
                endpoint.id
            )));
        }
    }

    let mut seen_gateways = HashSet::new();
    for gateway in &document.mcp_gateways {
        validate_mcp_gateway(gateway)?;
        if !seen_gateways.insert(gateway.id.clone()) {
            return Err(StasisdError::Validation(format!(
                "{}: duplicate mcp_gateway id '{}' within file",
                document.source_path.display(),
                gateway.id
            )));
        }
    }

    let endpoint_ids: HashSet<&str> = document.endpoints.iter().map(|e| e.id.as_str()).collect();
    let gateway_ids: HashSet<&str> = document
        .mcp_gateways
        .iter()
        .map(|g| g.id.as_str())
        .collect();
    for schedule in &document.schedules {
        validate_schedule_participant_refs(schedule, &endpoint_ids, &gateway_ids)?;
    }

    Ok(())
}

pub fn validate_desired_state(desired: &DesiredState) -> Result<(), Vec<StasisdError>> {
    let mut errors = Vec::new();
    let mut seen_schedule_ids: HashSet<String> = HashSet::new();
    let mut seen_endpoint_ids: HashSet<String> = HashSet::new();
    let mut seen_gateway_ids: HashSet<String> = HashSet::new();

    for document in &desired.documents {
        if let Err(err) = validate_document(document) {
            errors.push(err);
            continue;
        }
        for schedule in &document.schedules {
            if !seen_schedule_ids.insert(schedule.id.clone()) {
                errors.push(StasisdError::Validation(format!(
                    "duplicate schedule id '{}' across config sources",
                    schedule.id
                )));
            }
        }
        for endpoint in &document.endpoints {
            if !seen_endpoint_ids.insert(endpoint.id.clone()) {
                errors.push(StasisdError::Validation(format!(
                    "duplicate endpoint id '{}' across config sources",
                    endpoint.id
                )));
            }
        }
        for gateway in &document.mcp_gateways {
            if !seen_gateway_ids.insert(gateway.id.clone()) {
                errors.push(StasisdError::Validation(format!(
                    "duplicate mcp_gateway id '{}' across config sources",
                    gateway.id
                )));
            }
        }
    }

    // Cross-document participant refs against the global endpoint/gateway sets.
    if errors.is_empty() {
        let endpoint_ids: HashSet<&str> =
            desired.endpoints.iter().map(|e| e.id.as_str()).collect();
        let gateway_ids: HashSet<&str> =
            desired.mcp_gateways.iter().map(|g| g.id.as_str()).collect();
        for schedule in &desired.schedules {
            if let Err(err) =
                validate_schedule_participant_refs(schedule, &endpoint_ids, &gateway_ids)
            {
                errors.push(err);
            }
        }
    }

    if errors.is_empty() {
        Ok(())
    } else {
        Err(errors)
    }
}

pub fn validate_endpoint(endpoint: &StasisdEndpoint) -> Result<(), StasisdError> {
    if endpoint.id.trim().is_empty() {
        return Err(StasisdError::Validation(
            "endpoint id must not be empty".into(),
        ));
    }
    if endpoint.id.contains(':') {
        return Err(StasisdError::Validation(format!(
            "endpoint id '{}' must not contain ':' (runtime ids use stasisd:endpoint:<id>)",
            endpoint.id
        )));
    }
    if endpoint.name.trim().is_empty() {
        return Err(StasisdError::Validation(format!(
            "endpoint '{}': name must not be empty",
            endpoint.id
        )));
    }
    if endpoint.target.trim().is_empty() {
        return Err(StasisdError::Validation(format!(
            "endpoint '{}': target must not be empty",
            endpoint.id
        )));
    }
    Ok(())
}

pub fn validate_mcp_gateway(gateway: &StasisdMcpGateway) -> Result<(), StasisdError> {
    if gateway.id.trim().is_empty() {
        return Err(StasisdError::Validation(
            "mcp_gateway id must not be empty".into(),
        ));
    }
    if gateway.id.contains(':') {
        return Err(StasisdError::Validation(format!(
            "mcp_gateway id '{}' must not contain ':'",
            gateway.id
        )));
    }
    match gateway.transport {
        StasisdMcpGatewayTransport::Command => {
            if gateway
                .command
                .as_deref()
                .map(str::trim)
                .unwrap_or_default()
                .is_empty()
            {
                return Err(StasisdError::Validation(format!(
                    "mcp_gateway '{}': command transport requires command",
                    gateway.id
                )));
            }
        }
        StasisdMcpGatewayTransport::Socket => {
            if gateway
                .socket_path
                .as_deref()
                .map(str::trim)
                .unwrap_or_default()
                .is_empty()
            {
                return Err(StasisdError::Validation(format!(
                    "mcp_gateway '{}': socket transport requires socket_path",
                    gateway.id
                )));
            }
        }
    }
    Ok(())
}

pub fn validate_schedule(schedule: &StasisdSchedule) -> Result<(), StasisdError> {
    if schedule.id.trim().is_empty() {
        return Err(StasisdError::Validation(
            "schedule id must not be empty".into(),
        ));
    }
    if schedule.id.contains(':') {
        return Err(StasisdError::Validation(format!(
            "schedule id '{}' must not contain ':' (runtime ids use stasisd:<id>)",
            schedule.id
        )));
    }
    if schedule.queue.trim().is_empty() {
        return Err(StasisdError::Validation(format!(
            "schedule '{}': queue must not be empty",
            schedule.id
        )));
    }
    if schedule.job_type.trim().is_empty() {
        return Err(StasisdError::Validation(format!(
            "schedule '{}': job_type must not be empty",
            schedule.id
        )));
    }
    if !is_known_job_type(&schedule.job_type) {
        return Err(StasisdError::Validation(format!(
            "schedule '{}': unknown job_type '{}'",
            schedule.id, schedule.job_type
        )));
    }
    Schedule::from_str(&schedule.cron).map_err(|err| {
        StasisdError::Validation(format!(
            "schedule '{}': invalid cron '{}': {err}",
            schedule.id, schedule.cron
        ))
    })?;
    if schedule.timezone.parse::<Tz>().is_err() {
        return Err(StasisdError::Validation(format!(
            "schedule '{}': invalid timezone '{}'",
            schedule.id, schedule.timezone
        )));
    }
    if schedule.jitter_seconds < 0 {
        return Err(StasisdError::Validation(format!(
            "schedule '{}': jitter_seconds must be >= 0",
            schedule.id
        )));
    }
    if schedule.max_attempts == 0 {
        return Err(StasisdError::Validation(format!(
            "schedule '{}': max_attempts must be >= 1",
            schedule.id
        )));
    }

    let payload_bytes = serde_json::to_vec(&schedule.payload).map_err(|err| {
        StasisdError::Validation(format!(
            "schedule '{}': payload is not serializable: {err}",
            schedule.id
        ))
    })?;
    if payload_bytes.len() > MAX_PAYLOAD_BYTES {
        return Err(StasisdError::Validation(format!(
            "schedule '{}': payload exceeds {MAX_PAYLOAD_BYTES} bytes ({})",
            schedule.id,
            payload_bytes.len()
        )));
    }

    Ok(())
}

fn validate_schedule_participant_refs(
    schedule: &StasisdSchedule,
    endpoint_ids: &HashSet<&str>,
    gateway_ids: &HashSet<&str>,
) -> Result<(), StasisdError> {
    // Waitable turn payload may carry endpoint_ref directly.
    if schedule.job_type == "workflow.stasis.agent_turn.waitable" {
        if let Some(endpoint_ref) = schedule
            .payload
            .get("endpoint_ref")
            .and_then(|v| v.as_str())
            && !endpoint_ids.contains(endpoint_ref)
        {
            return Err(StasisdError::Validation(format!(
                "schedule '{}': unknown endpoint_ref '{}'",
                schedule.id, endpoint_ref
            )));
        }
        if let Some(gateway_ref) = schedule
            .payload
            .get("mcp_gateway_ref")
            .and_then(|v| v.as_str())
            && !gateway_ids.contains(gateway_ref)
        {
            return Err(StasisdError::Validation(format!(
                "schedule '{}': unknown mcp_gateway_ref '{}'",
                schedule.id, gateway_ref
            )));
        }
    }

    // Agent session participants may declare kind=external + endpoint_ref.
    if schedule.job_type == "workflow.stasis.agent_session" {
        let Some(participants) = schedule.payload.get("participants").and_then(|v| v.as_array())
        else {
            return Ok(());
        };
        for (idx, participant) in participants.iter().enumerate() {
            let kind = participant
                .get("kind")
                .and_then(|v| v.as_str())
                .unwrap_or("local_tool_loop");
            if kind != "external" {
                continue;
            }
            let endpoint_ref = participant
                .get("endpoint_ref")
                .and_then(|v| v.as_str())
                .unwrap_or("");
            if endpoint_ref.is_empty() {
                return Err(StasisdError::Validation(format!(
                    "schedule '{}': participants[{idx}] kind=external requires endpoint_ref",
                    schedule.id
                )));
            }
            if !endpoint_ids.contains(endpoint_ref) {
                return Err(StasisdError::Validation(format!(
                    "schedule '{}': participants[{idx}] unknown endpoint_ref '{endpoint_ref}'",
                    schedule.id
                )));
            }
            if let Some(gateway_ref) = participant
                .get("mcp_gateway_ref")
                .and_then(|v| v.as_str())
                && !gateway_ids.contains(gateway_ref)
            {
                return Err(StasisdError::Validation(format!(
                    "schedule '{}': participants[{idx}] unknown mcp_gateway_ref '{gateway_ref}'",
                    schedule.id
                )));
            }
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{
        OnRemovePolicy, StasisdEndpoint, StasisdEndpointProtocol, StasisdMcpGateway,
        StasisdMcpGatewayTransport,
    };
    use serde_json::json;
    use std::path::PathBuf;

    fn valid_schedule(id: &str) -> StasisdSchedule {
        StasisdSchedule {
            id: id.into(),
            enabled: true,
            queue: "agents".into(),
            job_type: "workflow.stasis.prompt".into(),
            cron: "0 0 * * * * *".into(),
            timezone: "UTC".into(),
            jitter_seconds: 0,
            max_attempts: 3,
            on_remove: OnRemovePolicy::Drain,
            payload: json!({"user_prompt": "hi"}),
        }
    }

    fn valid_endpoint(id: &str) -> StasisdEndpoint {
        StasisdEndpoint {
            id: id.into(),
            name: "Fake".into(),
            protocol: StasisdEndpointProtocol::HttpWebhook,
            target: "http://127.0.0.1:9/agent".into(),
            metadata: json!({}),
            enabled: true,
            on_remove: OnRemovePolicy::Drain,
        }
    }

    #[test]
    fn accepts_valid_schedule() {
        validate_schedule(&valid_schedule("ok")).unwrap();
    }

    #[test]
    fn rejects_empty_id_and_colon() {
        let mut s = valid_schedule("x");
        s.id.clear();
        assert!(validate_schedule(&s).unwrap_err().to_string().contains("empty"));
        s.id = "stasisd:x".into();
        assert!(validate_schedule(&s).unwrap_err().to_string().contains(':'));
    }

    #[test]
    fn rejects_unknown_job_type_and_bad_cron_tz() {
        let mut s = valid_schedule("x");
        s.job_type = "workflow.nope".into();
        assert!(validate_schedule(&s).unwrap_err().to_string().contains("unknown job_type"));
        s = valid_schedule("x");
        s.cron = "not-a-cron".into();
        assert!(validate_schedule(&s).unwrap_err().to_string().contains("invalid cron"));
        s = valid_schedule("x");
        s.timezone = "Not/AZone".into();
        assert!(validate_schedule(&s).unwrap_err().to_string().contains("timezone"));
    }

    #[test]
    fn rejects_bad_numeric_and_payload_limits() {
        let mut s = valid_schedule("x");
        s.jitter_seconds = -1;
        assert!(validate_schedule(&s).is_err());
        s = valid_schedule("x");
        s.max_attempts = 0;
        assert!(validate_schedule(&s).is_err());
        s = valid_schedule("x");
        s.payload = json!( "x".repeat(MAX_PAYLOAD_BYTES + 1) );
        assert!(validate_schedule(&s).unwrap_err().to_string().contains("payload exceeds"));
    }

    #[test]
    fn rejects_external_participant_missing_or_unknown_endpoint() {
        let mut schedule = valid_schedule("mixed");
        schedule.job_type = "workflow.stasis.agent_session".into();
        schedule.payload = json!({
            "initial_user_prompt": "hi",
            "participants": [{
                "agent_id": "ext",
                "kind": "external",
                "tool_name": ""
            }]
        });
        let doc = StasisdDocument {
            api_version: "stasisd/v1".into(),
            source_path: PathBuf::from("a.toml"),
            content_hash: "a".into(),
            schedules: vec![schedule.clone()],
            endpoints: vec![],
            mcp_gateways: vec![],
        };
        assert!(validate_document(&doc)
            .unwrap_err()
            .to_string()
            .contains("requires endpoint_ref"));

        schedule.payload = json!({
            "initial_user_prompt": "hi",
            "participants": [{
                "agent_id": "ext",
                "kind": "external",
                "endpoint_ref": "missing",
                "tool_name": ""
            }]
        });
        let doc = StasisdDocument {
            api_version: "stasisd/v1".into(),
            source_path: PathBuf::from("a.toml"),
            content_hash: "a".into(),
            schedules: vec![schedule],
            endpoints: vec![valid_endpoint("fake")],
            mcp_gateways: vec![],
        };
        assert!(validate_document(&doc)
            .unwrap_err()
            .to_string()
            .contains("unknown endpoint_ref"));
    }

    #[test]
    fn rejects_mcp_command_without_command() {
        let gateway = StasisdMcpGateway {
            id: "g".into(),
            enabled: true,
            transport: StasisdMcpGatewayTransport::Command,
            socket_path: None,
            command: None,
            args: vec![],
            export_allowlist: vec![],
            metadata: json!({}),
        };
        assert!(validate_mcp_gateway(&gateway)
            .unwrap_err()
            .to_string()
            .contains("requires command"));
    }

    #[test]
    fn rejects_duplicate_ids_across_documents() {
        let doc_a = StasisdDocument {
            api_version: "stasisd/v1".into(),
            source_path: PathBuf::from("a.toml"),
            content_hash: "a".into(),
            schedules: vec![valid_schedule("dup")],
            endpoints: vec![],
            mcp_gateways: vec![],
        };
        let doc_b = StasisdDocument {
            api_version: "stasisd/v1".into(),
            source_path: PathBuf::from("b.toml"),
            content_hash: "b".into(),
            schedules: vec![valid_schedule("dup")],
            endpoints: vec![],
            mcp_gateways: vec![],
        };
        let desired = DesiredState {
            sources: vec![doc_a.source_path.clone(), doc_b.source_path.clone()],
            documents: vec![doc_a, doc_b],
            schedules: vec![valid_schedule("dup"), valid_schedule("dup")],
            endpoints: vec![],
            mcp_gateways: vec![],
            diagnostics: Vec::new(),
        };
        let errs = validate_desired_state(&desired).unwrap_err();
        assert!(errs.iter().any(|e| e.to_string().contains("across config")));
    }
}
