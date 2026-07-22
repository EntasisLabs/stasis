use std::collections::HashSet;
use std::str::FromStr;

use chrono_tz::Tz;
use cron::Schedule;

use crate::error::StasisdError;
use crate::job_types::is_known_job_type;
use crate::model::{DesiredState, StasisdDocument, StasisdSchedule, MAX_PAYLOAD_BYTES};

pub fn validate_document(document: &StasisdDocument) -> Result<(), StasisdError> {
    let mut seen = HashSet::new();
    for schedule in &document.schedules {
        validate_schedule(schedule)?;
        if !seen.insert(schedule.id.clone()) {
            return Err(StasisdError::Validation(format!(
                "{}: duplicate schedule id '{}' within file",
                document.source_path.display(),
                schedule.id
            )));
        }
    }
    Ok(())
}

pub fn validate_desired_state(desired: &DesiredState) -> Result<(), Vec<StasisdError>> {
    let mut errors = Vec::new();
    let mut seen_ids: HashSet<String> = HashSet::new();

    for document in &desired.documents {
        if let Err(err) = validate_document(document) {
            errors.push(err);
            continue;
        }
        for schedule in &document.schedules {
            if !seen_ids.insert(schedule.id.clone()) {
                errors.push(StasisdError::Validation(format!(
                    "duplicate schedule id '{}' across config sources",
                    schedule.id
                )));
            }
        }
    }

    if errors.is_empty() {
        Ok(())
    } else {
        Err(errors)
    }
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
    // Same dialect as `RecurringDefinition`: cron crate 7-field
    // (sec min hour day_of_month month day_of_week year).
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{OnRemovePolicy, StasisdDocument};
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
    fn rejects_duplicate_ids_across_documents() {
        let doc_a = StasisdDocument {
            api_version: "stasisd/v1".into(),
            source_path: PathBuf::from("a.toml"),
            content_hash: "a".into(),
            schedules: vec![valid_schedule("dup")],
        };
        let doc_b = StasisdDocument {
            api_version: "stasisd/v1".into(),
            source_path: PathBuf::from("b.toml"),
            content_hash: "b".into(),
            schedules: vec![valid_schedule("dup")],
        };
        let desired = DesiredState {
            sources: vec![doc_a.source_path.clone(), doc_b.source_path.clone()],
            documents: vec![doc_a, doc_b],
            schedules: vec![valid_schedule("dup"), valid_schedule("dup")],
            diagnostics: Vec::new(),
        };
        let errs = validate_desired_state(&desired).unwrap_err();
        assert!(errs.iter().any(|e| e.to_string().contains("across config")));
    }
}
