use chrono::{DateTime, Utc};
use chrono_tz::Tz;
use cron::Schedule;
use std::str::FromStr;

use crate::domain::errors::{Result, StasisError};

#[derive(Clone, Debug)]
pub struct RecurringDefinition {
    pub id: String,
    pub queue: String,
    pub job_type: String,
    pub payload_template_ref: String,
    pub cron_expr: String,
    pub timezone: String,
    pub jitter_seconds: i64,
    pub enabled: bool,
    pub max_attempts: u32,
    pub next_run_at: DateTime<Utc>,
    pub last_run_at: Option<DateTime<Utc>>,
    pub lease_owner: Option<String>,
    pub lease_expires_at: Option<DateTime<Utc>>,
}

impl RecurringDefinition {
    pub fn compute_next_run_at(&self, from: DateTime<Utc>) -> Result<DateTime<Utc>> {
        let schedule = Schedule::from_str(&self.cron_expr).map_err(|e| {
            StasisError::PortFailure(format!(
                "invalid cron expression for recurring_id={}: {}",
                self.id, e
            ))
        })?;

        let tz: Tz = self.timezone.parse().map_err(|e| {
            StasisError::PortFailure(format!(
                "invalid timezone for recurring_id={}: {}",
                self.id, e
            ))
        })?;

        let local_from = from.with_timezone(&tz);
        let next_local = schedule.after(&local_from).next().ok_or_else(|| {
            StasisError::PortFailure(format!(
                "could not compute next run for recurring_id={}",
                self.id
            ))
        })?;

        Ok(next_local.with_timezone(&Utc))
    }
}
