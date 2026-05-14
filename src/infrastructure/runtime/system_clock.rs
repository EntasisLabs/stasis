use chrono::{DateTime, Utc};

use crate::ports::outbound::runtime::clock::Clock;

#[derive(Clone, Default)]
pub struct SystemClock;

impl Clock for SystemClock {
    fn now(&self) -> DateTime<Utc> {
        Utc::now()
    }
}
