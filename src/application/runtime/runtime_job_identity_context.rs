use crate::domain::runtime::job::Job;

#[derive(Clone)]
pub struct RuntimeJobIdentityContext {
    pub job_id: String,
    pub correlation_id: String,
    pub causation_id: String,
    pub trace_id: String,
    pub sttp_input_node_id: String,
}

impl From<&Job> for RuntimeJobIdentityContext {
    fn from(job: &Job) -> Self {
        Self {
            job_id: job.id.clone(),
            correlation_id: job.correlation_id.clone(),
            causation_id: job.causation_id.clone(),
            trace_id: job.trace_id.clone(),
            sttp_input_node_id: job.sttp_input_node_id.clone(),
        }
    }
}