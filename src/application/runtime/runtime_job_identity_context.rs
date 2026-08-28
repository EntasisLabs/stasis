use crate::domain::runtime::job::Job;
use crate::domain::runtime::provenance::ProvenanceRef;

#[derive(Clone)]
pub struct RuntimeJobIdentityContext {
    pub job_id: String,
    pub correlation_id: String,
    pub causation_id: String,
    pub trace_id: String,
    pub input_provenance: Option<ProvenanceRef>,
}

impl RuntimeJobIdentityContext {
    pub fn sttp_input_node_id(&self) -> String {
        use crate::domain::runtime::provenance::SttpProvenanceAdapter;
        SttpProvenanceAdapter::to_compat_string(self.input_provenance.as_ref())
    }
}

impl From<&Job> for RuntimeJobIdentityContext {
    fn from(job: &Job) -> Self {
        Self {
            job_id: job.id.clone(),
            correlation_id: job.correlation_id.clone(),
            causation_id: job.causation_id.clone(),
            trace_id: job.trace_id.clone(),
            input_provenance: job.input_provenance.clone(),
        }
    }
}