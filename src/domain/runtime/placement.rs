use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;

/// Per-job placement constraints evaluated atomically during lease acquisition.
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct PlacementConstraints {
    #[serde(default, skip_serializing_if = "BTreeSet::is_empty")]
    pub required_capabilities: BTreeSet<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub platform: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub architecture: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub region: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub target_node: Option<String>,
}

impl PlacementConstraints {
    pub fn unrestricted() -> Self {
        Self::default()
    }

    pub fn is_unrestricted(&self) -> bool {
        self.required_capabilities.is_empty()
            && self.platform.is_none()
            && self.architecture.is_none()
            && self.region.is_none()
            && self.target_node.is_none()
    }

    pub fn require_capability(mut self, capability: impl Into<String>) -> Self {
        self.required_capabilities.insert(capability.into());
        self
    }

    pub fn platform(mut self, platform: impl Into<String>) -> Self {
        self.platform = Some(platform.into());
        self
    }

    pub fn architecture(mut self, architecture: impl Into<String>) -> Self {
        self.architecture = Some(architecture.into());
        self
    }

    pub fn region(mut self, region: impl Into<String>) -> Self {
        self.region = Some(region.into());
        self
    }

    pub fn target_node(mut self, node_id: impl Into<String>) -> Self {
        self.target_node = Some(node_id.into());
        self
    }

    pub fn matches(&self, worker: &WorkerCapabilities) -> bool {
        if !self
            .required_capabilities
            .iter()
            .all(|cap| worker.capabilities.contains(cap))
        {
            return false;
        }
        if let Some(platform) = &self.platform {
            if worker.platform.as_ref() != Some(platform) {
                return false;
            }
        }
        if let Some(architecture) = &self.architecture {
            if worker.architecture.as_ref() != Some(architecture) {
                return false;
            }
        }
        if let Some(region) = &self.region {
            if worker.region.as_ref() != Some(region) {
                return false;
            }
        }
        if let Some(target) = &self.target_node {
            if worker.node_id.as_ref() != Some(target) {
                return false;
            }
        }
        true
    }
}

/// Declared capabilities of a worker attempting to lease work.
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct WorkerCapabilities {
    #[serde(default)]
    pub capabilities: BTreeSet<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub platform: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub architecture: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub region: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub node_id: Option<String>,
}

impl WorkerCapabilities {
    /// Matches only jobs with unrestricted placement.
    pub fn any() -> Self {
        Self::default()
    }

    pub fn with_capability(mut self, capability: impl Into<String>) -> Self {
        self.capabilities.insert(capability.into());
        self
    }

    pub fn platform(mut self, platform: impl Into<String>) -> Self {
        self.platform = Some(platform.into());
        self
    }

    pub fn architecture(mut self, architecture: impl Into<String>) -> Self {
        self.architecture = Some(architecture.into());
        self
    }

    pub fn region(mut self, region: impl Into<String>) -> Self {
        self.region = Some(region.into());
        self
    }

    pub fn node_id(mut self, node_id: impl Into<String>) -> Self {
        self.node_id = Some(node_id.into());
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unrestricted_matches_any_worker() {
        let constraints = PlacementConstraints::unrestricted();
        assert!(constraints.matches(&WorkerCapabilities::any()));
    }

    #[test]
    fn capability_and_target_node_are_enforced() {
        let constraints = PlacementConstraints::unrestricted()
            .require_capability("gpu")
            .region("us-west")
            .target_node("node-a");

        let ok = WorkerCapabilities::any()
            .with_capability("gpu")
            .region("us-west")
            .node_id("node-a");
        assert!(constraints.matches(&ok));

        let missing_gpu = WorkerCapabilities::any()
            .region("us-west")
            .node_id("node-a");
        assert!(!constraints.matches(&missing_gpu));

        let wrong_node = WorkerCapabilities::any()
            .with_capability("gpu")
            .region("us-west")
            .node_id("node-b");
        assert!(!constraints.matches(&wrong_node));
    }
}
