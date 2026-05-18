#[derive(Clone, Debug)]
pub struct HtmxSwap {
    pub target: String,
    pub swap: String,
    pub url: String,
    pub trigger: Option<String>,
}

impl HtmxSwap {
    pub fn inspector(url: impl Into<String>) -> Self {
        Self {
            target: "#inspector".to_string(),
            swap: "innerHTML".to_string(),
            url: url.into(),
            trigger: None,
        }
    }
}
