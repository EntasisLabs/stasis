pub trait IdGenerator: Send + Sync {
    fn next_id(&self, prefix: &str) -> String;
}
