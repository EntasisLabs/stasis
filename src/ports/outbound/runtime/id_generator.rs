pub trait IdGenerator: Send + Sync {
    fn next_id(&self, prefix: &str) -> String;
    /// Durable allocators must surface storage errors, never reuse a fallback ID.
    fn try_next_id(&self, prefix: &str) -> crate::domain::errors::Result<String> {
        Ok(self.next_id(prefix))
    }
}
