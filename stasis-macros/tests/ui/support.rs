pub mod fake_stasis {
    pub mod macro_support {
        pub use async_trait;
        pub use schemars;
        pub use serde;
        pub use serde_json;
    }

    pub mod domain {
        pub mod errors {
            pub type Result<T> = core::result::Result<T, StasisError>;

            #[derive(Debug)]
            pub enum StasisError {
                PortFailure(String),
            }
        }
    }

    pub mod application {
        pub mod orchestration {
            pub mod tool_registry {
                use async_trait::async_trait;
                use serde_json::Value;

                use crate::support::fake_stasis::domain::errors::Result;

                #[async_trait]
                pub trait StasisTool: Send + Sync {
                    fn name(&self) -> &'static str;
                    fn description(&self) -> Option<&'static str> {
                        None
                    }
                    fn input_schema(&self) -> Option<Value> {
                        None
                    }
                    fn output_schema(&self) -> Option<Value> {
                        None
                    }
                    async fn invoke(&self, input: Value) -> Result<Value>;
                }
            }
        }
    }
}
