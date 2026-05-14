use std::sync::Arc;

use async_trait::async_trait;

use crate::application::orchestration::tool_registry::{InMemoryToolRegistry, StasisTool};
use crate::application::runtime::agent_session_job_handler::AgentSessionJobHandler;
use crate::application::runtime::agent_turn_job_handler::AgentTurnJobHandler;
use crate::application::runtime::grapheme_echo_job_handler::GraphemeEchoJobHandler;
use crate::application::runtime::grapheme_healthcheck_job_handler::GraphemeHealthcheckJobHandler;
use crate::application::runtime::grapheme_job_handler::GraphemeJobHandler;
use crate::application::runtime::grapheme_textops_job_handler::GraphemeTextOpsJobHandler;
use crate::application::runtime::in_memory_runtime::{JobExecutionOutcome, JobHandler};
use crate::application::runtime::prompt_chat_job_handler::PromptChatJobHandler;
use crate::application::runtime::runtime_factory::{RuntimeBackend, RuntimeComposition, RuntimeFactory};
use crate::application::runtime::tool_loop_job_handler::ToolLoopJobHandler;
use crate::domain::errors::Result;
use crate::infrastructure::llm::genai_chat_client::GenaiChatClient;
use crate::infrastructure::runtime::grapheme_sdk_workflow_engine::GraphemeSdkWorkflowEngine;
use crate::ports::outbound::ai_chat_client::AiChatClient;

#[derive(Clone)]
struct DelegatingJobHandler {
    inner: Arc<dyn JobHandler>,
}

#[async_trait]
impl JobHandler for DelegatingJobHandler {
    fn job_type(&self) -> &'static str {
        self.inner.job_type()
    }

    async fn execute(&self, job: &crate::domain::runtime::job::Job) -> Result<JobExecutionOutcome> {
        self.inner.execute(job).await
    }
}

#[derive(Clone)]
pub struct StasisRuntimeBuilder {
    backend: RuntimeBackend,
    chat_client: Option<Arc<dyn AiChatClient>>,
    tool_registry: InMemoryToolRegistry,
    include_grapheme_handlers: bool,
    include_prompt_handler: bool,
    include_tool_loop_handler: bool,
    include_agent_handlers: bool,
    extra_handlers: Vec<Arc<dyn JobHandler>>,
}

impl StasisRuntimeBuilder {
    pub fn new(backend: RuntimeBackend) -> Self {
        Self {
            backend,
            chat_client: None,
            tool_registry: InMemoryToolRegistry::default(),
            include_grapheme_handlers: true,
            include_prompt_handler: true,
            include_tool_loop_handler: true,
            include_agent_handlers: true,
            extra_handlers: Vec::new(),
        }
    }

    pub fn with_chat_client(mut self, chat_client: Arc<dyn AiChatClient>) -> Self {
        self.chat_client = Some(chat_client);
        self
    }

    pub fn with_tool<T: StasisTool + 'static>(self, tool: T) -> Result<Self> {
        self.tool_registry.register_tool(tool)?;
        Ok(self)
    }

    pub fn with_extra_handler<H: JobHandler + 'static>(mut self, handler: H) -> Self {
        self.extra_handlers.push(Arc::new(handler));
        self
    }

    pub fn without_grapheme_handlers(mut self) -> Self {
        self.include_grapheme_handlers = false;
        self
    }

    pub fn without_prompt_handler(mut self) -> Self {
        self.include_prompt_handler = false;
        self
    }

    pub fn without_tool_loop_handler(mut self) -> Self {
        self.include_tool_loop_handler = false;
        self
    }

    pub fn without_agent_handlers(mut self) -> Self {
        self.include_agent_handlers = false;
        self
    }

    pub async fn build(self) -> Result<RuntimeComposition> {
        let runtime = RuntimeFactory::build(self.backend).await?;
        let workflow_engine = Arc::new(GraphemeSdkWorkflowEngine::new());
        let chat_client = self
            .chat_client
            .unwrap_or_else(|| Arc::new(GenaiChatClient::from_env()));
        let tool_registry = Arc::new(self.tool_registry);

        match &runtime {
            RuntimeComposition::InMemory(rt) => {
                if self.include_grapheme_handlers {
                    rt.register_handler(GraphemeJobHandler::new(workflow_engine.clone()))?;
                    rt.register_handler(GraphemeHealthcheckJobHandler::new(workflow_engine.clone()))?;
                    rt.register_handler(GraphemeEchoJobHandler::new(workflow_engine.clone()))?;
                    rt.register_handler(GraphemeTextOpsJobHandler::new(workflow_engine.clone()))?;
                }

                if self.include_prompt_handler {
                    rt.register_handler(PromptChatJobHandler::new(chat_client.clone()))?;
                }

                if self.include_tool_loop_handler {
                    rt.register_handler(ToolLoopJobHandler::new(chat_client.clone(), tool_registry.clone()))?;
                }

                if self.include_agent_handlers {
                    rt.register_handler(AgentTurnJobHandler::new(chat_client.clone(), tool_registry.clone()))?;
                    rt.register_handler(AgentSessionJobHandler::new(chat_client.clone(), tool_registry.clone()))?;
                }

                for handler in &self.extra_handlers {
                    rt.register_handler(DelegatingJobHandler {
                        inner: handler.clone(),
                    })?;
                }
            }
            RuntimeComposition::Surreal(rt) => {
                if self.include_grapheme_handlers {
                    rt.register_handler(GraphemeJobHandler::new(workflow_engine.clone()))?;
                    rt.register_handler(GraphemeHealthcheckJobHandler::new(workflow_engine.clone()))?;
                    rt.register_handler(GraphemeEchoJobHandler::new(workflow_engine.clone()))?;
                    rt.register_handler(GraphemeTextOpsJobHandler::new(workflow_engine.clone()))?;
                }

                if self.include_prompt_handler {
                    rt.register_handler(PromptChatJobHandler::new(chat_client.clone()))?;
                }

                if self.include_tool_loop_handler {
                    rt.register_handler(ToolLoopJobHandler::new(chat_client.clone(), tool_registry.clone()))?;
                }

                if self.include_agent_handlers {
                    rt.register_handler(AgentTurnJobHandler::new(chat_client.clone(), tool_registry.clone()))?;
                    rt.register_handler(AgentSessionJobHandler::new(chat_client.clone(), tool_registry.clone()))?;
                }

                for handler in &self.extra_handlers {
                    rt.register_handler(DelegatingJobHandler {
                        inner: handler.clone(),
                    })?;
                }
            }
        }

        Ok(runtime)
    }
}
