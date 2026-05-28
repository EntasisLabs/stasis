use stasis::sdk_prelude::{
    InvokeAgentRequest, InMemoryAgentRepository, RegisterAgentRequest, Result, StasisSdk,
};
use stasis::sdk_prelude_ext::GenaiLlmGateway;

#[tokio::main]
async fn main() -> Result<()> {
    let repo = InMemoryAgentRepository::default();
    let llm = GenaiLlmGateway::from_env();
    let sdk = StasisSdk::new(repo, llm);

    sdk.register_agent(RegisterAgentRequest {
        id: "planner".into(),
        name: "Planner".into(),
        system_prompt: "Break tasks into steps and keep output concise".into(),
    })
    .await?;

    let response = sdk
        .invoke_agent(InvokeAgentRequest {
            agent_id: "planner".into(),
            user_prompt: "Plan a production rollout checklist".into(),
        })
        .await?;

    println!("{}", response.completion);
    Ok(())
}
