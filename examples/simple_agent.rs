use stasis::sdk_prelude::{
    InvokeAgentRequest, InMemoryAgentRepository, MockLlmGateway, RegisterAgentRequest, StasisSdk,
};

#[tokio::main]
async fn main() -> stasis::sdk_prelude::Result<()> {
    let repo = InMemoryAgentRepository::default();
    let llm = MockLlmGateway::new("mock completion");
    let sdk = StasisSdk::new(repo, llm);

    sdk.register_agent(RegisterAgentRequest {
        id: "planner".into(),
        name: "Planner".into(),
        system_prompt: "Break tasks into steps".into(),
    })
    .await?;

    let response = sdk
        .invoke_agent(InvokeAgentRequest {
            agent_id: "planner".into(),
            user_prompt: "Plan a sprint kickoff".into(),
        })
        .await?;

    println!("{}", response.completion);
    Ok(())
}
