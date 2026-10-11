//! Auto-trait bounds shared by the runtime stores [`crate::application::runtime::store_runtime::StoreRuntime`] drives.
//!
//! Native builds keep `Send + Sync` stores and `Send` futures. `wasm32` drops both,
//! using `async_trait`'s `?Send` form, so a later Durable Object / wasm SQLite
//! store can be `!Send` without a second copy of the orchestration engine.
//! In-memory stores stay `Send` on every target because their concrete types are.

/// Supertrait for runtime stores.
///
/// On native this is `Send + Sync`. On `wasm32` it has no auto-trait bound.
#[cfg(not(target_arch = "wasm32"))]
pub trait StoreBound: Send + Sync {}

#[cfg(not(target_arch = "wasm32"))]
impl<T: ?Sized + Send + Sync> StoreBound for T {}

/// Supertrait for runtime stores.
///
/// On `wasm32` this is implemented for every type, including `!Send` ones.
#[cfg(target_arch = "wasm32")]
pub trait StoreBound {}

#[cfg(target_arch = "wasm32")]
impl<T: ?Sized> StoreBound for T {}

/// Apply the store `async_trait` split: `Send` futures natively, `?Send` on `wasm32`.
///
/// Store trait definitions and their impls must both use this macro (or the same
/// `cfg_attr` pair). Mixing `#[async_trait]` with `#[async_trait(?Send)]` does not compile.
#[macro_export]
macro_rules! async_store {
    ($($item:item)*) => {
        $(
            #[allow(clippy::double_must_use)]
            #[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
            #[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
            $item
        )*
    };
}

/// Native `StoreBound` implies `Send + Sync`, so `dyn JobStore` stays `Send`.
#[cfg(not(target_arch = "wasm32"))]
#[allow(dead_code)]
fn native_store_bound_is_send_sync<T: StoreBound + ?Sized>() {
    fn assert_send_sync<U: Send + Sync + ?Sized>() {}
    assert_send_sync::<T>();
}

/// Native trait objects for the orchestration stores stay `Send`.
#[cfg(not(target_arch = "wasm32"))]
#[allow(dead_code)]
fn native_dyn_stores_are_send() {
    use std::sync::Arc;

    fn assert_send_sync<T: Send + Sync>() {}
    assert_send_sync::<Arc<dyn crate::ports::outbound::runtime::job_store::JobStore>>();
    assert_send_sync::<Arc<dyn crate::ports::outbound::runtime::recurring_store::RecurringStore>>();
    assert_send_sync::<Arc<dyn crate::ports::outbound::runtime::outbox_store::OutboxStore>>();
    assert_send_sync::<Arc<dyn crate::ports::outbound::runtime::job_attempt_store::JobAttemptStore>>(
    );
    assert_send_sync::<
        Arc<dyn crate::ports::outbound::runtime::durable_wait_store::DurableWaitStore>,
    >();
    assert_send_sync::<
        Arc<dyn crate::ports::outbound::runtime::job_continuation_store::JobContinuationStore>,
    >();
    assert_send_sync::<
        Arc<dyn crate::ports::outbound::runtime::inbound_trigger_store::InboundTriggerStore>,
    >();
    assert_send_sync::<
        Arc<dyn crate::ports::outbound::runtime::resource_lease_store::ResourceLeaseStore>,
    >();
}

/// `wasm32` store bounds accept a `!Send` type (`Rc`).
#[cfg(target_arch = "wasm32")]
#[allow(dead_code)]
fn wasm_store_bound_accepts_non_send() {
    fn assert_bound<T: StoreBound>() {}
    assert_bound::<std::rc::Rc<()>>();
}
