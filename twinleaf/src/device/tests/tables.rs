//! How a call made of several devices waits for each device's RPC table,
//! on a paused clock.

use crate::device::connection::{table_by, TABLE_NOT_LOADED, TABLE_TIMEOUT};
use crate::proto::DeviceRoute;
use crate::session::rpc::{CallError, RpcRegistry, RpcRegistryError};
use crate::session::{Scope, Sink};
use std::sync::Arc;
use std::time::Duration;
use tokio::time::Instant;

fn scope() -> Scope {
    Scope::point(DeviceRoute::root())
}

/// A table that loads while the call waits is the one the call uses.
#[tokio::test(start_paused = true)]
async fn a_table_that_loads_in_time_is_used() {
    let (mut sink, updates) = Sink::latest(scope());
    let table = Arc::new(RpcRegistry::new(Vec::new()));
    let loaded = Arc::clone(&table);
    let started = Instant::now();
    let loading = crate::chan::spawn(async move {
        tokio::time::sleep(Duration::from_secs(5)).await;
        sink.lane.send(Ok(loaded))
    });
    let waited = table_by(&updates, started + TABLE_TIMEOUT).await;
    assert!(Arc::ptr_eq(&waited.unwrap(), &table));
    assert_eq!(started.elapsed(), Duration::from_secs(5));
    assert!(loading.await.unwrap());
}

/// A table that does not load by the deadline refuses the device, saying
/// its table has not loaded.
#[tokio::test(start_paused = true)]
async fn a_table_that_does_not_load_in_time_refuses_the_device() {
    let (_sink, updates) = Sink::latest(scope());
    let started = Instant::now();
    let waited = table_by(&updates, started + TABLE_TIMEOUT).await;
    assert_eq!(waited.unwrap_err(), TABLE_NOT_LOADED);
    assert_eq!(started.elapsed(), TABLE_TIMEOUT);
}

/// A table that failed to load refuses the device at once, saying why.
#[tokio::test(start_paused = true)]
async fn a_table_that_failed_to_load_refuses_the_device_with_why() {
    let (mut sink, updates) = Sink::latest(scope());
    let failed = RpcRegistryError::DeviceRpcError(CallError::Timeout);
    let why = format!("its RPC table did not load. {failed}");
    assert!(sink.lane.send(Err(failed)));
    let started = Instant::now();
    let waited = table_by(&updates, started + TABLE_TIMEOUT).await;
    assert_eq!(waited.unwrap_err(), why);
    assert_eq!(started.elapsed(), Duration::ZERO);
}
