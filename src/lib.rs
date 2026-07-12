pub mod connection;
pub mod protocol;
pub mod registry;

use std::io;

use std::net::SocketAddr;
use std::sync::Arc;

use axum::{Router, routing::any};
use tokio::task::JoinHandle;

use crate::connection::{AppState, handler};
use crate::registry::Registry;

pub async fn run_server(addr: &str) -> (io::Result<SocketAddr>, JoinHandle<()>, Arc<Registry>) {
    let reg = Arc::new(Registry::new());
    let state = AppState::new(reg.clone());
    let app = Router::new().route("/ws", any(handler)).with_state(state);

    let listener = tokio::net::TcpListener::bind(addr).await.unwrap();
    let local_address = listener.local_addr();
    let handle = tokio::spawn(async move {
        let _ = axum::serve(listener, app).await;
    });

    (local_address, handle, reg)
}
