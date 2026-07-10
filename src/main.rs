mod connection;
mod protocol;
mod registry;

use std::sync::Arc;

use axum::{Router, routing::any};

use crate::connection::{AppState, handler};
use crate::registry::Registry;

#[tokio::main]
async fn main() {
    let reg = Arc::new(Registry::new());
    let state = AppState::new(reg);
    let app = Router::new().route("/ws", any(handler)).with_state(state);

    let listener = tokio::net::TcpListener::bind("127.0.0.1:3000")
        .await
        .unwrap();
    axum::serve(listener, app).await.unwrap();
}
