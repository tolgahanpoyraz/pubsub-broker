use std::sync::{
    Arc,
    atomic::{
        AtomicU64,
        Ordering::{self},
    },
};

use axum::{
    extract::{
        State,
        ws::{Message, WebSocket, WebSocketUpgrade},
    },
    response::Response,
};
use futures_util::{sink::SinkExt, stream::StreamExt};
use tokio::sync::mpsc;

use crate::{
    protocol::{
        ClientMessage::{self, Publish, Subscribe, Unsubscribe},
        ServerMessage,
    },
    registry::Registry,
};

static COUNTER: AtomicU64 = AtomicU64::new(0);

#[derive(Clone)]
pub struct AppState {
    reg: Arc<Registry>,
}

impl AppState {
    pub fn new(reg: Arc<Registry>) -> Self {
        AppState { reg }
    }
}

pub async fn handler(ws: WebSocketUpgrade, State(state): State<AppState>) -> Response {
    ws.on_upgrade(|socket| handle_socket(socket, state))
}

async fn handle_socket(socket: WebSocket, state: AppState) {
    let subscriber_id = COUNTER.fetch_add(1, Ordering::Relaxed);

    let (tx, mut rx) = mpsc::channel(1024);

    let (mut sender, mut receiver) = socket.split();

    let reg = state.reg;
    let reader_reg = reg.clone();

    let mut reader = tokio::spawn(async move {
        while let Some(msg) = receiver.next().await {
            let Ok(msg) = msg else {
                return;
            };

            let Ok(text_bytes) = msg.into_text() else {
                continue;
            };

            let Ok(json) = serde_json::from_str::<ClientMessage>(text_bytes.as_str()) else {
                continue;
            };

            match json {
                Subscribe { topic } => {
                    let _ = tx.try_send(Arc::new(
                        serde_json::to_string(&ServerMessage::Ack {
                            op: "subscribe".to_string(),
                            topic: topic.clone(),
                        })
                        .unwrap(),
                    ));
                    reader_reg.subscribe(subscriber_id, topic, tx.clone());
                }
                Unsubscribe { topic } => {
                    let _ = tx.try_send(Arc::new(
                        serde_json::to_string(&ServerMessage::Ack {
                            op: "unsubscribe".to_string(),
                            topic: topic.clone(),
                        })
                        .unwrap(),
                    ));
                    reader_reg.unsubscribe(subscriber_id, &topic);
                }
                Publish { topic, data } => reader_reg.publish(ServerMessage::Message {
                    topic,
                    data,
                    ts: chrono::Utc::now(),
                }),
            }
        }
    });

    let mut writer = tokio::spawn(async move {
        while let Some(rec) = rx.recv().await {
            if sender
                .send(Message::Text(rec.as_str().into()))
                .await
                .is_err()
            {
                return;
            }
        }
    });

    tokio::select! {
        _ = &mut writer => {
            reg.disconnect_client(subscriber_id);
            reader.abort();
        }
        _ = &mut reader => {
            reg.disconnect_client(subscriber_id);
            writer.abort();
        },
    }
}
