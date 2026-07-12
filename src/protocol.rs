use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::protocol::ServerMessage::{Ack, Lagged, Message};

#[derive(Serialize, Deserialize, Debug)]
#[serde(tag = "op", rename_all = "lowercase")]
pub enum ClientMessage {
    Subscribe { topic: String },
    Unsubscribe { topic: String },
    Publish { topic: String, data: String },
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum ServerMessage {
    Message {
        topic: String,
        data: String,
        ts: DateTime<Utc>,
    },
    Lagged {
        topic: String,
        dropped: u64,
    },
    Ack {
        op: String,
        topic: String,
    },
}

impl ServerMessage {
    pub fn topic(&self) -> &str {
        match self {
            Message {
                topic,
                data: _,
                ts: _,
            } => &topic,
            Lagged { topic, dropped: _ } => &topic,
            Ack { topic, op: _ } => &topic,
        }
    }
}
