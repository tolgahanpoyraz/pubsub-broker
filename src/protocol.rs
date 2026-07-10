use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug)]
#[serde(tag = "op", rename_all = "lowercase")]
enum ClientMessage {
    Subscribe { topic: String },
    Unsubscribe { topic: String },
    Publish { topic: String, data: String },
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(tag = "type", rename_all = "lowercase")]
enum ServerMessage {
    Message {
        topic: String,
        data: String,
        ts: u64,
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
