use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use tokio::sync::mpsc;
use tokio::sync::mpsc::error::TrySendError;

use crate::protocol::ServerMessage;

struct Subscriber {
    sender: tokio::sync::mpsc::Sender<Arc<String>>,
    dropped: AtomicU64,
}

pub struct Registry {
    reg: RwLock<HashMap<String, HashMap<u64, Subscriber>>>,
}

impl Registry {
    pub fn new() -> Self {
        Registry {
            reg: RwLock::new(HashMap::new()),
        }
    }

    pub fn subscribe(&self, subscriber_id: u64, topic_id: String, tx: mpsc::Sender<Arc<String>>) {
        let mut guard = self.reg.write().unwrap();
        guard
            .entry(topic_id)
            .or_default()
            .entry(subscriber_id)
            .insert_entry(Subscriber {
                sender: tx,
                dropped: AtomicU64::new(0),
            });
    }

    pub fn unsubscribe(&self, subscriber_id: u64, topic_id: &String) {
        let mut guard = self.reg.write().unwrap();
        if let Some(inner) = guard.get_mut(topic_id) {
            inner.remove(&subscriber_id);
            if inner.is_empty() {
                guard.remove(topic_id);
            }
        }
    }

    pub fn publish(&self, message: ServerMessage) {
        let guard = self.reg.read().unwrap();
        let serialized = Arc::new(serde_json::to_string(&message).unwrap());
        for subscriber_map in guard.get(message.topic()).iter() {
            for (_, subscriber) in subscriber_map.iter() {
                let res = subscriber.sender.try_send(serialized.clone());
                if let Err(TrySendError::Full(_)) = res {
                    subscriber.dropped.fetch_add(1, Ordering::Relaxed);
                }
            }
        }
    }

    pub fn disconnect_client(&self, subscriber_id: u64) {
        let mut guard = self.reg.write().unwrap();
        for inner in guard.values_mut() {
            inner.remove(&subscriber_id);
        }
    }
}
