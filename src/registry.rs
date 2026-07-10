use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use tokio::sync::mpsc;

pub struct Registry {
    reg: RwLock<HashMap<String, HashMap<u64, tokio::sync::mpsc::Sender<Arc<String>>>>>,
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
            .insert_entry(tx);
    }

    pub fn unsubscribe(&self, subscriber_id: u64, topic_id: String) {
        let mut guard = self.reg.write().unwrap();
        if let Some(inner) = guard.get_mut(&topic_id) {
            inner.remove(&subscriber_id);
        }
    }

    pub fn publish(&self, topic_id: String, message: Arc<String>) {
        let guard = self.reg.read().unwrap();
        for subscriber_map in guard.get(&topic_id).iter() {
            for (_, sender) in subscriber_map.iter() {
                let _ = sender.try_send(Arc::clone(&message));
            }
        }
    }
}
