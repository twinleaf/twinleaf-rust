use std::{
    collections::HashMap,
};

use crate::tio::proto::identifiers::ColumnKey;
use crate::data::{AlignedWindow, Buffer}; 

pub type SubscriptionId = usize;
pub struct SubscriptionManager {
    pub buffer: Buffer,
    subscriptions: HashMap<SubscriptionId, Subscription>,
    next_id: SubscriptionId,
}

pub struct Subscription {
    columns: Vec<ColumnKey>,
    n_samples: usize,
    tx: crossbeam::channel::Sender<AlignedWindow>,
}

impl SubscriptionManager {
    pub fn new(buffer: Buffer) -> Self {
        Self {
            buffer,
            subscriptions: HashMap::new(),
            next_id: 0,
        }
    }

    pub fn subscribe(
        &mut self,
        columns: Vec<ColumnKey>,
        n_samples: usize,
    ) -> (SubscriptionId, crossbeam::channel::Receiver<AlignedWindow>) {
        let (tx, rx) = crossbeam::channel::bounded(10); // bounded to allow drops
        let id = self.next_id;
        self.next_id += 1;

        self.subscriptions.insert(
            id,
            Subscription {
                columns,
                n_samples,
                tx,
            },
        );

        (id, rx)
    }

    pub fn unsubscribe(&mut self, id: SubscriptionId) {
        self.subscriptions.remove(&id);
    }

    pub fn unsubscribe_all(&mut self) {
        self.subscriptions.clear();
    }

    pub fn broadcast(&self) {
        for sub in self.subscriptions.values() {
            match self.buffer.read_aligned_window(&sub.columns, sub.n_samples) {
                Ok(window) => {
                    let _ = sub.tx.try_send(window);
                }
                Err(_) => {
                }
            }
        }
    }
}