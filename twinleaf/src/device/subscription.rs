use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use crate::device::{buffer::AlignedWindow, Buffer, ColumnSpec, SubscriptionId};
pub struct SubscriptionManager {
    buffer: Arc<Mutex<Buffer>>,
    subscriptions: HashMap<SubscriptionId, Subscription>,
    next_id: SubscriptionId,
}

pub struct Subscription {
    columns: Vec<ColumnSpec>,
    n_samples: usize,
    tx: crossbeam::channel::Sender<AlignedWindow>,
}

impl SubscriptionManager {
    pub fn new(buffer: Arc<Mutex<Buffer>>) -> Self {
        Self {
            buffer,
            subscriptions: HashMap::new(),
            next_id: 0,
        }
    }

    pub fn subscribe(
        &mut self,
        columns: Vec<ColumnSpec>,
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

    pub fn broadcast(&self) {
        let buffer = self.buffer.lock().unwrap();

        for sub in self.subscriptions.values() {
            match buffer.read_aligned_window(&sub.columns, sub.n_samples) {
                Ok(window) => {
                    // try_send will drop if channel full (subscriber too slow)
                    let _ = sub.tx.try_send(window);
                }
                Err(_) => {
                    // Subscriber will detect missing data
                }
            }
        }
    }
}
