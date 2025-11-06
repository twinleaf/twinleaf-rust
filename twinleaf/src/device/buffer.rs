//! Buffer
//! Stores `Sample`s, emits data according to a `Reader`.
//! 
//! Assumptions:
//! 1. Time stamps of `Sample`s are exactly aligned
//! 2. Sample rates of columns sharing the same name are the same
//! 3. Sample numbers are independent between `Device`s

use std::collections::{HashMap, VecDeque};
use crate::{data::Sample, device::DeviceTree, tio::{self, proto::DeviceRoute}};

pub type SessionId = u32;
pub type SampleNumber = u32;

pub enum BufferEvent {
    MetadataChanged(DeviceRoute),
    SegmentChanged(DeviceRoute),
    SessionChanged {
        route: DeviceRoute,
        new_id: SessionId,
    },
    SamplesSkipped {
        route: DeviceRoute,
        session_id: SessionId,
        expected: SampleNumber,
        received: SampleNumber,
        count: u32,
    },
    SamplesBackward {
        route: DeviceRoute,
        session_id: SessionId,
        previous: SampleNumber,
        current: SampleNumber,
    },
    TimeSkipped {
        route: DeviceRoute,
        session_id: SessionId,
        previous_timestamp: f64,
        current_timestamp: f64,
        gap: f64,
        expected_gap: f64,
    },
    TimeBackward {
        route: DeviceRoute,
        session_id: SessionId,
        previous_timestamp: f64,
        current_timestamp: f64,
    },
}

pub struct Buffer {
    tree: DeviceTree,
    pub route_buffers: HashMap<DeviceRoute, HashMap<SessionId, VecDeque<Sample>>>,
    pub latest_timestamps: HashMap<DeviceRoute, f64>,
    last_sample_numbers: HashMap<(DeviceRoute, SessionId), SampleNumber>,
    last_timestamps: HashMap<(DeviceRoute, SessionId), f64>,
    event_tx: crossbeam::channel::Sender<BufferEvent>,
    capacity: usize,
}

impl Buffer {
    pub fn new(tree: DeviceTree, event_tx: crossbeam::channel::Sender<BufferEvent>, capacity: usize) -> Self {       
        Buffer {
            tree,
            route_buffers: HashMap::new(),
            latest_timestamps: HashMap::new(),
            last_sample_numbers: HashMap::new(),
            last_timestamps: HashMap::new(),
            event_tx,
            capacity,
        }
    }

    pub fn drain_tree(&mut self) -> Result<(), tio::proxy::RpcError> {
        let samples = self.tree.drain()?;
        for (sample, route) in samples {
            self.process_sample(sample, route);
        }
        Ok(())
    }

    pub fn process_sample(&mut self, sample: Sample, route: DeviceRoute) {
        if sample.meta_changed {
            let _ = self.event_tx.try_send(BufferEvent::MetadataChanged(route.clone()));
        }
        if sample.segment_changed{
            let _ = self.event_tx.try_send(BufferEvent::SegmentChanged(route.clone()));
        }

        let session_id = sample.device.session_id;
        let key = (route.clone(), session_id);

        if !self.last_sample_numbers.contains_key(&key) {
            let _ = self.event_tx.try_send(BufferEvent::SessionChanged { 
                route: route.clone(), 
                new_id: session_id 
            });
            self.last_sample_numbers.insert(key.clone(), sample.n);
            self.last_timestamps.insert(key.clone(), sample.timestamp_end());
        } else {
            let last_n = *self.last_sample_numbers.get(&key).unwrap();
            
            if sample.n < last_n {
                let _ = self.event_tx.try_send(BufferEvent::SamplesBackward {
                    route: route.clone(),
                    session_id,
                    previous: last_n,
                    current: sample.n,
                });
            } else if sample.n > last_n + 1 {
                let skipped_count = sample.n - (last_n + 1);
                let _ = self.event_tx.try_send(BufferEvent::SamplesSkipped {
                    route: route.clone(),
                    session_id,
                    expected: last_n + 1,
                    received: sample.n,
                    count: skipped_count,
                });
            }
            
            let last_timestamp_end = *self.last_timestamps.get(&key).unwrap();
            let current_timestamp_begin = sample.timestamp_begin();
            let current_timestamp_end = sample.timestamp_end();
            
            if current_timestamp_begin < last_timestamp_end {
                let _ = self.event_tx.try_send(BufferEvent::TimeBackward {
                    route: route.clone(),
                    session_id,
                    previous_timestamp: last_timestamp_end,
                    current_timestamp: current_timestamp_begin,
                });
            } else if current_timestamp_begin > last_timestamp_end {
                let time_gap = current_timestamp_begin - last_timestamp_end;
                let sample_period = sample.timestamp_end() - sample.timestamp_begin();
                
                if time_gap > sample_period * 0.5 {
                    let _ = self.event_tx.try_send(BufferEvent::TimeSkipped {
                        route: route.clone(),
                        session_id,
                        previous_timestamp: last_timestamp_end,
                        current_timestamp: current_timestamp_begin,
                        gap: time_gap,
                        expected_gap: 0.0,
                    });
                }
            }
            *self.last_sample_numbers.get_mut(&key).unwrap() = sample.n;
            *self.last_timestamps.get_mut(&key).unwrap() = current_timestamp_end;
        }

        let route_map = self.route_buffers.entry(route.clone()).or_default();
        let buffer = route_map.entry(session_id).or_default();
        
        self.latest_timestamps.insert(route, sample.timestamp_end());
        buffer.push_back(sample);
        
        if buffer.len() > self.capacity {
            buffer.pop_front();
        }
    }
}