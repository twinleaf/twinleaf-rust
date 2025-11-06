//! Reader
//! Takes keys (e.g., `DeviceKey`, `StreamKey`, `ColumnKey`) and tracks a uniform cursor from a `Buffer`
//! - Derives `DeviceKey` from `DeviceRoute`
//! - Derives `StreamKey` from `stream_id` and `DeviceRoute`
//! - Derives `ColumnKey` from `column_id` and `stream_id` and `DeviceRoute`
//! 
//! Has atomic reference to a `Buffer`, allowing it to return either aligned `Sample`s or a more efficient data structure ([timestamp, value, route] matrix / HashMap)
//! 
//! Concerns:
//! 1. What a `Reader` returns (window contents).
//! 2. Whether it may wait (blocking vs. non-blocking).
//! 3. How frames are aligned across routes (exact time alignment).
//!
//! Anchors:
//! - T: cursor from `Buffer`, given by min(latest_timestamp) across the `Reader` routes.
//! - C: cursor from `Reader` (advanced only by successful `next` reads).
//!
//! - `get_next(N)`
//!     Blocking; consume-based. Anchor at C.
//!     Wait until there are at least N aligned samples strictly after C for each `DeviceRoute` in `Reader.routes()`.
//!     Return exactly N samples per route; advance C by N.
//!
//! - `try_next(N)`
//!     Non-blocking; consume-based. Anchor at C.
//!     If there are at least N aligned samples after C for each `DeviceRoute` in `Reader.routes()`, return exactly N and advance C by N.
//!     Otherwise, return None and do not advance C.
//!
//! - `get_last(N)`
//!     Blocking; snapshot-based. Anchor at T.
//!     Wait until each `DeviceRoute` in `Reader.routes()` has at least N aligned samples ending at T.
//!     Return exactly N samples per route
//!
//! - `try_last(N)`
//!     Non-blocking; snapshot-based. Anchor at T.
//!     If each `DeviceRoute` in `Reader.routes()` has at least N aligned samples ending at T, return exactly N. 
//!     Otherwise, return None.

use std::collections::HashMap;
use crate::{data::Sample, device::buffer::Buffer, tio::{self, proto::DeviceRoute}};

pub struct Reader {
    routes: Vec<DeviceRoute>,
    cursors: HashMap<DeviceRoute, usize>,
}

impl Reader {
    pub fn new(routes: Vec<DeviceRoute>) -> Self {
        let mut cursors = HashMap::new();
        for route in &routes {
            cursors.insert(route.clone(), 0);
        }
        
        Reader { routes, cursors }
    }
    
    pub fn routes(&self) -> &[DeviceRoute] {
        &self.routes
    }
    
    fn compute_t(&self, buffer: &Buffer) -> Option<f64> {
        self.routes
            .iter()
            .filter_map(|route| buffer.latest_timestamps.get(route))
            .copied()
            .min_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal))
    }
    
    fn get_newest_session_id(&self, buffer: &Buffer, route: &DeviceRoute) -> Option<u32> {
        let route_map = buffer.route_buffers.get(route)?;
        route_map.keys().max().copied()
    }
    
    fn get_samples_from_cursor(
        &self,
        buffer: &Buffer,
        route: &DeviceRoute,
        n: usize,
    ) -> Option<Vec<Sample>> {
        let route_map = buffer.route_buffers.get(route)?;
        let newest_session_id = self.get_newest_session_id(buffer, route)?;
        let session_buffer = route_map.get(&newest_session_id)?;
        
        if session_buffer.len() < n {
            return None;
        }
        
        Some(session_buffer.iter().take(n).cloned().collect())
    }
    
    pub fn get_next(
        &mut self,
        buffer: &mut Buffer,
        n: usize,
    ) -> Result<HashMap<DeviceRoute, Vec<Sample>>, tio::proxy::RpcError> {
        loop {
            if let Some(result) = self.try_next(buffer, n)? {
                return Ok(result);
            }
            
            buffer.drain_tree()?;
        }
    }
    
    pub fn try_next(
        &mut self,
        buffer: &mut Buffer,
        n: usize,
    ) -> Result<Option<HashMap<DeviceRoute, Vec<Sample>>>, tio::proxy::RpcError> {
        buffer.drain_tree()?;
        
        let mut result = HashMap::new();
        
        for route in &self.routes {
            match self.get_samples_from_cursor(buffer, route, n) {
                Some(samples) => {
                    result.insert(route.clone(), samples);
                }
                None => {
                    return Ok(None);
                }
            }
        }
        
        for route in &self.routes {
            *self.cursors.get_mut(route).unwrap() += n;
        }
        
        Ok(Some(result))
    }
    
    pub fn get_last(
        &mut self,
        buffer: &mut Buffer,
        n: usize,
    ) -> Result<HashMap<DeviceRoute, Vec<Sample>>, tio::proxy::RpcError> {
        loop {
            if let Some(result) = self.try_last(buffer, n)? {
                return Ok(result);
            }
            
            buffer.drain_tree()?;
        }
    }
    
    pub fn try_last(
        &mut self,
        buffer: &mut Buffer,
        n: usize,
    ) -> Result<Option<HashMap<DeviceRoute, Vec<Sample>>>, tio::proxy::RpcError> {
        buffer.drain_tree()?;
        
        let t = match self.compute_t(buffer) {
            Some(t) => t,
            None => return Ok(None),
        };
        
        let mut result = HashMap::new();
        
        for route in &self.routes {
            let route_map = buffer.route_buffers.get(route)
                .ok_or(tio::proxy::RpcError::TypeError)?;
            
            let newest_session_id = self.get_newest_session_id(buffer, route)
                .ok_or(tio::proxy::RpcError::TypeError)?;
            
            let session_buffer = route_map.get(&newest_session_id)
                .ok_or(tio::proxy::RpcError::TypeError)?;
            
            let valid_samples: Vec<Sample> = session_buffer
                .iter()
                .filter(|s| s.timestamp_end() <= t)
                .cloned()
                .collect();
            
            if valid_samples.len() < n {
                return Ok(None);
            }
            
            let last_n = valid_samples[valid_samples.len() - n..].to_vec();
            result.insert(route.clone(), last_n);
        }
        
        Ok(Some(result))
    }
}