//! Reader
//! Takes keys (e.g., `DeviceKey`, `StreamKey`, `ColumnKey`) and tracks a uniform cursor from a `Buffer`
//! - Derives `StreamKey` from `(DeviceRoute, StreamId)`
//!
//! Has atomic reference to a `Buffer` using Arc<RwLock<Buffer>>
//!
//! Anchors:
//! - T: cursor from `Buffer`, given by min(latest_timestamp) across the `Reader` (route,stream) keys.
//! - C: cursor from `Reader` (advanced only by successful `next` reads).

use std::{
    collections::HashMap,
    sync::{Arc, RwLock},
    time::Duration,
};

use crate::device::{ColumnSpec, CursorPosition, StreamKey};
use crate::data::{AlignedWindow, Buffer, ReadError}; 

pub struct Reader {
    buffer: Arc<RwLock<Buffer>>,
    columns: Vec<ColumnSpec>,
    cursor: ReaderCursor,
}

struct ReaderCursor {
    positions: HashMap<StreamKey, CursorPosition>,
}

impl ReaderCursor {
    fn new() -> Self {
        Self {
            positions: HashMap::new(),
        }
    }
}

impl Reader {
    pub fn new(buffer: Arc<RwLock<Buffer>>, columns: Vec<ColumnSpec>) -> Self {
        Self {
            buffer,
            columns,
            cursor: ReaderCursor::new(),
        }
    }

    pub fn next(&mut self, n_samples: usize) -> Result<AlignedWindow, ReadError> {
        loop {
            let buffer = self.buffer.read().unwrap();
            match self.try_read_from_cursor(&buffer, n_samples) {
                Ok(window) => {
                    drop(buffer);
                    self.advance_cursor(&window);
                    return Ok(window);
                }
                Err(ReadError::InsufficientData { .. }) => {
                    drop(buffer);
                    std::thread::sleep(Duration::from_millis(1)); // didn't do: find actual blocking implementaiton
                    continue;
                }
                Err(e) => return Err(e),
            }
        }
    }

    pub fn try_next(&mut self, n_samples: usize) -> Result<Option<AlignedWindow>, ReadError> {
        let buffer = self.buffer.read().unwrap();
        match self.try_read_from_cursor(&buffer, n_samples) {
            Ok(window) => {
                drop(buffer);
                self.advance_cursor(&window);
                Ok(Some(window))
            }
            Err(ReadError::InsufficientData { .. }) => Ok(None),
            Err(e) => Err(e),
        }
    }

    fn try_read_from_cursor(
        &self,
        buffer: &Buffer,
        n_samples: usize,
    ) -> Result<AlignedWindow, ReadError> {
        if self.cursor.positions.is_empty() {
            return buffer.read_aligned_window(&self.columns, n_samples);
        }

        buffer.read_from_cursor(&self.columns, &self.cursor.positions, n_samples)
    }

    fn advance_cursor(&mut self, window: &AlignedWindow) {
        let Some(&last_sample_number) = window.sample_numbers.last() else {
            return;
        };

        for (stream_key, segment_meta) in &window.segment_metadata {
            let session_id = *window.session_ids.get(stream_key).unwrap();
            let segment_id = segment_meta.segment_id;

            self.cursor.positions.insert(
                stream_key.clone(),
                CursorPosition {
                    session_id,
                    segment_id,
                    last_sample_number,
                },
            );
        }
    }
}
