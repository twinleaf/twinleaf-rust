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

use crate::tio::proto::identifiers::{ColumnKey, StreamKey, SampleNumber};
use crate::data::{AlignedWindow, Buffer, ReadError, RunId};

pub struct CursorPosition {
    pub run_id: RunId,
    pub last_sample_number: SampleNumber,
}

pub struct Reader {
    buffer: Arc<RwLock<Buffer>>,
    columns: Vec<ColumnKey>,
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

    fn clear(&mut self) {
        self.positions.clear();
    }
}

impl Reader {
    pub fn new(buffer: Arc<RwLock<Buffer>>, columns: Vec<ColumnKey>) -> Self {
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
                    std::thread::sleep(Duration::from_millis(1));
                    continue;
                }
                Err(ReadError::CursorInvalidated { .. }) => {
                    drop(buffer);
                    self.cursor.clear();
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
            Err(ReadError::CursorInvalidated { .. }) => {
                drop(buffer);
                self.cursor.clear();
                Ok(None)
            }
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

        for (stream_key, &run_id) in &window.run_ids {
            self.cursor.positions.insert(
                stream_key.clone(),
                CursorPosition {
                    run_id,
                    last_sample_number,
                },
            );
        }
    }
}