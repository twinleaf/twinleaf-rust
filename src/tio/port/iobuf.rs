//! Internal buffer for stream-oriented ports.

use super::{RecvError, SendError};
use std::io;

/// Size of the internal buffer.
const IOBUF_SIZE: usize = 4096;

/// Buffer used internally by ports with an underlying byte stream
/// to implement packetization for both reception and transmission.
pub struct IOBuf {
    /// Internal buffer. Valid data (possibly none) is
    /// in a slice delimited by `start` and `end`.
    buf: [u8; IOBUF_SIZE],
    /// Start offset of valid data in `buf`.
    start: usize,
    /// End offset of valid data in `buf`.
    end: usize,
}

impl IOBuf {
    /// Returns an empty `IOBuf`.
    pub fn new() -> IOBuf {
        IOBuf {
            buf: [0; IOBUF_SIZE],
            start: 0,
            end: 0,
        }
    }

    /// Returns whether or not this `IOBuf` is empty.
    pub fn empty(&self) -> bool {
        self.start == self.end
    }

    /// Returns the amount of data contained in this buffer, in bytes.
    pub fn size(&self) -> usize {
        self.end - self.start
    }

    /// Returns the data contained in this buffer.
    pub fn data(&self) -> &[u8] {
        &self.buf[self.start..self.end]
    }

    /// Discards the given amount of bytes off the beginning of the contained data.
    /// `len` must be at most the value returned by `size()`.
    pub fn consume(&mut self, len: usize) {
        if len > self.size() {
            panic!("Invalid consume for more data that is contained");
        }
        self.start += len;
    }

    /// Discard the entire content of the buffer.
    pub fn flush(&mut self) {
        self.start = 0;
        self.end = 0;
    }

    /// Moves the data internally to the start of the buffer.
    fn compact(&mut self) {
        if self.start != 0 {
            let len = self.size();
            self.buf.copy_within(self.start..self.end, 0);
            self.start = 0;
            self.end = len;
        }
    }

    /// Refills the buffer as much as possible from an object implementing `io::Read`.
    pub fn refill<T: io::Read>(&mut self, reader: &mut T) -> Result<(), RecvError> {
        self.compact();
        match reader.read(&mut self.buf[self.end..]) {
            Ok(size) => {
                if size > 0 {
                    self.end += size;
                    Ok(())
                } else {
                    Err(RecvError::Disconnected)
                }
            }
            Err(e) => {
                if e.kind() == io::ErrorKind::WouldBlock {
                    Err(RecvError::NotReady)
                } else {
                    Err(RecvError::IO(e))
                }
            }
        }
    }

    /// Appends as much of the given data to the existing data. Returns `Ok` if all the data
    /// was appended, otherwise an `Err` with the number of bytes that were successfully appended.
    pub fn add_data(&mut self, data: &[u8]) -> Result<(), usize> {
        self.compact();
        let copy_size = std::cmp::min(IOBUF_SIZE - self.end, data.len());
        self.buf[self.end..self.end + copy_size].copy_from_slice(&data[0..copy_size]);
        if copy_size == data.len() {
            Ok(())
        } else {
            Err(copy_size)
        }
    }

    /// Sends as much of the contained data as possible to an object implementing `io::Write`.
    pub fn drain<T: io::Write>(&mut self, writer: &mut T) -> Result<(), SendError> {
        if self.end > self.start {
            match writer.write(&self.buf[self.start..self.end]) {
                Ok(size) => {
                    self.consume(size);
                    if self.empty() {
                        Ok(())
                    } else {
                        Err(SendError::MustDrain)
                    }
                }
                Err(e) => {
                    if e.kind() == io::ErrorKind::WouldBlock {
                        Err(SendError::MustDrain)
                    } else {
                        Err(SendError::IO(e))
                    }
                }
            }
        } else {
            Ok(())
        }
    }
}
