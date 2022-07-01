use super::{RecvError, SendError};
use std::io; // TODO: can we do without these?

const IOBUF_SIZE: usize = 4096;

// TODO: interface without these being public?
pub struct IOBuf {
    buf: [u8; IOBUF_SIZE],
    start: usize,
    end: usize,
}

impl IOBuf {
    pub fn new() -> IOBuf {
        IOBuf {
            buf: [0; IOBUF_SIZE],
            start: 0,
            end: 0,
        }
    }

    pub fn empty(&self) -> bool {
        self.start == self.end
    }

    pub fn data(&self) -> &[u8] {
        &self.buf[self.start..self.end]
    }

    pub fn consume(&mut self, len: usize) {
        if len > (self.end - self.start) {
            panic!("Invalid consume for more data that is contained");
        }
        self.start += len;
    }

    fn compact(&mut self) {
        if self.start != 0 {
            let len = self.end - self.start;
            self.buf.copy_within(self.start..self.end, 0);
            self.start = 0;
            self.end = len;
        }
    }

    pub fn refill<T: io::Read>(&mut self, reader: &mut T) -> Result<(), RecvError> {
        self.compact();
        match reader.read(&mut self.buf[self.end..]) {
            Ok(size) => {
                if size > 0 {
                    // TODO: how does this work with windows timing out?
                    // check that it errs below and change this code accordingly.
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

    pub fn drain<T: io::Write>(&mut self, writer: &mut T) -> Result<(), SendError> {
        if self.end > self.start {
            match writer.write(&self.buf[self.start..self.end]) {
                Ok(size) => {
                    self.start += size;
                    if self.start == self.end {
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
