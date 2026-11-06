//! The [`LogReader`] that `tio log dump`, `csv` and `hdf` read logs with, in
//! one pass over the files.

use super::progress::ByteProgress;
use std::collections::HashSet;
use std::convert::Infallible;
use twinleaf::data::{LogFile, PacketOutcome, PacketParser, SampleBatch};
use twinleaf::tio::{packet::Payload, Packet};
use twinleaf::DeviceRoute;

/// Log files read in order, each as far as it decodes.
pub(super) struct LogReader {
    logs: Vec<(String, LogFile)>,
    progress: bool,
}

/// What a read found besides the packets and batches it handed on.
pub(super) struct Read {
    /// Whether some log stopped at a packet that would not decode or apply.
    pub(super) truncated: bool,
    /// The routes whose stream data waited for metadata that did not arrive,
    /// in route order.
    pub(super) missing_metadata: Vec<DeviceRoute>,
}

impl LogReader {
    /// Opens every log at `paths`, before any is read.
    pub(super) fn open(paths: &[String]) -> eyre::Result<LogReader> {
        use eyre::WrapErr;

        let logs = paths
            .iter()
            .map(|path| {
                LogFile::open(path)
                    .map(|log| (path.clone(), log))
                    .wrap_err_with(|| format!("could not open {path}"))
            })
            .collect::<eyre::Result<_>>()?;
        Ok(LogReader {
            logs,
            progress: false,
        })
    }

    /// Makes the reader show a progress bar over each log while it reads it.
    pub(super) fn with_progress(self) -> LogReader {
        LogReader {
            progress: true,
            ..self
        }
    }

    /// Returns the size of every log together, in bytes.
    #[cfg(feature = "hdf5")]
    pub(super) fn total_bytes(&self) -> u64 {
        self.logs.iter().map(|(_, log)| log.len() as u64).sum()
    }

    /// Hands `visit` every packet of every log in order. A log stops at its
    /// first packet that does not decode, with a warning, and the next starts.
    pub(super) fn packets(&self, mut visit: impl FnMut(&Packet)) {
        let Ok(_) = self.each_packet::<Infallible>(|_, _, packet| {
            visit(packet);
            Ok(true)
        });
    }

    /// Hands `visit` every batch that `parser` decodes from the logs, read in
    /// order through the one parser. Metadata and continuity carry from one
    /// log into the next. Stream data that `decode` refuses is skipped before
    /// the parser. A log stops, with a warning, at its first packet that does
    /// not decode or that the parser rejects.
    pub(super) fn batches(
        &self,
        mut parser: PacketParser,
        decode: impl Fn(&Packet) -> bool,
        mut visit: impl FnMut(SampleBatch) -> eyre::Result<()>,
    ) -> eyre::Result<Read> {
        let mut parsed = HashSet::new();
        let mut waiting = HashSet::new();
        let truncated = self.each_packet::<eyre::Report>(|path, offset, packet| {
            if matches!(packet.payload(), Payload::Samples(_)) && !decode(packet) {
                return Ok(true);
            }
            match parser.push_packet(packet) {
                Ok(PacketOutcome::Rows(rows)) if rows > 0 => {
                    parsed.insert(packet.route());
                }
                Ok(PacketOutcome::WaitingForMetadata) => {
                    waiting.insert(packet.route());
                }
                Ok(_) => {}
                Err(error) => {
                    log::warn!("{path}: invalid data at byte offset {offset}: {error}; stopping");
                    return Ok(false);
                }
            }
            while let Some(batch) = parser.pop_batch() {
                visit(batch)?;
            }
            Ok(true)
        })?;
        for batch in parser.finish() {
            visit(batch)?;
        }
        let mut missing_metadata: Vec<_> = waiting.difference(&parsed).copied().collect();
        missing_metadata.sort();
        Ok(Read {
            truncated,
            missing_metadata,
        })
    }

    /// Hands `visit` each packet of each log with the log's path and the
    /// packet's offset in it, until `visit` returns `false` for that log.
    /// Returns `true` if any log stopped short of its end.
    fn each_packet<E>(
        &self,
        mut visit: impl FnMut(&str, usize, &Packet) -> Result<bool, E>,
    ) -> Result<bool, E> {
        let mut truncated = false;
        for (path, log) in &self.logs {
            let mut progress = self.progress.then(|| ByteProgress::new(log.len() as u64));
            let mut packets = log.packets();
            loop {
                let offset = packets.position();
                let packet = match packets.next() {
                    Some(Ok(packet)) => packet,
                    Some(Err(error)) => {
                        log::warn!("{path}: {error}; stopping");
                        truncated = true;
                        break;
                    }
                    None => break,
                };
                if let Some(progress) = &mut progress {
                    progress.update(packets.position() as u64);
                }
                if !visit(path, offset, &packet)? {
                    truncated = true;
                    break;
                }
            }
            if let Some(progress) = progress {
                progress.finish_and_clear(packets.position() as u64);
            }
        }
        Ok(truncated)
    }
}
