//! What a receiver keeps of one stream, so that a batch it gets after
//! batches it did not get says what those changed and lost.

use crate::model::continuity::{Boundary, Change, Loss, Lost};
use crate::model::sample::SampleBatch;
use crate::model::schema::StreamMetadataSnapshot;
use crate::proto::SampleNumber;

/// What one receiver keeps of one stream, so that a batch it gets after
/// batches it did not get says what those changed and lost.
///
/// It keeps the last batch the receiver got, and what the batches it missed
/// since then changed and lost. A batch after misses names the higher ranked
/// of their change and its own, with the `old` values of the last batch
/// received and the `new` values of its own, and a [`Loss`] that counts every
/// row missed. Any other batch goes on as the parser made it. The first
/// batch of the stream the receiver gets says [`Change::Initial`], as its
/// reader starts there, with a [`Loss`] that names no sample due when the
/// receiver missed the batches before it. Each batch keeps the generations
/// the parser gave it.
#[derive(Debug, Default)]
pub struct Carried {
    /// The metadata and last sample number of the last batch received.
    last: Option<(StreamMetadataSnapshot, SampleNumber)>,
    /// What the batches missed since then changed and lost.
    pending: Option<Pending>,
}

/// What the batches of one stream that a receiver missed changed and lost.
#[derive(Debug)]
struct Pending {
    change: Option<Change>,
    /// The batches missed and their rows, and the rows those batches said
    /// were lost before them.
    dropped: u64,
    rows: u64,
    lost: Lost,
}

impl Carried {
    /// Keeps what `batch`, which the receiver does not get, changed and lost.
    pub fn miss(&mut self, batch: &SampleBatch) {
        let pending = self.pending.get_or_insert(Pending {
            change: None,
            dropped: 0,
            rows: 0,
            lost: Lost::default(),
        });
        pending.dropped += 1;
        pending.rows += batch.len() as u64;
        if let Some(loss) = batch.boundary().and_then(|b| b.loss.as_ref()) {
            pending.lost.add(loss);
        }
        let change = batch.boundary().and_then(|b| b.change.clone());
        pending.change = higher(pending.change.take(), change);
    }

    /// Returns `batch` as the receiver gets it, and keeps it as the last one.
    pub fn queue(&mut self, batch: SampleBatch) -> SampleBatch {
        let batch = match (self.pending.take(), &self.last) {
            (None, Some(_)) => batch,
            (None, None) if batch.is_initial() => batch,
            (None, None) => batch.with_boundary(Some(Boundary::of(Change::Initial))),
            (Some(pending), last) => {
                let boundary = pending.onto(&batch, last.as_ref());
                batch.with_boundary(Some(boundary))
            }
        };
        if let Some(&n) = batch.sample_numbers().last() {
            self.last = Some((batch.metadata().clone(), n));
        }
        batch
    }
}

impl Pending {
    /// Returns the boundary of `batch`, the first one received after these
    /// misses, where `last` is the metadata and last sample number of the
    /// batch received before them.
    fn onto(
        self,
        batch: &SampleBatch,
        last: Option<&(StreamMetadataSnapshot, SampleNumber)>,
    ) -> Boundary {
        let own = batch.boundary().and_then(|b| b.change.clone());
        let change = match last {
            Some((metadata, _)) => {
                higher(self.change, own).map(|change| restate(change, metadata, batch.metadata()))
            }
            None => Some(Change::Initial),
        };
        let expected = last
            .filter(|_| change.as_ref().is_none_or(Change::is_continuous))
            .map(|&(_, n)| SampleNumber::new(n.value() + 1));
        let mut lost = self.lost;
        if let Some(loss) = batch.boundary().and_then(|b| b.loss.as_ref()) {
            lost.add(loss);
        }
        Boundary {
            change,
            loss: Some(Loss {
                samples: self.rows + lost.total(),
                expected,
                dropped_batches: self.dropped,
                dropped_samples: self.rows + lost.dropped,
                link_samples: lost.link,
                proxy_samples: lost.proxy,
            }),
        }
    }
}

/// Returns the higher ranked of two changes a reader learns of at once. Two
/// backward steps in time add up.
fn higher(first: Option<Change>, then: Option<Change>) -> Option<Change> {
    let (first, then) = match (first, then) {
        (Some(first), Some(then)) => (first, then),
        (first, then) => return first.or(then),
    };
    Some(match (first, then) {
        (Change::TimeBackward { gap_seconds: a }, Change::TimeBackward { gap_seconds: b }) => {
            Change::TimeBackward { gap_seconds: a + b }
        }
        (first, then) if then.rank() > first.rank() => then,
        (first, _) => first,
    })
}

/// Returns `change` naming the values of `old`, the metadata of the last
/// batch received, and `new`, that of the batch it goes on.
fn restate(change: Change, old: &StreamMetadataSnapshot, new: &StreamMetadataSnapshot) -> Change {
    match change {
        Change::SessionChanged { .. } => Change::SessionChanged {
            old: old.device().session(),
            new: new.device().session(),
        },
        Change::TimeRefSessionChanged { .. } => Change::TimeRefSessionChanged {
            old: old.segment().timeref_session(),
            new: new.segment().timeref_session(),
        },
        Change::RateChanged { .. } => Change::RateChanged {
            old_rate: old.rate_hz(),
            new_rate: new.rate_hz(),
        },
        Change::Restarted { .. } => Change::Restarted {
            old_id: old.segment().segment_id(),
            new_id: new.segment().segment_id(),
        },
        Change::SegmentRollover { .. } => Change::SegmentRollover {
            old_id: old.segment().segment_id(),
            new_id: new.segment().segment_id(),
        },
        change @ (Change::Initial | Change::TimeBackward { .. }) => change,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fixtures;
    use crate::model::continuity::{BoundaryClass, Generations};
    use crate::model::sample::{BatchContext, ColumnData, SampleBatchBuilder};
    use crate::proto::data as wire;
    use crate::proto::data::DataType;
    use crate::proto::{DeviceRoute, SessionId};

    /// A batch of one row, sample `n`, of the one stream of the root device
    /// in `session`, with `boundary`.
    fn batch(session: u32, n: u32, boundary: Option<Boundary>) -> SampleBatch {
        let metadata = fixtures::metadata(
            DeviceRoute::root(),
            wire::Device {
                session: SessionId::new(session),
                ..fixtures::device()
            },
            fixtures::stream(1),
            fixtures::segment(1),
            &[fixtures::column(1, 0, DataType::F32)],
        );
        let context = BatchContext {
            metadata,
            boundary,
            generations: Generations {
                stream: 1,
                device: 0,
                global: 0,
            },
        };
        let mut builder = SampleBatchBuilder::new(context, 1);
        builder.push_row(SampleNumber::new(n), [ColumnData::F32(0.0)]);
        builder.finish()
    }

    /// The first batch of the stream, as its parser starts it.
    fn first() -> SampleBatch {
        batch(1, 0, Some(Boundary::of(Change::Initial)))
    }

    /// The boundary of a batch whose parser counted `loss`.
    fn lost(loss: Loss) -> Option<Boundary> {
        Some(fixtures::boundary(None, Some(loss)))
    }

    /// What a missed batch said was lost before it, and where, reaches the
    /// next batch received with the rows of the missed batch and what that
    /// batch says it lost itself. Each row counts in one place.
    #[test]
    fn a_batch_after_a_miss_counts_where_the_rows_before_the_missed_batch_went() {
        let mut carried = Carried::default();
        carried.queue(first());
        // The proxy dropped two rows before the missed batch.
        let due = Some(SampleNumber::new(1));
        carried.miss(&batch(1, 3, lost(fixtures::loss(2, due, 0, 0, 0, 2))));
        // The device skipped one row before the batch received.
        let skipped = fixtures::loss(1, Some(SampleNumber::new(4)), 0, 0, 0, 0);
        let received = carried.queue(batch(1, 5, lost(skipped)));

        let loss = received.boundary().and_then(|b| b.loss.clone());
        assert_eq!(loss, Some(fixtures::loss(4, due, 1, 1, 0, 2)));
        assert_eq!(loss.map(|loss| loss.skipped_samples()), Some(1));
    }

    /// The change of a missed batch reaches the next batch received, naming
    /// the session of the batch received before. A loss next to a reboot
    /// names no sample due.
    #[test]
    fn a_batch_after_a_miss_names_the_change_of_the_missed_batch() {
        let mut carried = Carried::default();
        carried.queue(first());
        carried.queue(batch(1, 1, None));
        let rebooted = Change::SessionChanged {
            old: SessionId::new(1),
            new: SessionId::new(2),
        };
        carried.miss(&batch(2, 0, Some(Boundary::of(rebooted.clone()))));
        let received = carried.queue(batch(2, 1, None));

        let boundary = received.boundary().expect("the missed reboot");
        assert_eq!(boundary.change, Some(rebooted));
        let loss = boundary.loss.as_ref().expect("the missed batch");
        assert_eq!((loss.expected, loss.dropped_batches), (None, 1));
    }

    /// A receiver that missed the first batches of a stream starts it at the
    /// first batch it gets, with the loss, which names no sample due.
    #[test]
    fn a_receiver_that_missed_a_streams_start_starts_it_with_the_loss() {
        let mut carried = Carried::default();
        carried.miss(&first());
        let received = carried.queue(batch(1, 1, None));

        let boundary = received.boundary().expect("the start of the stream");
        assert_eq!(boundary.change, Some(Change::Initial));
        assert_eq!(boundary.class(), BoundaryClass::Startup);
        assert_eq!(boundary.loss, Some(fixtures::loss(1, None, 1, 1, 0, 0)));
    }

    /// The backward steps in time of the missed batches add up on the next
    /// batch received.
    #[test]
    fn the_time_jumps_of_missed_batches_add_up() {
        let mut carried = Carried::default();
        carried.queue(first());
        carried.queue(batch(1, 1, None));
        for (n, gap_seconds) in [(2, 5.0), (3, 2.0)] {
            let jumped = Change::TimeBackward { gap_seconds };
            carried.miss(&batch(1, n, Some(Boundary::of(jumped))));
        }
        let received = carried.queue(batch(1, 4, None));

        let change = received.boundary().and_then(|b| b.change.clone());
        assert_eq!(change, Some(Change::TimeBackward { gap_seconds: 7.0 }));
    }

    /// A receiver that joins a stream after its start says the first batch
    /// it gets starts the stream, and the next goes on as its parser made it.
    #[test]
    fn a_receiver_that_joins_late_starts_the_stream() {
        let mut carried = Carried::default();
        let boundaries = [batch(1, 1, None), batch(1, 2, None)]
            .map(|batch| carried.queue(batch).boundary().cloned());

        let initial = fixtures::boundary(Some(Change::Initial), None);
        assert_eq!(boundaries, [Some(initial), None]);
    }
}
