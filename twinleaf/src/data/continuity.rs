//! The continuity the parser reports with every batch.
//!
//! A [`Boundary`] says what changed ahead of a batch, as a [`Change`], and
//! what was lost there, as a [`Loss`]. [`BoundaryClass`] groups boundaries.
//! [`Generations`] number the runs that boundaries separate.

use crate::proto::{SampleNumber, SegmentId, SessionId};

/// Parser-assigned continuity generations, constant within a batch.
///
/// The parser stamps them in arrival order. Comparing them tells runs apart
/// without re-reading boundaries. A receiver's queue hands them on as the
/// parser assigned them, so a loss in that queue alone bumps none of them.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Generations {
    /// Per-stream run ordinal. It bumps at every boundary the parser finds
    /// that is not continuous.
    pub stream: u32,
    /// Per-device generation. It bumps at any boundary the parser finds on
    /// the device's streams that is not continuous, other than the
    /// [`Change::Initial`] of a stream's first data, and when the device's
    /// state resets.
    pub device: u32,
    /// Whole-parse generation. It bumps as `device` does, across all routed
    /// devices.
    pub global: u32,
}

/// What a boundary says about the data around it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BoundaryClass {
    /// The first data observed for a stream, or its first after it started
    /// over, as [`Change::Initial`] says.
    Startup,
    /// The stream continues without a gap.
    Seamless,
    /// Samples are missing, but time remains monotonic.
    DataLoss,
    /// The session, time reference or rate changed, or the device started
    /// acquiring again after a stop.
    Reconfig,
    /// The timeline is inconsistent, possibly from corrupted data or a device clock error.
    Anomaly,
}

impl BoundaryClass {
    /// Every class in declaration order, which is a stream's start and then
    /// the rest from the least disruptive to the most.
    pub const ALL: [BoundaryClass; 5] = [
        BoundaryClass::Startup,
        BoundaryClass::Seamless,
        BoundaryClass::DataLoss,
        BoundaryClass::Reconfig,
        BoundaryClass::Anomaly,
    ];
}

/// The class in lowercase words, such as `data loss`.
impl std::fmt::Display for BoundaryClass {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.pad(match self {
            BoundaryClass::Startup => "startup",
            BoundaryClass::Seamless => "seamless",
            BoundaryClass::DataLoss => "data loss",
            BoundaryClass::Reconfig => "reconfig",
            BoundaryClass::Anomaly => "anomaly",
        })
    }
}

/// What changed and what was lost between a receiver's previous batch of a
/// stream and this one.
///
/// A batch has no boundary exactly when nothing that its receiver can
/// detect changed or was lost since that receiver's previous batch of the
/// stream. A boundary names a change, a loss, or both. The `old` values of
/// its change come from the receiver's previous batch, and the `new` values
/// from this one. These methods classify it.
///
/// - [`class`](Self::class) categorizes the cause.
/// - [`is_continuous`](Self::is_continuous) checks for an unbroken timeline.
/// - [`is_monotonic`](Self::is_monotonic) checks that time did not move backward.
///
/// | Boundary                   | [`class`](Self::class) | [`is_continuous`](Self::is_continuous) | [`is_monotonic`](Self::is_monotonic) |
/// |----------------------------|----------------|-----|-----------------|
/// | change alone               | the change's   | the change's | the change's |
/// | loss alone                 | `DataLoss`     | no  | yes             |
/// | `SegmentRollover` and loss | `DataLoss`     | no  | yes             |
/// | another change and loss    | the change's   | no  | the change's    |
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub struct Boundary {
    /// What changed, or `None` when only samples were lost.
    pub change: Option<Change>,
    /// The samples lost, or `None` when none were lost, or when a change
    /// other than a rollover stands for the loss and the receiver's queue
    /// dropped no batch of the stream.
    pub loss: Option<Loss>,
}

impl Boundary {
    /// Returns a boundary that names `change` and no loss.
    pub(crate) fn of(change: Change) -> Boundary {
        Boundary {
            change: Some(change),
            loss: None,
        }
    }

    /// Returns the class of the boundary. It is the change's class, or
    /// [`BoundaryClass::DataLoss`] when the boundary names a loss and its
    /// change is none or a rollover.
    pub fn class(&self) -> BoundaryClass {
        match (&self.change, &self.loss) {
            (None, Some(_)) => BoundaryClass::DataLoss,
            (Some(change), Some(_)) if change.is_continuous() => BoundaryClass::DataLoss,
            (Some(change), _) => change.class(),
            (None, None) => BoundaryClass::Seamless,
        }
    }

    /// Returns `true` if the timeline continues unbroken, which is when no
    /// sample was lost and the change is none or a rollover. A continuous
    /// boundary is also monotonic.
    pub fn is_continuous(&self) -> bool {
        self.loss.is_none() && self.change.as_ref().is_none_or(Change::is_continuous)
    }

    /// Returns `true` if time stays monotonic across the boundary, though
    /// rows may be missing. A loss alone is monotonic.
    pub fn is_monotonic(&self) -> bool {
        self.change.as_ref().is_none_or(Change::is_monotonic)
    }

    /// Returns `true` for the first data a stream delivers to its receiver,
    /// and for its first after it started over, as [`Change::Initial`] says.
    pub fn is_initial(&self) -> bool {
        self.change.as_ref().is_some_and(Change::is_initial)
    }
}

/// Why a stream started or changed.
///
/// These methods classify it.
///
/// - [`class`](Self::class) categorizes the cause.
/// - [`is_continuous`](Self::is_continuous) checks for an unbroken timeline.
/// - [`is_monotonic`](Self::is_monotonic) checks that time did not move backward.
///
/// | Change                  | [`class`](Self::class) | [`is_continuous`](Self::is_continuous) | [`is_monotonic`](Self::is_monotonic) |
/// |-------------------------|------------|-----|-----|
/// | `SegmentRollover`       | `Seamless` | yes | yes |
/// | `Initial`               | `Startup`  | no  | no  |
/// | `SessionChanged`        | `Reconfig` | no  | no  |
/// | `TimeRefSessionChanged` | `Reconfig` | no  | no  |
/// | `RateChanged`           | `Reconfig` | no  | yes |
/// | `Restarted`             | `Reconfig` | no  | yes |
/// | `TimeBackward`          | `Anomaly`  | no  | no  |
///
/// Continuous changes are always monotonic. The class describes the cause,
/// not its effect on the timeline. Every change other than a rollover stands
/// for any loss next to it, so its boundary names no loss of the parser's.
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub enum Change {
    /// The stream's first sample, or its first after it started over. It
    /// starts over when another device answers at its route, when new
    /// firmware or a layout change comes with no new session, and when
    /// metadata contradicts what was learned. Nothing ties it to the samples
    /// before.
    Initial,
    /// The device session changed.
    SessionChanged {
        /// Session the preceding rows carried.
        old: SessionId,
        /// Session the device reports now.
        new: SessionId,
    },
    /// The time reference epoch changed.
    TimeRefSessionChanged {
        /// Time reference session the preceding rows carried.
        old: SessionId,
        /// Time reference session the segment names now.
        new: SessionId,
    },
    /// Time jumped backward unexpectedly.
    TimeBackward {
        /// How far time went back, in seconds, as a positive magnitude.
        gap_seconds: f64,
    },
    /// The sampling rate changed.
    RateChanged {
        /// Rate the preceding rows were sampled at, in Hz after decimation.
        old_rate: f64,
        /// Rate now in force, in Hz after decimation.
        new_rate: f64,
    },
    /// The device stopped acquiring and started again, and the stream went
    /// on in a segment of the new acquisition, in the same session, time
    /// reference and rate, with time not going backward. The device took no
    /// sample in the time between, so the change names no loss for it.
    ///
    /// Rows in another segment after the device's link was lost are told
    /// so too, as the parser cannot tell a restart while the link was down
    /// from a rollover. Samples may then have been lost, and the change does
    /// not count them.
    Restarted {
        /// Segment of the preceding rows.
        old_id: SegmentId,
        /// Segment of the new acquisition the rows go on in.
        new_id: SegmentId,
    },
    /// The stream went on into another segment in the same session, time
    /// reference and rate, with time not going backward. Segments between
    /// may have passed unseen.
    SegmentRollover {
        /// Segment of the preceding rows.
        old_id: SegmentId,
        /// Segment the rows continue into.
        new_id: SegmentId,
    },
}

impl Change {
    /// Returns `true` only for a segment rollover, the one change that
    /// preserves continuity. A continuous change is also monotonic.
    pub fn is_continuous(&self) -> bool {
        matches!(self, Change::SegmentRollover { .. })
    }

    /// Returns `true` if time stays monotonic across the change.
    ///
    /// Every [`is_continuous`](Self::is_continuous) change is also
    /// monotonic.
    pub fn is_monotonic(&self) -> bool {
        matches!(
            self,
            Change::RateChanged { .. } | Change::Restarted { .. } | Change::SegmentRollover { .. }
        )
    }

    /// Returns `true` for [`Change::Initial`].
    pub fn is_initial(&self) -> bool {
        matches!(self, Change::Initial)
    }

    /// Returns the class of the change's cause, independent of continuity
    /// and monotonicity.
    pub fn class(&self) -> BoundaryClass {
        match self {
            Change::SegmentRollover { .. } => BoundaryClass::Seamless,
            Change::Initial => BoundaryClass::Startup,
            Change::SessionChanged { .. }
            | Change::TimeRefSessionChanged { .. }
            | Change::RateChanged { .. }
            | Change::Restarted { .. } => BoundaryClass::Reconfig,
            Change::TimeBackward { .. } => BoundaryClass::Anomaly,
        }
    }

    /// Returns how much of what happened before it the change stands for.
    /// Of two changes a receiver learns of at once, it is told the higher.
    ///
    /// The ranks follow the order the parser tells changes in, so a receiver
    /// told of two at once learns what the parser tells of both at once. A
    /// restart ranks below a backward step in time, which it does not stand
    /// for, and above the rollover it stands for.
    pub(crate) fn rank(&self) -> u8 {
        match self {
            Change::Initial => 7,
            Change::SessionChanged { .. } => 6,
            Change::TimeRefSessionChanged { .. } => 5,
            Change::RateChanged { .. } => 4,
            Change::TimeBackward { .. } => 3,
            Change::Restarted { .. } => 2,
            Change::SegmentRollover { .. } => 1,
        }
    }
}

/// The rows of a stream that a receiver did not get, between its previous
/// batch of the stream and this one.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct Loss {
    /// The rows lost, never 0.
    ///
    /// Next to no change or a rollover, it counts the rows lost, plus up to
    /// one for each segment that passed unseen whose time ends inside a
    /// decimation group, as it counts the whole output periods of the
    /// unseen segments' time.
    ///
    /// Next to another change, it counts the rows of the batches the
    /// receiver's queue dropped and the losses they and this batch name,
    /// which has the same upper bound.
    pub samples: u64,
    /// The sample number after the receiver's previous row. It is `Some`
    /// exactly when the receiver had a previous batch of the stream and the
    /// change is none or a rollover.
    pub expected: Option<SampleNumber>,
    /// The batches of this stream that this receiver's queue dropped, and 0
    /// when only the device or the link lost samples. For each stream, these
    /// add up over its batches to that stream's share of the counts
    /// [`RecvError::Lagged`](crate::RecvError::Lagged) reports.
    pub dropped_batches: u64,
}
