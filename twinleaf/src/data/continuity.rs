//! The continuity the parser reports with every batch.
//!
//! [`BoundaryReason`] says why a stream started or changed, and
//! [`BoundaryClass`] groups the reasons. [`Generations`] number the runs that
//! boundaries separate.

use crate::proto::{SampleNumber, SegmentId, SessionId};

/// Parser-assigned continuity generations, constant within a batch.
///
/// The parser stamps them in arrival order. Comparing them tells runs apart
/// without re-reading boundaries.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Generations {
    /// Per-stream run ordinal. It bumps at every non-continuous boundary.
    pub stream: u32,
    /// Per-device generation. It bumps at any non-continuous boundary on the
    /// device's streams other than [`BoundaryReason::Initial`], and when the
    /// device's state resets.
    pub device: u32,
    /// Whole-parse generation. It bumps as `device` does, across all routed
    /// devices.
    pub global: u32,
}

/// What a boundary says about the data around it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BoundaryClass {
    /// The first data observed for a stream.
    Startup,
    /// The stream continues without a gap.
    Seamless,
    /// Samples are missing, but time remains monotonic.
    DataLoss,
    /// The session, time reference, rate, or segment changed.
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

/// Why a stream started or changed.
///
/// Attached to the first batch affected. These methods classify it.
///
/// - [`class`](Self::class) categorizes the cause.
/// - [`is_continuous`](Self::is_continuous) checks for an unbroken timeline.
/// - [`is_monotonic`](Self::is_monotonic) checks that time did not move backward.
///
/// | Reason                  | [`class`](Self::class) | [`is_continuous`](Self::is_continuous) | [`is_monotonic`](Self::is_monotonic) |
/// |-------------------------|------------|-----|-----|
/// | `SegmentRollover`       | `Seamless` | yes | yes |
/// | `Initial`               | `Startup`  | no  | no  |
/// | `SamplesLost`           | `DataLoss` | no  | yes |
/// | `SessionChanged`        | `Reconfig` | no  | no  |
/// | `TimeRefSessionChanged` | `Reconfig` | no  | no  |
/// | `RateChanged`           | `Reconfig` | no  | yes |
/// | `SegmentChanged`        | `Reconfig` | no  | yes |
/// | `TimeForward`           | `Anomaly`  | no  | yes |
/// | `TimeBackward`          | `Anomaly`  | no  | no  |
///
/// Continuous boundaries are always monotonic. The class describes the cause,
/// not its effect on the timeline.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum BoundaryReason {
    /// The stream's first sample.
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
    /// Time jumped forward with no gap in sample numbers, for example after a
    /// segment's start time was corrected in place.
    TimeForward {
        /// How far time skipped ahead, in seconds.
        gap_seconds: f64,
    },
    /// The sampling rate changed.
    RateChanged {
        /// Rate the preceding rows were sampled at, in Hz after decimation.
        old_rate: f64,
        /// Rate now in force, in Hz after decimation.
        new_rate: f64,
    },

    /// The segment rolled over. The timeline continues in a new segment.
    SegmentRollover {
        /// Segment that ended.
        old_id: SegmentId,
        /// Segment the rows continue into.
        new_id: SegmentId,
    },
    /// The segment changed other than by a rollover.
    SegmentChanged {
        /// Segment the stream had been in.
        old_id: SegmentId,
        /// Segment the rows arrived in.
        new_id: SegmentId,
    },
    /// Samples were lost, leaving a gap in sample numbers.
    SamplesLost {
        /// Sample number the first row should have carried.
        expected: SampleNumber,
        /// Sample number the first row actually carried.
        received: SampleNumber,
    },
}

impl BoundaryReason {
    /// Returns `true` only for a segment rollover, the one boundary that
    /// preserves continuity. A continuous boundary is also monotonic.
    pub fn is_continuous(&self) -> bool {
        matches!(self, BoundaryReason::SegmentRollover { .. })
    }

    /// Returns `true` if time stays monotonic across the boundary, though rows
    /// may be missing.
    ///
    /// Every [`is_continuous`](Self::is_continuous) boundary is also
    /// monotonic.
    pub fn is_monotonic(&self) -> bool {
        matches!(
            self,
            BoundaryReason::SamplesLost { .. }
                | BoundaryReason::RateChanged { .. }
                | BoundaryReason::SegmentRollover { .. }
                | BoundaryReason::SegmentChanged { .. }
                | BoundaryReason::TimeForward { .. }
        )
    }

    /// Returns `true` only for the first data a stream has ever delivered.
    pub fn is_initial(&self) -> bool {
        matches!(self, BoundaryReason::Initial)
    }

    /// Returns the class of the boundary's cause, independent of continuity
    /// and monotonicity.
    pub fn class(&self) -> BoundaryClass {
        match self {
            BoundaryReason::SegmentRollover { .. } => BoundaryClass::Seamless,
            BoundaryReason::Initial => BoundaryClass::Startup,
            BoundaryReason::SamplesLost { .. } => BoundaryClass::DataLoss,
            BoundaryReason::SessionChanged { .. }
            | BoundaryReason::TimeRefSessionChanged { .. }
            | BoundaryReason::RateChanged { .. }
            | BoundaryReason::SegmentChanged { .. } => BoundaryClass::Reconfig,
            BoundaryReason::TimeForward { .. } | BoundaryReason::TimeBackward { .. } => {
                BoundaryClass::Anomaly
            }
        }
    }
}
