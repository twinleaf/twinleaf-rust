//! The signal model, which is what an edge of the graph carries.

use crate::error::BuildError;
use std::collections::{HashMap, HashSet};
use std::fmt;
use std::sync::Arc;
use twinleaf::proto::sync::Epoch;
use twinleaf::proto::SessionId;

/// Names a rate domain within one graph.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct DomainId(pub(crate) u32);

/// Continuity token. A change means "reset all downstream state", the same
/// contract a bumped `Generations::stream` carries in the twinleaf library.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct RunId(pub u64);

/// A rate domain, which is one timescale, one rate and one sample grid.
///
/// Signals in one domain have coincident sample instants, so combining them is
/// index arithmetic. Nothing crosses from one domain to another implicitly.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Domain {
    pub id: DomainId,
    /// The timescale, which is unix, systime, zero or invalid.
    pub epoch: Epoch,
    /// The timebase instance the epoch was reported for.
    pub session: SessionId,
    /// Samples per second.
    pub rate: f64,
    /// Time of sample 0, on `epoch`.
    pub origin: f64,
}

impl Domain {
    /// Returns the time of sample `index` on the domain's epoch.
    pub fn time_of(&self, index: u64) -> f64 {
        self.origin + index as f64 / self.rate
    }
}

impl fmt::Display for Domain {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{} Hz, {} time, session {}",
            self.rate, self.epoch, self.session
        )
    }
}

/// The rate domains of one graph.
#[derive(Debug, Clone, Default)]
pub struct Domains {
    list: Vec<Domain>,
    /// Domains derived by a rate change, by parent and ratio, so that two
    /// nodes that change the rate alike land in one domain.
    scaled: HashMap<(DomainId, u64, u64), DomainId>,
}

impl Domains {
    pub fn get(&self, id: DomainId) -> Option<&Domain> {
        self.list.get(id.0 as usize)
    }

    /// Adds a domain that shares its sample grid with no other.
    pub fn add(
        &mut self,
        epoch: Epoch,
        session: SessionId,
        rate: f64,
        origin: f64,
    ) -> Result<DomainId, BuildError> {
        if !(rate.is_finite() && rate > 0.0) {
            return Err(BuildError::InvalidRate { rate });
        }
        let id = DomainId(self.list.len() as u32);
        self.list.push(Domain {
            id,
            epoch,
            session,
            rate,
            origin,
        });
        Ok(id)
    }

    /// Returns the domain `up / down` times as fast as `from` on the same
    /// clock, in which sample `k` falls at `from`'s sample `k * down / up`.
    /// It is made the first time it is asked for.
    pub fn scaled(&mut self, from: DomainId, up: u64, down: u64) -> Result<DomainId, BuildError> {
        let divisor = gcd(up, down);
        if divisor == 0 || up == 0 || down == 0 {
            return Err(BuildError::InvalidRate { rate: 0.0 });
        }
        let (up, down) = (up / divisor, down / divisor);
        if (up, down) == (1, 1) {
            return Ok(from);
        }
        if let Some(&known) = self.scaled.get(&(from, up, down)) {
            return Ok(known);
        }
        let parent = *self.get(from).ok_or(BuildError::UnknownDomain)?;
        let rate = parent.rate * up as f64 / down as f64;
        let id = self.add(parent.epoch, parent.session, rate, parent.origin)?;
        self.scaled.insert((from, up, down), id);
        Ok(id)
    }
}

pub(crate) fn gcd(a: u64, b: u64) -> u64 {
    if b == 0 {
        a
    } else {
        gcd(b, a % b)
    }
}

/// A unit symbol such as `nT`, compared by spelling. The empty symbol means no
/// unit was declared.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Default)]
pub struct Unit(String);

impl Unit {
    /// Creates a unit from its symbol.
    pub fn new(symbol: impl Into<String>) -> Self {
        Self(symbol.into())
    }

    /// Returns the unit of a signal that declares none.
    pub fn none() -> Self {
        Self::default()
    }

    /// Returns whether no unit was declared.
    pub fn is_none(&self) -> bool {
        self.0.is_empty()
    }

    /// Returns the symbol, empty when no unit was declared.
    pub fn symbol(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for Unit {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.0.is_empty() {
            f.write_str("no unit")
        } else {
            f.write_str(&self.0)
        }
    }
}

/// One channel of a signal.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Channel {
    /// The name channels are matched by, such as `x` or `a.x`.
    pub name: String,
    pub unit: Unit,
}

/// The channels of one edge, in storage order. Never empty, and names are
/// unique, because channels are matched by name and never by position.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChannelSet {
    channels: Vec<Channel>,
}

impl ChannelSet {
    /// Creates a channel set, refusing an empty one and repeated names.
    pub fn new(channels: Vec<Channel>) -> Result<Arc<Self>, BuildError> {
        if channels.is_empty() {
            return Err(BuildError::NoChannels);
        }
        let mut seen = HashSet::with_capacity(channels.len());
        for channel in &channels {
            if !seen.insert(channel.name.as_str()) {
                return Err(BuildError::DuplicateChannel {
                    name: channel.name.clone(),
                });
            }
        }
        Ok(Arc::new(Self { channels }))
    }

    /// Creates a channel set whose channels all share one unit.
    pub fn uniform<'a>(
        names: impl IntoIterator<Item = &'a str>,
        unit: &Unit,
    ) -> Result<Arc<Self>, BuildError> {
        Self::new(
            names
                .into_iter()
                .map(|name| Channel {
                    name: name.to_owned(),
                    unit: unit.clone(),
                })
                .collect(),
        )
    }

    /// Returns the number of channels, which is at least one.
    #[allow(clippy::len_without_is_empty)]
    pub fn len(&self) -> usize {
        self.channels.len()
    }

    /// Returns the channels in storage order.
    pub fn iter(&self) -> impl Iterator<Item = &Channel> {
        self.channels.iter()
    }

    /// Returns the channel stored at `index`.
    pub fn get(&self, index: usize) -> Option<&Channel> {
        self.channels.get(index)
    }

    /// Returns where the channel called `name` is stored.
    pub fn position(&self, name: &str) -> Option<usize> {
        self.channels.iter().position(|c| c.name == name)
    }
}

/// Static type of an edge, resolved when the graph is built.
#[derive(Debug, Clone, PartialEq)]
pub struct SignalType {
    pub domain: DomainId,
    pub channels: Arc<ChannelSet>,
}

/// A run of consecutive samples in a domain's global numbering.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Span {
    /// Index of the first sample.
    pub first: u64,
    pub frames: usize,
}

/// A contiguous run of uniformly sampled, multi-channel data.
#[derive(Debug, Clone, PartialEq)]
pub struct Block {
    /// Which rate domain these samples live in.
    pub domain: DomainId,
    pub run: RunId,
    /// Index of frame 0 within the domain's global sample numbering. Two
    /// blocks in one domain align by this integer, with no float comparison.
    pub first: u64,
    pub frames: usize,
    pub channels: Arc<ChannelSet>,
    /// Channel-major, as `data[c * frames + i]`.
    pub data: Vec<f64>,
}

impl Block {
    /// Returns the samples of the channel stored at `index`.
    pub fn channel(&self, index: usize) -> &[f64] {
        &self.data[index * self.frames..(index + 1) * self.frames]
    }

    /// Returns the samples this block covers.
    pub fn span(&self) -> Span {
        Span {
            first: self.first,
            frames: self.frames,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_channel_set_refuses_repeated_names_and_no_names() {
        let unit = Unit::new("nT");
        assert_eq!(
            ChannelSet::uniform(["x", "y", "x"], &unit),
            Err(BuildError::DuplicateChannel { name: "x".into() })
        );
        assert_eq!(ChannelSet::new(Vec::new()), Err(BuildError::NoChannels));
        let set = ChannelSet::uniform(["x", "y"], &unit).unwrap();
        assert_eq!(set.position("y"), Some(1));
        assert_eq!(set.position("z"), None);
    }
}
