use super::{TioPktHdr, TIO_PACKET_MAX_ROUTING_SIZE};
use std::cmp::Ordering;
use std::fmt::{Display, Formatter};
use std::hash::{Hash, Hasher};

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum RouteError {
    #[error("route exceeds the maximum depth of {TIO_PACKET_MAX_ROUTING_SIZE} hops")]
    TooLong,
    #[error("invalid route hop")]
    InvalidHop,
    #[error("route is outside the requested subtree")]
    OutsideSubtree,
}

/// An absolute or port-relative TIO device route.
///
/// TIO packets can encode at most [`TIO_PACKET_MAX_ROUTING_SIZE`] one-byte
/// hops. Private storage and checked construction make that a type invariant.
#[derive(Clone, Copy)]
pub struct DeviceRoute {
    hops: [u8; TIO_PACKET_MAX_ROUTING_SIZE],
    len: u8,
}

impl DeviceRoute {
    pub const fn root() -> Self {
        Self {
            hops: [0; TIO_PACKET_MAX_ROUTING_SIZE],
            len: 0,
        }
    }

    /// Parse routing bytes from their reverse on-wire order.
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, RouteError> {
        if bytes.len() > TIO_PACKET_MAX_ROUTING_SIZE {
            return Err(RouteError::TooLong);
        }

        let mut route = Self::root();
        for &hop in bytes.iter().rev() {
            route.push(hop)?;
        }
        Ok(route)
    }

    pub fn from_str(route_str: &str) -> Result<Self, RouteError> {
        let mut route = Self::root();
        let stripped = route_str.strip_prefix('/').unwrap_or(route_str);
        if stripped.is_empty() {
            return Ok(route);
        }

        for segment in stripped.split('/') {
            let hop = segment.parse::<u8>().map_err(|_| RouteError::InvalidHop)?;
            route.push(hop)?;
        }
        Ok(route)
    }

    fn from_hops(hops: &[u8]) -> Result<Self, RouteError> {
        if hops.len() > TIO_PACKET_MAX_ROUTING_SIZE {
            return Err(RouteError::TooLong);
        }

        let mut route = Self::root();
        route.hops[..hops.len()].copy_from_slice(hops);
        route.len = hops.len() as u8;
        Ok(route)
    }

    fn push(&mut self, hop: u8) -> Result<(), RouteError> {
        let index = self.len();
        if index == TIO_PACKET_MAX_ROUTING_SIZE {
            return Err(RouteError::TooLong);
        }
        self.hops[index] = hop;
        self.len += 1;
        Ok(())
    }

    pub fn as_slice(&self) -> &[u8] {
        &self.hops[..self.len()]
    }

    pub fn len(&self) -> usize {
        usize::from(self.len)
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    pub fn iter(&self) -> std::slice::Iter<'_, u8> {
        self.as_slice().iter()
    }

    pub fn serialize(&self, mut rest_of_packet: Vec<u8>) -> Result<Vec<u8>, ()> {
        if rest_of_packet.len() < std::mem::size_of::<TioPktHdr>() {
            return Err(());
        }

        rest_of_packet[1] |= self.len;
        rest_of_packet.extend(self.iter().rev());
        Ok(rest_of_packet)
    }

    /// Strip `self` from the absolute `other_route`.
    pub fn relative_route(&self, other_route: &Self) -> Result<Self, RouteError> {
        let relative = other_route
            .as_slice()
            .strip_prefix(self.as_slice())
            .ok_or(RouteError::OutsideSubtree)?;
        Self::from_hops(relative)
    }

    /// Append a port-relative route, failing before an invalid route can exist.
    pub fn absolute_route(&self, other_route: &Self) -> Result<Self, RouteError> {
        let combined_len = self.len() + other_route.len();
        if combined_len > TIO_PACKET_MAX_ROUTING_SIZE {
            return Err(RouteError::TooLong);
        }

        let mut route = *self;
        route.hops[self.len()..combined_len].copy_from_slice(other_route.as_slice());
        route.len = combined_len as u8;
        Ok(route)
    }
}

impl Default for DeviceRoute {
    fn default() -> Self {
        Self::root()
    }
}

impl PartialEq for DeviceRoute {
    fn eq(&self, other: &Self) -> bool {
        self.as_slice() == other.as_slice()
    }
}

impl Eq for DeviceRoute {}

impl Hash for DeviceRoute {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.as_slice().hash(state);
    }
}

impl Ord for DeviceRoute {
    fn cmp(&self, other: &Self) -> Ordering {
        self.as_slice().cmp(other.as_slice())
    }
}

impl PartialOrd for DeviceRoute {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl std::fmt::Debug for DeviceRoute {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("DeviceRoute")
            .field(&self.as_slice())
            .finish()
    }
}

impl Display for DeviceRoute {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        if self.is_empty() {
            return write!(f, "/");
        }
        for segment in self.as_slice() {
            write!(f, "/{segment}")?;
        }
        Ok(())
    }
}
