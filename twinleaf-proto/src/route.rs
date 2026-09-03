//! Routes, the path from the root device to a device in the tree.
//!
//! A route is written `/1/2/3` and stored on the wire as `[3, 2, 1]`, one byte per hop.

use core::cmp::Ordering;
use core::fmt;
use core::hash::{Hash, Hasher};

use crate::packet::{Header, PacketError};

/// Why a route is invalid.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[cfg_attr(feature = "defmt", derive(defmt::Format))]
pub enum RouteError {
    /// More than eight hops.
    #[error("route exceeds the maximum depth of {} hops", DeviceRoute::MAX_HOPS)]
    TooLong,
    /// A hop that is not a number from 0 to 255.
    #[error("invalid route hop")]
    InvalidHop,
    /// The route does not start with the given subtree.
    #[error("route is outside the requested subtree")]
    OutsideSubtree,
}

/// Route in root-to-leaf order. Wire packets store hops in reverse order.
#[derive(Clone, Copy)]
pub struct DeviceRoute {
    hops: [u8; DeviceRoute::MAX_HOPS],
    len: u8,
}

impl DeviceRoute {
    /// Most hops in a route, 8.
    pub const MAX_HOPS: usize = 8;

    /// The root device, `/`.
    pub const fn root() -> Self {
        Self {
            hops: [0; DeviceRoute::MAX_HOPS],
            len: 0,
        }
    }

    /// Route from hops in root-to-leaf order. Errors above eight hops.
    pub fn from_hops(hops: &[u8]) -> Result<Self, RouteError> {
        if hops.len() > DeviceRoute::MAX_HOPS {
            return Err(RouteError::TooLong);
        }
        let mut route = Self::root();
        route.hops[..hops.len()].copy_from_slice(hops);
        route.len = hops.len() as u8;
        Ok(route)
    }

    /// Route from its wire bytes, which hold the leaf hop first.
    pub fn from_wire(bytes: &[u8]) -> Result<Self, RouteError> {
        if bytes.len() > DeviceRoute::MAX_HOPS {
            return Err(RouteError::TooLong);
        }
        let mut route = Self::root();
        for &hop in bytes.iter().rev() {
            route.push(hop)?;
        }
        Ok(route)
    }

    /// Append a hop. Errors at eight hops.
    pub fn push(&mut self, hop: u8) -> Result<(), RouteError> {
        let index = self.len();
        if index == DeviceRoute::MAX_HOPS {
            return Err(RouteError::TooLong);
        }
        self.hops[index] = hop;
        self.len += 1;
        Ok(())
    }

    /// The hops in root-to-leaf order.
    pub fn as_slice(&self) -> &[u8] {
        &self.hops[..self.len()]
    }

    /// Number of hops.
    pub fn len(&self) -> usize {
        self.len as usize
    }

    /// Whether this is the root.
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Write reverse-order on-wire hops and return the number written.
    pub fn write_wire(&self, output: &mut [u8]) -> Result<usize, RouteError> {
        if output.len() < self.len() {
            return Err(RouteError::TooLong);
        }
        for (destination, hop) in output.iter_mut().zip(self.as_slice().iter().rev()) {
            *destination = *hop;
        }
        Ok(self.len())
    }

    /// Whether this route is `ancestor` or below it.
    pub fn starts_with(&self, ancestor: &Self) -> bool {
        self.as_slice().starts_with(ancestor.as_slice())
    }

    /// `absolute` with this route removed from its front.
    pub fn relative_route(&self, absolute: &Self) -> Result<Self, RouteError> {
        let relative = absolute
            .as_slice()
            .strip_prefix(self.as_slice())
            .ok_or(RouteError::OutsideSubtree)?;
        Self::from_hops(relative)
    }

    /// `relative` appended to this route.
    pub fn absolute_route(&self, relative: &Self) -> Result<Self, RouteError> {
        let combined_len = self.len() + relative.len();
        if combined_len > DeviceRoute::MAX_HOPS {
            return Err(RouteError::TooLong);
        }
        let mut route = *self;
        route.hops[self.len()..combined_len].copy_from_slice(relative.as_slice());
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

impl fmt::Debug for DeviceRoute {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_tuple("DeviceRoute")
            .field(&self.as_slice())
            .finish()
    }
}

impl fmt::Display for DeviceRoute {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.is_empty() {
            return formatter.write_str("/");
        }
        for hop in self.as_slice() {
            write!(formatter, "/{hop}")?;
        }
        Ok(())
    }
}

impl core::str::FromStr for DeviceRoute {
    type Err = RouteError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let mut route = Self::root();
        let value = value.strip_prefix('/').unwrap_or(value);
        if value.is_empty() {
            return Ok(route);
        }
        for segment in value.split('/') {
            route.push(segment.parse().map_err(|_| RouteError::InvalidHop)?)?;
        }
        Ok(route)
    }
}

/// Why a packet could not be forwarded in place.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[cfg_attr(feature = "defmt", derive(defmt::Format))]
pub enum ForwardError {
    /// The buffer does not hold a valid packet.
    Packet(PacketError),
    /// The packet is addressed to this device.
    NotRouted,
    /// The packet already has eight hops.
    RoutingFull,
    /// No room in the buffer for another hop.
    NoCapacity,
}

/// Remove and return the next downstream hop and the new packet length.
pub fn pop_hop(buf: &mut [u8]) -> Result<(u8, usize), ForwardError> {
    let mut header = Header::parse_prefix(buf).map_err(ForwardError::Packet)?;
    let packet_len = header.packet_len();
    if buf.len() < packet_len {
        return Err(ForwardError::Packet(PacketError::NeedMore));
    }
    if header.routing_size == 0 {
        return Err(ForwardError::NotRouted);
    }
    let hop = buf[packet_len - 1];
    header.routing_size -= 1;
    header.write((&mut buf[..Header::SIZE]).try_into().unwrap());
    Ok((hop, packet_len - 1))
}

/// Append an upstream hop and return the new packet length.
pub fn push_hop(buf: &mut [u8], hop: u8) -> Result<usize, ForwardError> {
    let mut header = Header::parse_prefix(buf).map_err(ForwardError::Packet)?;
    let packet_len = header.packet_len();
    if buf.len() < packet_len {
        return Err(ForwardError::Packet(PacketError::NeedMore));
    }
    if header.routing_size as usize == DeviceRoute::MAX_HOPS {
        return Err(ForwardError::RoutingFull);
    }
    if buf.len() < packet_len + 1 {
        return Err(ForwardError::NoCapacity);
    }
    buf[packet_len] = hop;
    header.routing_size += 1;
    header.write((&mut buf[..Header::SIZE]).try_into().unwrap());
    Ok(packet_len + 1)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::packet::PacketType;

    #[test]
    fn route_roundtrips_wire_order() {
        let route: DeviceRoute = "/1/2/3".parse().unwrap();
        let mut wire = [0; DeviceRoute::MAX_HOPS];
        let len = route.write_wire(&mut wire).unwrap();
        assert_eq!(&wire[..len], [3, 2, 1]);
        assert_eq!(DeviceRoute::from_wire(&wire[..len]).unwrap(), route);
        assert_eq!(route.to_string(), "/1/2/3");
    }

    #[test]
    fn route_composition_is_checked() {
        let parent: DeviceRoute = "/1/2".parse().unwrap();
        let child: DeviceRoute = "/3/4".parse().unwrap();
        let absolute = parent.absolute_route(&child).unwrap();
        assert_eq!(absolute.to_string(), "/1/2/3/4");
        assert_eq!(parent.relative_route(&absolute).unwrap(), child);
        assert_eq!(
            child.relative_route(&parent),
            Err(RouteError::OutsideSubtree)
        );
    }

    fn packet_to(route: &str, payload: &[u8]) -> (Vec<u8>, usize) {
        let route: DeviceRoute = route.parse().unwrap();
        let mut header = Header::new(PacketType::RPC_REQ, payload.len() as u16);
        header.routing_size = route.len() as u8;
        header.ttl = 3;
        let len = header.packet_len();
        let mut buf = vec![0; len + 1];
        header.write((&mut buf[..Header::SIZE]).try_into().unwrap());
        buf[Header::SIZE..Header::SIZE + payload.len()].copy_from_slice(payload);
        route
            .write_wire(&mut buf[Header::SIZE + payload.len()..len])
            .unwrap();
        (buf, len)
    }

    #[test]
    fn pop_hop_forwards_downstream_through_a_daisy_chain() {
        let (mut buf, len) = packet_to("/10/3", b"cmd");

        let (hop, len) = pop_hop(&mut buf[..len]).unwrap();
        assert_eq!(hop, 10);
        let (hop, len) = pop_hop(&mut buf[..len]).unwrap();
        assert_eq!(hop, 3);

        let header = Header::parse_prefix(&buf[..len]).unwrap();
        assert_eq!(header.routing_size, 0);
        assert_eq!(header.ttl, 3);
        assert_eq!(&buf[header.payload_range()], b"cmd");
        assert_eq!(pop_hop(&mut buf[..len]), Err(ForwardError::NotRouted));
    }

    #[test]
    fn push_hop_builds_the_upstream_route() {
        let (mut buf, len) = packet_to("/", b"data");
        buf.push(0);
        let len = push_hop(&mut buf[..len + 1], 7).unwrap();
        let len = push_hop(&mut buf[..len + 1], 2).unwrap();

        let header = Header::parse_prefix(&buf[..len]).unwrap();
        assert_eq!(header.routing_size, 2);
        let routing = &buf[Header::SIZE + 4..len];
        assert_eq!(DeviceRoute::from_wire(routing).unwrap().to_string(), "/2/7");
        assert_eq!(&buf[header.payload_range()], b"data");
    }

    #[test]
    fn pop_then_push_restores_the_packet() {
        let (mut buf, len) = packet_to("/5", b"x");
        let original = buf[..len].to_vec();
        let (hop, popped_len) = pop_hop(&mut buf[..len]).unwrap();
        let restored_len = push_hop(&mut buf[..popped_len + 1], hop).unwrap();
        assert_eq!(restored_len, len);
        assert_eq!(&buf[..restored_len], original);
    }

    #[test]
    fn forwarding_rejects_bad_inputs() {
        let (mut buf, len) = packet_to("/1/2/3/4/5/6/7/8", b"");
        assert_eq!(
            push_hop(&mut buf[..len + 1], 9),
            Err(ForwardError::RoutingFull)
        );

        let (mut buf, len) = packet_to("/", b"y");
        assert_eq!(push_hop(&mut buf[..len], 1), Err(ForwardError::NoCapacity));

        let (mut buf, len) = packet_to("/5", b"payload");
        assert_eq!(
            pop_hop(&mut buf[..len - 3]),
            Err(ForwardError::Packet(PacketError::NeedMore))
        );
    }
}
