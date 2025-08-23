use super::TioPktHdr;
use super::TIO_PACKET_MAX_ROUTING_SIZE;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct DeviceRoute {
    route: Vec<u8>,
}

impl DeviceRoute {
    pub fn root() -> DeviceRoute {
        DeviceRoute { route: vec![] }
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<DeviceRoute, ()> {
        if bytes.len() > TIO_PACKET_MAX_ROUTING_SIZE {
            Err(())
        } else {
            let mut route = bytes.to_vec();
            route.reverse();
            Ok(DeviceRoute { route })
        }
    }

    pub fn from_str(route_str: &str) -> Result<DeviceRoute, ()> {
        let mut ret = DeviceRoute::root();
        let stripped = match route_str.strip_prefix("/") {
            Some(s) => s,
            None => route_str,
        };
        if stripped.len() > 0 {
            for segment in stripped.split('/') {
                if ret.route.len() >= TIO_PACKET_MAX_ROUTING_SIZE {
                    return Err(());
                }
                if let Ok(n) = segment.parse() {
                    ret.route.push(n);
                } else {
                    return Err(());
                }
            }
        }
        Ok(ret)
    }

    pub fn len(&self) -> usize {
        self.route.len()
    }

    pub fn iter(&self) -> std::slice::Iter<'_, u8> {
        self.route.iter()
    }

    pub fn serialize(&self, mut rest_of_packet: Vec<u8>) -> Result<Vec<u8>, ()> {
        if (self.route.len() > TIO_PACKET_MAX_ROUTING_SIZE)
            || (rest_of_packet.len() < std::mem::size_of::<TioPktHdr>())
        {
            Err(())
        } else {
            rest_of_packet[1] |= self.route.len() as u8;
            for hop in self.route.iter().rev() {
                rest_of_packet.push(*hop);
            }
            Ok(rest_of_packet)
        }
    }

    // Returns the relative route from this to other_route (which is absolute).
    // Error if other route is not in the subtree rooted by this route.
    pub fn relative_route(&self, other_route: &DeviceRoute) -> Result<DeviceRoute, ()> {
        if (self.len() <= other_route.len()) && (self.route == other_route.route[0..self.len()]) {
            Ok(DeviceRoute {
                route: other_route.route[self.len()..].to_vec(),
            })
        } else {
            Err(())
        }
    }

    pub fn absolute_route(&self, other_route: &DeviceRoute) -> DeviceRoute {
        let mut route = self.route.clone();
        route.extend_from_slice(&other_route.route);
        DeviceRoute { route }
    }
}

use std::fmt::{Display, Formatter};

impl Display for DeviceRoute {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        if self.route.len() == 0 {
            write!(f, "/")?;
        } else {
            for segment in &self.route {
                write!(f, "/{}", segment)?;
            }
        }
        Ok(())
    }
}
