//! What each device made of one call made of several devices.

use super::{CallError, RpcMetaExt, RpcRegistry, RpcValue, RpcValueType, RpcValueTypeExt};
use crate::proto::DeviceRoute;
use std::sync::Arc;

/// What one device made of a call that
/// [`DeviceTree::call_each`](crate::device::DeviceTree::call_each) made of
/// several devices, or of one call or read of its own.
#[derive(Debug, Clone, PartialEq)]
pub enum Outcome {
    /// The device answered, with a value, or with none for an RPC of no
    /// value.
    Ok(Option<RpcValue>),
    /// The device answered with an error, or its reply did not decode.
    Error(String),
    /// The reply timed out or was lost, so the call may have taken effect.
    Unknown(String),
    /// The call could not be made of this device, so nothing was sent to
    /// any device.
    Refused(String),
    /// This device would have taken the call, but another refused it, so
    /// nothing was sent.
    Skipped,
}

impl Outcome {
    /// Returns what a device made of a call whose reply is of `kind`, from
    /// its `reply`. A reply of a unit type is an answer of no value. A call
    /// that timed out or lost its reply is unknown, not failed, as it may
    /// have taken effect.
    pub fn of_reply(kind: RpcValueType, reply: Result<Vec<u8>, CallError>) -> Outcome {
        match reply {
            Ok(_) if kind == RpcValueType::Unit => Outcome::Ok(None),
            Ok(bytes) => match kind.decode_shortest(&bytes) {
                Ok(value) => Outcome::Ok(Some(value)),
                Err(error) => Outcome::Error(error.to_string()),
            },
            Err(error @ (CallError::Timeout | CallError::ResponseLost)) => {
                Outcome::Unknown(error.to_string())
            }
            Err(error) => Outcome::Error(error.to_string()),
        }
    }
}

/// One device's part of a call, its argument encoded by its own table.
#[derive(Debug)]
pub(crate) struct Planned {
    pub(crate) route: DeviceRoute,
    /// What the reply decodes as.
    pub(crate) kind: RpcValueType,
    pub(crate) arg: Vec<u8>,
}

/// Encodes `text` as the argument of `name` for each device, by the table
/// each has, `Ok` for a device whose table is loaded and `Err` with why it
/// is not for one whose table is not. When any device cannot take the call,
/// returns an outcome for every device instead, the devices that cannot
/// refused and the others skipped.
pub(crate) fn plan(
    name: &str,
    text: &str,
    tables: Vec<(DeviceRoute, Result<Arc<RpcRegistry>, String>)>,
) -> Result<Vec<Planned>, Vec<(DeviceRoute, Outcome)>> {
    let encoded: Vec<(DeviceRoute, Result<Planned, String>)> = tables
        .into_iter()
        .map(|(route, table)| {
            let planned = table.and_then(|table| {
                let desc = table.find(name).ok_or("it has no such RPC")?;
                let arg = desc
                    .meta
                    .text_type()
                    .encode_text(text)
                    .map_err(|error| error.to_string())?;
                Ok(Planned {
                    route,
                    kind: desc.meta.kind(),
                    arg,
                })
            });
            (route, planned)
        })
        .collect();
    if encoded.iter().all(|(_, planned)| planned.is_ok()) {
        return Ok(encoded
            .into_iter()
            .filter_map(|(_, planned)| planned.ok())
            .collect());
    }
    Err(encoded
        .into_iter()
        .map(|(route, planned)| match planned {
            Ok(_) => (route, Outcome::Skipped),
            Err(error) => (route, Outcome::Refused(error)),
        })
        .collect())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::session::rpc::{RpcDescriptor, RpcMeta, RpcMetaFlags};

    const F32: RpcValueType = RpcValueType::Float { size: 4 };

    fn table(entries: &[(&str, RpcValueType)]) -> Result<Arc<RpcRegistry>, String> {
        let setting = RpcMetaFlags::READABLE | RpcMetaFlags::WRITABLE;
        let rpcs = entries
            .iter()
            .map(|(name, kind)| {
                RpcDescriptor::from_meta(RpcMeta::typed(*kind, setting).bits(), name.to_string())
            })
            .collect();
        Ok(Arc::new(RpcRegistry::new(rpcs)))
    }

    fn route(text: &str) -> DeviceRoute {
        text.parse().unwrap()
    }

    #[test]
    fn a_call_every_device_takes_is_planned_for_each() {
        let tables = vec![
            (route("/0"), table(&[("gain", F32)])),
            (route("/1"), table(&[("gain", F32)])),
        ];
        let planned = plan("gain", "2.5", tables).unwrap();
        let routes: Vec<_> = planned.iter().map(|p| p.route).collect();
        assert_eq!(routes, [route("/0"), route("/1")]);
        assert_eq!(planned[0].arg, 2.5f32.to_le_bytes());
        assert_eq!(planned[1].kind, F32);
    }

    #[test]
    fn a_device_that_refuses_a_call_stops_it_for_every_device() {
        let tables = vec![
            (route("/0"), table(&[("gain", F32)])),
            (route("/1"), table(&[("other", F32)])),
            (route("/2"), Err("its RPC table has not loaded".into())),
        ];
        let results = plan("gain", "2.5", tables).unwrap_err();
        assert_eq!(
            results,
            [
                (route("/0"), Outcome::Skipped),
                (route("/1"), Outcome::Refused("it has no such RPC".into())),
                (
                    route("/2"),
                    Outcome::Refused("its RPC table has not loaded".into())
                ),
            ]
        );
    }

    #[test]
    fn a_text_one_type_cannot_hold_refuses_the_call() {
        let tables = vec![
            (route("/0"), table(&[("gain", F32)])),
            (route("/1"), table(&[("gain", RpcValueType::Bool)])),
        ];
        let results = plan("gain", "2.5", tables).unwrap_err();
        assert_eq!(results[0].1, Outcome::Skipped);
        assert!(matches!(results[1].1, Outcome::Refused(_)));
    }

    #[test]
    fn a_reply_that_never_came_is_unknown() {
        for lost in [CallError::Timeout, CallError::ResponseLost] {
            assert!(matches!(
                Outcome::of_reply(F32, Err(lost)),
                Outcome::Unknown(_)
            ));
        }
        assert_eq!(
            Outcome::of_reply(F32, Ok(0.5f32.to_le_bytes().to_vec())),
            Outcome::Ok(Some(RpcValue::F64(0.5)))
        );
        assert_eq!(
            Outcome::of_reply(RpcValueType::Unit, Ok(Vec::new())),
            Outcome::Ok(None)
        );
    }

    #[test]
    fn a_reply_that_does_not_decode_is_an_error() {
        assert!(matches!(
            Outcome::of_reply(F32, Ok(vec![1])),
            Outcome::Error(_)
        ));
    }
}
