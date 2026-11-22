//! What each device made of one call made of several devices.

use super::{
    CallError, RpcErrorPayload, RpcMetaExt, RpcRegistry, RpcRegistryError, RpcValue,
    RpcValueEncodeError, RpcValueType, RpcValueTypeExt,
};
use crate::proto::rpc::RpcError;
use crate::proto::{DeviceRoute, RouteError};
use std::sync::Arc;

/// What one device made of a call that
/// [`DeviceTree::call_each`](crate::device::DeviceTree::call_each) made of
/// several devices, or of one call or read of its own.
#[derive(Debug, Clone)]
pub enum Outcome {
    /// The device answered, with a value, or with none for an RPC of no
    /// value.
    Ok(Option<RpcValue>),
    /// The device answered with an error, or its reply did not decode.
    Error(CallError),
    /// The reply timed out or was lost, or the answer was the error
    /// `Timeout`, so the call may have taken effect.
    Unknown(CallError),
    /// The call could not be made of this device, so nothing was sent to
    /// any device.
    Refused(Refusal),
    /// This device would have taken the call, but another refused it, so
    /// nothing was sent.
    Skipped,
}

/// Why a call could not be made of a device. Its text is a clause that
/// follows the device's route.
#[derive(Debug, Clone, thiserror::Error)]
#[non_exhaustive]
pub enum Refusal {
    /// The route lies outside the view the call was made from.
    #[error(transparent)]
    OutsideView(RouteError),
    /// The device's RPC table did not load in time, or the connection
    /// ended before it did.
    #[error("its RPC table has not loaded")]
    TableNotLoaded,
    /// The device's RPC table failed to load, for this reason.
    #[error("its RPC table did not load. {0}")]
    TableFailed(RpcRegistryError),
    /// The device's RPC table has no entry of the name called.
    #[error("it has no such RPC")]
    NoSuchRpc,
    /// The argument is no value of the type the RPC takes.
    #[error(transparent)]
    DoesNotFit(RpcValueEncodeError),
    /// The route is named more than once in the one call.
    #[error("it is named more than once")]
    NamedTwice,
}

impl Outcome {
    /// Returns what a device made of a call whose reply is of `kind`, from
    /// its `reply`. A reply of a unit type is an answer of no value. A call
    /// that timed out, lost its reply or was answered with the error
    /// `Timeout` is unknown, not failed, as it may have taken effect.
    pub fn of_reply(kind: RpcValueType, reply: Result<Vec<u8>, CallError>) -> Outcome {
        match reply {
            Ok(_) if kind == RpcValueType::Unit => Outcome::Ok(None),
            Ok(bytes) => match kind.decode_shortest(&bytes) {
                Ok(value) => Outcome::Ok(Some(value)),
                Err(error) => Outcome::Error(CallError::InvalidReply(error)),
            },
            Err(
                error @ (CallError::Timeout
                | CallError::ResponseLost
                | CallError::DeviceError(RpcErrorPayload {
                    error: RpcError::Timeout,
                    ..
                })),
            ) => Outcome::Unknown(error),
            Err(error) => Outcome::Error(error),
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
/// is not for one whose table is not. With no `text` the argument is none.
/// When any device cannot take the call, returns an outcome for every
/// device instead, the devices that cannot refused and the others skipped.
pub(crate) fn plan(
    name: &str,
    text: Option<&str>,
    tables: Vec<(DeviceRoute, Result<Arc<RpcRegistry>, Refusal>)>,
) -> Result<Vec<Planned>, Vec<(DeviceRoute, Outcome)>> {
    let encoded: Vec<(DeviceRoute, Result<Planned, Refusal>)> = tables
        .into_iter()
        .map(|(route, table)| {
            let planned = table.and_then(|table| {
                let desc = table.find(name).ok_or(Refusal::NoSuchRpc)?;
                let arg = match text {
                    Some(text) => desc.meta.text_type().encode_text(text),
                    None => Ok(Vec::new()),
                };
                let arg = arg.map_err(Refusal::DoesNotFit)?;
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
            Err(refusal) => (route, Outcome::Refused(refusal)),
        })
        .collect())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::session::rpc::{RpcDescriptor, RpcMeta, RpcMetaFlags, RpcValueDecodeError};
    use crate::tio::rpc::RpcDecodeError;

    const F32: RpcValueType = RpcValueType::Float { size: 4 };

    fn table(entries: &[(&str, RpcValueType)]) -> Result<Arc<RpcRegistry>, Refusal> {
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
        let planned = plan("gain", Some("2.5"), tables).unwrap();
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
            (route("/2"), Err(Refusal::TableNotLoaded)),
        ];
        let results = plan("gain", Some("2.5"), tables).unwrap_err();
        assert!(
            matches!(
                &results[..],
                [
                    (_, Outcome::Skipped),
                    (_, Outcome::Refused(Refusal::NoSuchRpc)),
                    (_, Outcome::Refused(Refusal::TableNotLoaded)),
                ]
            ),
            "{results:?}"
        );
        let routes: Vec<_> = results.iter().map(|(route, _)| *route).collect();
        assert_eq!(routes, [route("/0"), route("/1"), route("/2")]);
    }

    /// A call with no text takes no argument on any device, whatever its
    /// type, and decodes each reply by that device's table. A device whose
    /// table lacks the name still refuses it for all.
    #[test]
    fn a_call_with_no_text_reads_each_device_by_its_own_table() {
        let tables = vec![
            (route("/0"), table(&[("gain", F32)])),
            (route("/1"), table(&[("gain", RpcValueType::Bool)])),
        ];
        let planned = plan("gain", None, tables).unwrap();
        assert!(planned.iter().all(|p| p.arg.is_empty()));
        assert_eq!(planned[0].kind, F32);
        assert_eq!(planned[1].kind, RpcValueType::Bool);
        // Each reply decodes by its own device's table, four bytes as the
        // f32 of one and a byte as the bool of the other. A byte is too
        // short for the f32.
        let replies = [0.5f32.to_le_bytes().to_vec(), vec![1]];
        let decoded: Vec<Outcome> = (planned.iter().zip(replies))
            .map(|(p, reply)| Outcome::of_reply(p.kind, Ok(reply)))
            .collect();
        assert!(
            matches!(
                &decoded[..],
                [Outcome::Ok(Some(RpcValue::F64(half))), Outcome::Ok(Some(RpcValue::Bool(true)))]
                    if *half == 0.5
            ),
            "{decoded:?}"
        );
        let short = Outcome::of_reply(planned[0].kind, Ok(vec![1]));
        assert!(
            matches!(short, Outcome::Error(CallError::InvalidReply(_))),
            "{short:?}"
        );
        let tables = vec![
            (route("/0"), table(&[("gain", F32)])),
            (route("/1"), table(&[("other", F32)])),
        ];
        let results = plan("gain", None, tables).unwrap_err();
        assert!(
            matches!(
                &results[..],
                [
                    (_, Outcome::Skipped),
                    (_, Outcome::Refused(Refusal::NoSuchRpc))
                ]
            ),
            "{results:?}"
        );
    }

    #[test]
    fn a_text_one_type_cannot_hold_refuses_the_call() {
        let tables = vec![
            (route("/0"), table(&[("gain", F32)])),
            (route("/1"), table(&[("gain", RpcValueType::Bool)])),
        ];
        let results = plan("gain", Some("2.5"), tables).unwrap_err();
        assert!(matches!(results[0].1, Outcome::Skipped));
        assert!(matches!(
            results[1].1,
            Outcome::Refused(Refusal::DoesNotFit(RpcValueEncodeError::BoolText(_)))
        ));
    }

    /// Each refusal reads as the words a person reads after the route.
    #[test]
    fn a_refusal_says_why_in_words() {
        let not_bool = "2.5".parse::<bool>().unwrap_err();
        let said = [
            (
                Refusal::OutsideView(RouteError::OutsideSubtree),
                "route is outside the requested subtree",
            ),
            (Refusal::TableNotLoaded, "its RPC table has not loaded"),
            (
                Refusal::TableFailed(RpcRegistryError::DeviceRpcError(CallError::Timeout)),
                "its RPC table did not load. RPC error: timed out waiting for the RPC reply",
            ),
            (Refusal::NoSuchRpc, "it has no such RPC"),
            (Refusal::NamedTwice, "it is named more than once"),
            (
                Refusal::DoesNotFit(RpcValueEncodeError::BoolText(not_bool)),
                "provided string was not `true` or `false`",
            ),
        ];
        for (refusal, words) in said {
            assert_eq!(refusal.to_string(), words);
        }
    }

    #[test]
    fn a_reply_that_never_came_is_unknown() {
        assert!(matches!(
            Outcome::of_reply(F32, Err(CallError::Timeout)),
            Outcome::Unknown(CallError::Timeout)
        ));
        assert!(matches!(
            Outcome::of_reply(F32, Err(CallError::ResponseLost)),
            Outcome::Unknown(CallError::ResponseLost)
        ));
        assert!(matches!(
            Outcome::of_reply(F32, Ok(0.5f32.to_le_bytes().to_vec())),
            Outcome::Ok(Some(RpcValue::F64(0.5)))
        ));
        assert!(matches!(
            Outcome::of_reply(RpcValueType::Unit, Ok(Vec::new())),
            Outcome::Ok(None)
        ));
    }

    /// A reply that does not decode is an error, which reads as the reason
    /// it did not decode.
    #[test]
    fn a_reply_that_does_not_decode_is_an_error() {
        let outcome = Outcome::of_reply(F32, Ok(vec![1]));
        let Outcome::Error(error) = &outcome else {
            panic!("{outcome:?}")
        };
        let short = RpcDecodeError::TooShort {
            expected: 4,
            actual: 1,
        };
        assert!(
            matches!(error, CallError::InvalidReply(RpcValueDecodeError::Reply(e)) if *e == short),
            "{error:?}"
        );
        assert_eq!(
            error.to_string(),
            "RPC reply is too short: expected at least 4 bytes, got 1"
        );
    }
}
