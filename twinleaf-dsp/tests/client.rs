//! Reading and writing a running graph's spec over its RPCs.

use twinleaf::proto::rpc::{Method, RpcError};
use twinleaf_dsp::client::{read_spec, write_spec, TransferError};
use twinleaf_dsp::run::RunOptions;
use twinleaf_dsp::serve::{Device, SPEC_CHUNK};

const SPEC: &str = "\
source a = /0/field @rate(1000) @channels(x, y, z) @unit(nT)

param gain = 1 [0.5 .. 4]

out y : nT = a * gain
";

fn device() -> Device {
    Device::new(SPEC.to_owned(), RunOptions::default()).unwrap()
}

/// A spec too long for one packet, or for three.
fn long() -> String {
    let mut text = String::from(SPEC);
    for line in 0..40 {
        text.push_str(&format!(
            "# Line {line} of a comment, in µT and nothing else.\n"
        ));
    }
    assert!(text.len() > 3 * SPEC_CHUNK);
    text
}

#[test]
fn a_spec_goes_over_and_comes_back_in_pieces() {
    let mut device = device();
    let mut calls = Vec::new();
    let mut call = |name: &str, args: &[u8]| {
        calls.push((name.to_owned(), args.len()));
        device.call(Method::ByName(name.as_bytes()), args)
    };
    assert_eq!(read_spec(&mut call).unwrap(), SPEC);

    let text = long();
    write_spec(&mut call, &text).unwrap();
    assert_eq!(read_spec(&mut call).unwrap(), text);

    // No request is longer than a packet holds: four bytes of offset and
    // a piece of the text.
    assert!(calls.iter().all(|(_, size)| *size <= 4 + SPEC_CHUNK));
    let writes = calls.iter().filter(|(name, _)| name == "dsp.spec.write");
    assert_eq!(writes.count(), text.len().div_ceil(SPEC_CHUNK));
    assert_eq!(device.source(), text);
}

#[test]
fn a_text_that_does_not_check_is_refused_and_the_old_one_runs_on() {
    let mut device = device();
    let mut call = |name: &str, args: &[u8]| device.call(Method::ByName(name.as_bytes()), args);
    let broken = SPEC.replace("a * gain", "a * gian");
    assert_eq!(
        write_spec(&mut call, &broken),
        Err(TransferError::Call(RpcError::Invalid))
    );
    assert_eq!(read_spec(&mut call).unwrap(), SPEC);

    // Nothing at all is a text too, and is taken or refused like any
    // other: the transfer itself does not stumble on it.
    let emptied = write_spec(&mut call, "");
    assert!(matches!(
        emptied,
        Ok(()) | Err(TransferError::Call(RpcError::Invalid))
    ));
    let now = read_spec(&mut call).unwrap();
    assert!(now.is_empty() || now == SPEC);
}

#[test]
fn a_spec_that_changes_under_a_read_is_read_again() {
    // Someone sets a param between the size being asked and the text being
    // read. What comes back is the text after the change, whole.
    let mut device = device();
    let mut reads = 0;
    let mut call = |name: &str, args: &[u8]| {
        if name == "dsp.spec.read" {
            reads += 1;
            if reads == 1 {
                device
                    .call(Method::ByName(b"dsp.gain"), &2.5f64.to_le_bytes())
                    .unwrap();
            }
        }
        device.call(Method::ByName(name.as_bytes()), args)
    };
    let text = read_spec(&mut call).unwrap();
    assert_eq!(text, SPEC.replace("gain = 1", "gain = 2.5"));
    assert_eq!(reads, 2);
}

#[test]
fn a_spec_that_never_holds_still_is_given_up_on() {
    // Every read finds the gain a digit longer than the size said: 1.5,
    // 1.25, 1.125 and on.
    let mut device = device();
    let mut step = 1.0f64;
    let mut call = |name: &str, args: &[u8]| {
        if name == "dsp.spec.read" {
            step /= 2.0;
            device
                .call(Method::ByName(b"dsp.gain"), &(1.0 + step).to_le_bytes())
                .unwrap();
        }
        device.call(Method::ByName(name.as_bytes()), args)
    };
    assert_eq!(read_spec(&mut call), Err(TransferError::Changing));
    // It was the text that moved, not the device that failed: it reads
    // once left alone.
    let mut call = |name: &str, args: &[u8]| device.call(Method::ByName(name.as_bytes()), args);
    assert!(read_spec(&mut call)
        .unwrap()
        .contains("param gain = 1.0625 "));
}

#[test]
fn a_device_that_is_not_there_says_so() {
    let call = |_: &str, _: &[u8]| -> Result<Vec<u8>, &'static str> { Err("no route") };
    assert_eq!(read_spec(call), Err(TransferError::Call("no route")));
    assert_eq!(write_spec(call, SPEC), Err(TransferError::Call("no route")));

    // And one that answers with something else is not believed.
    let call = |_: &str, _: &[u8]| -> Result<Vec<u8>, &'static str> { Ok(vec![1, 2]) };
    assert_eq!(read_spec(call), Err(TransferError::Reply("dsp.spec.size")));
}
