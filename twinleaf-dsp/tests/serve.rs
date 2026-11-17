//! The device that publishes a spec, without its sockets: what it sends, and
//! what it answers.

use std::sync::mpsc::Receiver;
use twinleaf::data::PacketParser;
use twinleaf::proto::data::MetadataReply;
use twinleaf::proto::rpc::{Method, RpcError};
use twinleaf::tio::packet::Payload;
use twinleaf::tio::Packet;
use twinleaf_dsp::lang;
use twinleaf_dsp::run::{run_log, RunOptions};
use twinleaf_dsp::serve::{Device, SPEC_CHUNK};

mod common;
use common::*;

/// Memoryless but for the windows of `decimate` and `align`, so that what it
/// makes of a sample does not depend on where it began.
const SPEC: &str = "\
# A gradiometer with a gain.
source a = /0/field @rate(1000) @channels(x, y, z) @unit(nT)
source b = /1/field @rate(500) @channels(x, y, z) @unit(nT)

param gain = 1 [0.5 .. 4]
param spare : Hz = 10

let fr = align(a | decimate(2), b)
out gradient : nT = (fr.a - fr.b) * gain
";

fn device() -> Device {
    Device::new(SPEC.to_owned(), RunOptions::default()).unwrap()
}

fn call(device: &mut Device, name: &str, args: &[u8]) -> Result<Vec<u8>, RpcError> {
    device.call(Method::ByName(name.as_bytes()), args)
}

fn float(reply: Result<Vec<u8>, RpcError>) -> f64 {
    f64::from_le_bytes(reply.unwrap().try_into().unwrap())
}

fn count(reply: Result<Vec<u8>, RpcError>) -> u32 {
    u32::from_le_bytes(reply.unwrap().try_into().unwrap())
}

/// The text of every log message waiting on a connection, with its level.
fn logs(link: &Receiver<Packet>) -> Vec<(u8, String)> {
    link.try_iter()
        .filter_map(|packet| match packet.payload() {
            Payload::Log(log) => Some((
                log.level.value(),
                String::from_utf8_lossy(log.message).into_owned(),
            )),
            _ => None,
        })
        .collect()
}

#[test]
fn the_device_lists_what_the_spec_gives_it() {
    let mut device = device();
    assert_eq!(call(&mut device, "dev.name", &[]).unwrap(), b"tiodsp");
    assert_eq!(count(call(&mut device, "dev.session", &[])) & 1, 1);

    // Every listed RPC, by walking the list as a client does.
    let listed = u16::from_le_bytes(
        call(&mut device, "rpc.listinfo", &[])
            .unwrap()
            .try_into()
            .unwrap(),
    );
    let mut table = Vec::new();
    for index in 0..listed {
        let entry = call(&mut device, "rpc.listinfo", &index.to_le_bytes()).unwrap();
        let meta = u16::from_le_bytes([entry[0], entry[1]]);
        let name = String::from_utf8(entry[2..].to_vec()).unwrap();
        assert_eq!(
            call(&mut device, "rpc.info", name.as_bytes()).unwrap(),
            meta.to_le_bytes()
        );
        table.push((name, meta));
    }
    // A double is eight bytes of float; 0x0100 reads and 0x0200 writes. A
    // param with a range has its ends beside it, as the app's sliders expect.
    assert_eq!(
        table,
        [
            ("dsp.gain.min".to_owned(), 0x8182),
            ("dsp.gain.max".to_owned(), 0x8182),
            ("dsp.gain".to_owned(), 0x8382),
            ("dsp.spare".to_owned(), 0x8382),
            ("dsp.spec.size".to_owned(), 0x8140),
            ("dsp.spec.read".to_owned(), 0),
            ("dsp.spec.write".to_owned(), 0),
            ("dsp.spec.apply".to_owned(), 0x8000),
        ]
    );
    assert_eq!(
        call(&mut device, "rpc.listinfo", &listed.to_le_bytes()),
        Err(RpcError::Range)
    );
    assert_eq!(
        call(&mut device, "dsp.nothing", &[]),
        Err(RpcError::NotFound)
    );

    // The device describes itself before it has a stream to describe.
    let reply = call(&mut device, "dev.metadata", &[]).unwrap();
    assert!(MetadataReply::parse(&reply).is_some());
}

#[test]
fn a_param_is_read_and_written_and_the_spec_follows() {
    let mut device = device();
    let link = device.attach();
    assert_eq!(float(call(&mut device, "dsp.gain", &[])), 1.0);
    assert_eq!(float(call(&mut device, "dsp.gain.min", &[])), 0.5);
    assert_eq!(float(call(&mut device, "dsp.gain.max", &[])), 4.0);
    let hash = count(call(&mut device, "rpc.hash", &[]));
    let version = count(call(&mut device, "settings.version", &[]));
    logs(&link);

    // The reply to a write is the value now held.
    assert_eq!(
        float(call(&mut device, "dsp.gain", &2.5_f64.to_le_bytes())),
        2.5
    );
    assert_eq!(float(call(&mut device, "dsp.gain", &[])), 2.5);
    assert!(device.source().contains("param gain = 2.5 [0.5 .. 4]\n"));
    assert_eq!(logs(&link), [(3, "dsp.gain = 2.5".to_owned())]);
    // The same RPCs, so the same table; but something was set.
    assert_eq!(count(call(&mut device, "rpc.hash", &[])), hash);
    assert_eq!(
        count(call(&mut device, "settings.version", &[])),
        version + 1
    );

    // What cannot be set says why in the log, and changes nothing.
    assert_eq!(
        call(&mut device, "dsp.gain", &9.0_f64.to_le_bytes()),
        Err(RpcError::Range)
    );
    assert_eq!(
        logs(&link),
        [(2, "`gain` cannot be 9: its range is 0.5 to 4".to_owned())]
    );
    assert_eq!(
        call(&mut device, "dsp.gain.max", &9.0_f64.to_le_bytes()),
        Err(RpcError::ReadOnly)
    );
    assert_eq!(
        call(&mut device, "dsp.gain", &[1, 2, 3]),
        Err(RpcError::ArgsSize)
    );
    assert_eq!(float(call(&mut device, "dsp.gain", &[])), 2.5);

    // By number, as a client that has walked the list may call it.
    let by_number = device.call(Method::ById(twinleaf::proto::RpcMethodId::new(2)), &[]);
    assert_eq!(float(by_number), 2.5);
}

#[test]
fn the_spec_is_read_and_replaced_in_pieces() {
    let mut device = device();
    let link = device.attach();
    // On connecting, the device says what it is running.
    let said: String = logs(&link)
        .into_iter()
        .map(|(_, text)| text + "\n")
        .collect();
    assert!(said.contains("spec revision 1:"), "{said}");
    assert!(
        said.contains("out gradient : nT = (fr.a - fr.b) * gain"),
        "{said}"
    );

    let read = |device: &mut Device| {
        let size = count(call(device, "dsp.spec.size", &[])) as usize;
        let mut text = Vec::new();
        while text.len() < size {
            let piece = call(device, "dsp.spec.read", &(text.len() as u32).to_le_bytes()).unwrap();
            assert!(!piece.is_empty() && piece.len() <= SPEC_CHUNK);
            text.extend(piece);
        }
        String::from_utf8(text).unwrap()
    };
    let write = |device: &mut Device, text: &str| {
        for (index, piece) in text.as_bytes().chunks(100).enumerate() {
            let at = (index * 100) as u32;
            let request = [&at.to_le_bytes()[..], piece].concat();
            let held = count(call(device, "dsp.spec.write", &request));
            assert_eq!(held as usize, index * 100 + piece.len());
        }
        call(device, "dsp.spec.apply", &[])
    };
    assert_eq!(read(&mut device), SPEC);

    // A spec that does not check is refused, the reasons go to the log as
    // they would be printed, and the old one is still running.
    let broken = SPEC.replace("fr.a - fr.b", "fr.a - fr.c");
    assert_eq!(write(&mut device, &broken), Err(RpcError::Invalid));
    let reasons: String = logs(&link)
        .into_iter()
        .map(|(level, text)| {
            assert_eq!(level, 1);
            text + "\n"
        })
        .collect();
    assert!(
        reasons
            .starts_with("error[E0420]: `fr` has no channel or group called `c`\n --> dsp.spec:9:"),
        "{reasons}"
    );
    assert!(reasons.contains("= help: it has: a, b"), "{reasons}");
    assert_eq!(read(&mut device), SPEC);

    // A write that does not follow the last is refused.
    let request = [&7u32.to_le_bytes()[..], b"out"].concat();
    assert_eq!(
        call(&mut device, "dsp.spec.write", &request),
        Err(RpcError::Range)
    );

    // One that checks replaces it, and the RPCs follow its params.
    let hash = count(call(&mut device, "rpc.hash", &[]));
    let renamed = SPEC.replace("spare", "cutoff") + "out slow : nT = a | decimate(10)\n";
    assert_eq!(write(&mut device, &renamed), Ok(Vec::new()));
    assert_eq!(read(&mut device), renamed);
    assert_ne!(count(call(&mut device, "rpc.hash", &[])), hash);
    assert_eq!(float(call(&mut device, "dsp.cutoff", &[])), 10.0);
    assert_eq!(call(&mut device, "dsp.spare", &[]), Err(RpcError::NotFound));
    let said: String = logs(&link)
        .into_iter()
        .map(|(_, text)| text + "\n")
        .collect();
    assert!(said.contains("spec revision 2:"), "{said}");
}

#[test]
fn outputs_are_published_as_a_run_would_compute_them() {
    let log = record(
        "served",
        &[
            Sensor::new("/0", FAST, first_sensor(4000, 1.0, 0.01)),
            Sensor::new("/1", SLOW, second_sensor(2000, 0.01)),
        ],
    );
    let analysis = lang::analyze(SPEC);
    let offline = run_log(&analysis.program, &log, &RunOptions::default()).unwrap();
    let expected = &offline.outputs[0].runs[0];

    // Live, the same batches arrive one at a time, and halfway through the
    // gain is set to 2 over RPC.
    let mut device = device();
    let link = device.attach();
    let batches = batches(&log);
    let halfway = batches.len() / 2;
    for (index, batch) in batches.iter().enumerate() {
        if index == halfway {
            call(&mut device, "dsp.gain", &2.0_f64.to_le_bytes()).unwrap();
        }
        device.take(batch);
    }

    // Read what the device sent the way any client would.
    let mut parser = PacketParser::new(false);
    for packet in link.try_iter() {
        let _ = parser.push_packet(&packet);
    }
    let mut samples = 0;
    let mut doubled = 0;
    for batch in parser.finish() {
        let metadata = batch.metadata();
        assert_eq!(metadata.schema().stream().name(), "gradient");
        assert_eq!(metadata.rate_hz(), 500.0);
        // The outputs tell time by the clock their sources did.
        assert_eq!(metadata.segment().timeref_serial(), "hub");
        let names: Vec<_> = metadata
            .schema()
            .columns()
            .map(|c| c.name().to_owned())
            .collect();
        assert_eq!(names, ["x", "y", "z"]);
        assert!(metadata.schema().columns().all(|c| c.units() == "nT"));

        let columns: Vec<_> = batch.columns().iter().map(|c| c.to_f64()).collect();
        for row in 0..batch.len() {
            let time = batch.row(row).unwrap().timestamp_begin();
            let index = ((time - 1_700_000_000.0) * 500.0).round() as u64;
            let at = (index - expected.first) as usize;
            // Each sample is the one a run computes for that instant, or
            // twice it once the gain was set. Doubling is exact.
            let gain = columns[0][row] / expected.rows[0][at];
            assert!(gain == 1.0 || gain == 2.0, "sample {index} has gain {gain}");
            for (column, row_of) in columns.iter().zip(&expected.rows) {
                assert_eq!(column[row].to_bits(), (row_of[at] * gain).to_bits());
            }
            samples += 1;
            doubled += usize::from(gain == 2.0);
        }
    }
    assert!(samples > 1500, "only {samples} samples were published");
    assert!(
        doubled > 500 && doubled < samples - 500,
        "{doubled} of {samples} were doubled"
    );
}

#[test]
fn a_spec_that_does_not_fit_the_sensors_says_so_once() {
    // The second sensor runs at a rate the spec does not declare.
    let log = record(
        "misfit",
        &[
            Sensor::new("/0", FAST, first_sensor(400, 1.0, 0.0)),
            Sensor::new("/1", FAST, second_sensor(400, 0.0)),
        ],
    );
    let mut device = device();
    let link = device.attach();
    logs(&link);
    for batch in batches(&log) {
        device.take(&batch);
    }
    let said = logs(&link);
    assert_eq!(said.len(), 1, "{said:?}");
    assert_eq!(said[0].0, 1);
    assert!(
        said[0].1.starts_with(
            "error[E0501]: `b` is declared at 500 Hz, and the device has `/1/field` at 1 kHz\n --> dsp.spec:3:1"
        ),
        "{}",
        said[0].1
    );
}
