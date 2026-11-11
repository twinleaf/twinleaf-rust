//! The `tio capture` subcommand, which triggers a capture RPC and prints its
//! data with [`run_capture`].

use crate::CaptureCli;
use twinleaf::device::capture::{read_capture, CaptureReadout};
use twinleaf::Connection;

pub fn run_capture(cli: CaptureCli) -> eyre::Result<()> {
    use eyre::WrapErr;

    let connection = Connection::open(&cli.tio.root)?;
    let device = connection.device(cli.tio.route);

    let readout = read_capture(&device, &cli.rpc_name, cli.timeout)
        .wrap_err_with(|| format!("failed to read capture {}", cli.rpc_name))?;

    write_capture_readout(&mut std::io::stdout().lock(), &readout)
}

/// Writes `readout` as lines of text, what it is and then an x and a
/// calibrated y for each element.
fn write_capture_readout(
    out: &mut impl std::io::Write,
    readout: &CaptureReadout,
) -> eyre::Result<()> {
    let values = readout.values_f64()?;
    let meta = &readout.metadata;

    writeln!(out, "# capture {}", meta.name)?;
    writeln!(out, "# y {} ({})", meta.name, meta.units)?;
    writeln!(out, "# x {} ({})", meta.x_name, meta.x_units)?;
    writeln!(
        out,
        "# data_type={} length={} size={} blocksize={} y_calibration={}",
        meta.data_type_label(),
        meta.length,
        meta.size,
        meta.blocksize,
        meta.y_calibration
    )?;
    for (index, y) in values.into_iter().enumerate() {
        writeln!(out, "{}\t{}", meta.x_value_f64(index), y)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Arc;
    use std::time::Duration;
    use twinleaf::device::rpc::RpcMetaFlags;
    use twinleaf::proxy::{HostOptions, Layout, Mount, Proxy};
    use twinleaf::DeviceRoute;

    /// The simulated device lists its captures as captures, and `tio
    /// capture` reads and prints each, an FID that decays and a phase that
    /// climbs at the FID's frequency.
    #[test]
    fn the_simulated_captures_read_and_print() {
        let stop = Arc::new(AtomicBool::new(false));
        let url = crate::tools::simulate::serve(&["--no-drop"], stop.clone());
        let proxy = Proxy::host(HostOptions {
            port: 0,
            name: "a test".to_string(),
            default: Some(Layout {
                mounts: vec![Mount {
                    url,
                    source: DeviceRoute::root(),
                    prefix: DeviceRoute::root(),
                }],
            }),
            ..HostOptions::default()
        })
        .unwrap();
        let connection = Connection::open(&proxy.endpoint()).unwrap();
        let device = connection.device(DeviceRoute::root());

        let registry = device.rpc_registry().unwrap();
        for rpc in ["test.capture", "fid.capture", "phase.capture"] {
            let flags = registry.find(rpc).unwrap().meta.flags();
            assert!(flags.contains(RpcMetaFlags::CAPTURE), "{rpc}");
        }

        let wait = Duration::from_secs(5);
        let fid = read_capture(&device, "fid.capture", wait).unwrap();
        let phase = read_capture(&device, "phase.capture", wait).unwrap();
        stop.store(true, Ordering::Relaxed);
        proxy.shutdown();

        let mut printed = Vec::new();
        write_capture_readout(&mut printed, &fid).unwrap();
        let printed = String::from_utf8(printed).unwrap();
        let lines: Vec<&str> = printed.lines().collect();
        assert_eq!(
            lines[..4],
            [
                "# capture FID",
                "# y FID (V)",
                "# x Time (s)",
                "# data_type=i16 length=1500 size=3000 blocksize=256 y_calibration=0.000076293945",
            ]
        );
        assert!(lines[4].starts_with("0\t"), "{}", lines[4]);
        assert_eq!(lines.len(), 4 + 1500);
        println!("{}", lines[..12].join("\n"));

        let volts = fid.values_f64().unwrap();
        let rms = |values: &[f64]| {
            (values.iter().map(|v| v * v).sum::<f64>() / values.len() as f64).sqrt()
        };
        println!("rms by tenth of the shot (V):");
        for tenth in volts.chunks(150) {
            let bar = "#".repeat((rms(tenth) * 80.0).round() as usize);
            println!("{:.4} {bar}", rms(tenth));
        }
        assert!(rms(&volts[..150]) > 10.0 * rms(&volts[1350..]));

        assert_eq!(phase.metadata.name, "Unwrapped phase");
        assert_eq!(phase.metadata.units, "rad");
        let radians = phase.values_f64().unwrap();
        let hertz = radians[750] / (std::f64::consts::TAU * phase.metadata.x_value_f64(750));
        assert!((hertz / 175_000.0 - 1.0).abs() < 0.01, "{hertz}");
    }
}
