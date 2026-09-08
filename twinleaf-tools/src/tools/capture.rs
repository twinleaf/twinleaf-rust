use std::time::Duration;

use crate::{CaptureCli, TioOpts};
use twinleaf::device::capture::{read_capture, CaptureReadout};
use twinleaf::Connection;

pub fn run_capture(capture_cli: CaptureCli) -> eyre::Result<()> {
    capture(&capture_cli.tio, capture_cli.rpc_name, capture_cli.timeout)
}

pub fn capture(tio: &TioOpts, rpc_name: String, timeout: Duration) -> eyre::Result<()> {
    use eyre::WrapErr;

    let connection = Connection::connect(&tio.root)?;
    let device = connection.device(tio.route);

    let readout = read_capture(&device, &rpc_name, timeout)
        .wrap_err_with(|| format!("failed to read capture {}", rpc_name))?;

    print_capture_readout(&readout)
}

fn print_capture_readout(readout: &CaptureReadout) -> eyre::Result<()> {
    let values = readout.values_f64()?;
    let meta = &readout.metadata;

    println!("# capture {}", meta.name);
    println!("# y {} ({})", meta.name, meta.units);
    println!("# x {} ({})", meta.x_name, meta.x_units);
    println!(
        "# data_type={} length={} size={} blocksize={} y_calibration={}",
        meta.data_type_label(),
        meta.length,
        meta.size,
        meta.blocksize,
        meta.y_calibration
    );
    for (index, y) in values.into_iter().enumerate() {
        println!("{}\t{}", meta.x_value_f64(index), y);
    }
    Ok(())
}
