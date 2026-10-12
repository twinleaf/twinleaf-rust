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
