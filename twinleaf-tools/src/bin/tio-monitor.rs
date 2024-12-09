use tio::proto::DeviceRoute;
use tio::proxy;
use tio::util;
use twinleaf::data::{ColumnData, Device};
use twinleaf::tio;

use getopts::Options;
use pancurses::*;
use std::env;

fn tio_opts() -> Options {
    let mut opts = Options::new();
    opts.optopt(
        "r",
        "",
        &format!("sensor root (default {})", util::default_proxy_url()),
        "address",
    );
    opts.optopt(
        "s",
        "",
        "sensor path in the sensor tree (default /)",
        "path",
    );
    opts
}

fn tio_parseopts(opts: Options, args: &[String]) -> (getopts::Matches, String, DeviceRoute) {
    let matches = match opts.parse(args) {
        Ok(m) => m,
        Err(f) => {
            panic!("{}", f.to_string())
        }
    };
    let root = if let Some(url) = matches.opt_str("r") {
        url
    } else {
        "tcp://localhost".to_string()
    };
    let route = if let Some(path) = matches.opt_str("s") {
        DeviceRoute::from_str(&path).unwrap()
    } else {
        DeviceRoute::root()
    };
    (matches, root, route)
}

fn run_monitor(args: &[String]) {
    let opts = tio_opts();
    let (_matches, root, route) = tio_parseopts(opts, args);

    let proxy = proxy::Interface::new(&root);
    let device = proxy.device_full(route).unwrap();
    let mut device = Device::new(device);

    //initialize terminal window
    let window = initscr();
    window.refresh();
    noecho();
    let mut y_position = 3;

    loop {
        let sample = device.next();

        let name = format!(
            "Device Name: {}  Serial: {}   Session ID: {}",
            sample.device.name, sample.device.serial_number, sample.device.session_id
        );
        window.mvprintw(1, 0, &name);

        for col in &sample.columns {
            let string = format!(
                " {}: {}",
                col.desc.name,
                match col.value {
                    ColumnData::Int(x) => format!("{}", x),
                    ColumnData::UInt(x) => format!("{:.3}", x),
                    ColumnData::Float(x) => format!("{:.3}", x),
                    ColumnData::Unknown => "?".to_string(),
                }
            );

            if sample.stream.stream_id == 0x02 {
                y_position += 1;
            }

            window.mvprintw(y_position, 0, &string);
            window.refresh();
        }
        y_position = 3;
    }
}

fn main() {
    let args: Vec<String> = env::args().collect();

    run_monitor(&args);
}
