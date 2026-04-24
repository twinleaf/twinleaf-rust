//! Device discovery.
//!
//! Enumerate Twinleaf devices reachable from the host. Currently supports
//! local serial ports; future mechanisms (mDNS, UDP broadcast) will live
//! alongside as additional entry points in this module.

/// The USB interface a discovered device uses.
#[derive(Debug, Clone)]
pub enum PortInterface {
    FTDI,
    STM32,
    Unknown(u16, u16),
}

/// A device found during discovery.
#[derive(Debug, Clone)]
pub struct DiscoveredDevice {
    pub url: String,
    pub interface: PortInterface,
}

/// Enumerate Twinleaf devices on local serial ports.
///
/// Matches known Twinleaf USB VID/PIDs (FTDI and STM32 variants). If
/// `include_unknown` is true, other serial ports are also returned with
/// `PortInterface::Unknown(vid, pid)` so callers can surface them as
/// "also found these serial ports" style output.
pub fn enumerate_serial(include_unknown: bool) -> Vec<DiscoveredDevice> {
    let mut ports: Vec<DiscoveredDevice> = Vec::new();

    if let Ok(avail_ports) = serialport::available_ports() {
        for p in avail_ports.iter() {
            if let serialport::SerialPortType::UsbPort(info) = &p.port_type {
                let interface = match (info.vid, info.pid) {
                    (0x0403, 0x6015) => PortInterface::FTDI,
                    (0x0483, 0x5740) => PortInterface::STM32,
                    (vid, pid) => {
                        if !include_unknown {
                            continue;
                        }
                        PortInterface::Unknown(vid, pid)
                    }
                };
                #[cfg(target_os = "macos")]
                if p.port_name.starts_with("/dev/tty.") && !include_unknown {
                    continue;
                }
                ports.push(DiscoveredDevice {
                    url: format!("serial://{}", p.port_name),
                    interface,
                });
            }
        }
    }

    ports
}
