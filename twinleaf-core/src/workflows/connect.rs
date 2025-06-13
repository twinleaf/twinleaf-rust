use thiserror::Error;

#[derive(Error, Debug)]
pub enum ConnectError {
    #[error("Failed to enumerate available serial ports")]
    EnumerationFailed(#[from] serialport::Error),

    #[error("Could not automatically find a sensor")]
    AutoDetectionFailed,

    #[error("Found {0} multiple possible sensors; please specify one explicitly")]
    MultipleSensorsFound(usize),
}

#[derive(Debug)]
pub enum TwinleafPortInterface {
    FTDI,
    STM32,
    Unknown(u16, u16),
}

#[derive(Debug)]
pub struct SerialDevice {
    pub url: String,
    pub ifc: TwinleafPortInterface,
}

pub fn enum_devices(all: bool) -> Result<Vec<SerialDevice>, ConnectError> {
    let mut ports: Vec<SerialDevice> = Vec::new();

    let avail_ports = serialport::available_ports()?;

    for p in avail_ports.iter() {
        if let serialport::SerialPortType::UsbPort(info) = &p.port_type {
            let interface = match (info.vid, info.pid) {
                (0x0403, 0x6015) => TwinleafPortInterface::FTDI,
                (0x0483, 0x5740) => TwinleafPortInterface::STM32,
                (vid, pid) => {
                    if !all {
                        continue;
                    };
                    TwinleafPortInterface::Unknown(vid, pid)
                }
            };
            #[cfg(target_os = "macos")]
            if p.port_name.starts_with("/dev/tty.") && !all {
                continue;
            }
            ports.push(SerialDevice {
                url: format!("serial://{}", p.port_name),
                ifc: interface,
            });
        } // else ignore other types for now: bluetooth, pci, unknown
    }
    
    Ok(ports)
}

pub fn auto_detect_sensor() -> Result<String, ConnectError> {
    let devices = enum_devices(false)?;

    let valid_urls: Vec<String> = devices
        .into_iter()
        .filter_map(|dev| match dev.ifc {
            TwinleafPortInterface::STM32 | TwinleafPortInterface::FTDI => Some(dev.url),
            _ => None,
        })
        .collect();

    if valid_urls.is_empty() {
        Err(ConnectError::AutoDetectionFailed)
    } else if valid_urls.len() > 1 {
        Err(ConnectError::MultipleSensorsFound(valid_urls.len()))
    } else {
        Ok(valid_urls[0].clone())
    }
}