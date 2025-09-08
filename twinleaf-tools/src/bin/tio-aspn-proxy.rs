use std::io::Write;
use std::net::{TcpListener, TcpStream};
use std::{env, thread};
use twinleaf::tio;
use twinleaf_tools::{tio_opts, tio_parseopts};

/// ASPN (Advanced Sensor Protocol for Navigation) format implementation
/// Based on the data-model-repository specification

// ASPN Message Types
const ASPN_MEASUREMENT_MAGNETIC_FIELD: u64 = 0x23000001; // Twinleaf vendor ID + magnetic field device ID

/// ASPN Header structure
#[derive(Debug, Clone)]
struct AspnHeader {
    vendor_id: u32,    // Twinleaf vendor ID
    device_id: u64,    // Device type identifier
    context_id: u32,   // Stream context identifier
    sequence_id: u16,  // Message sequence number
}

/// ASPN Timestamp structure
#[derive(Debug, Clone)]
struct AspnTimestamp {
    elapsed_nsec: i64, // Nanoseconds since epoch
}

/// ASPN Magnetic Field Measurement
#[derive(Debug, Clone)]
struct AspnMagneticFieldMeasurement {
    header: AspnHeader,
    time_of_validity: AspnTimestamp,
    num_meas: u8,
    x_field_strength: Option<f64>,
    y_field_strength: Option<f64>,
    z_field_strength: Option<f64>,
    covariance: Vec<f64>, // num_meas x num_meas matrix
    error_model: u32,     // Error model enum
    num_error_model_params: u16,
    error_model_params: Vec<f64>,
    num_integrity: u8,
    // integrity: Vec<AspnIntegrity>, // Simplified for now
}

impl AspnMagneticFieldMeasurement {
    fn new(sample: &twinleaf::data::Sample, sequence_id: u16) -> Self {
        let timestamp_ns = (sample.timestamp_end() * 1e9) as i64;
        println!("Sample: {:?}", sample);

        // Extract magnetic field data from sample columns
        let mut x_field = None;
        let mut y_field = None;
        let mut z_field = None;
        let mut num_meas = 0u8;
        
        for col in &sample.columns {
            match col.desc.name.to_lowercase().as_str() {
                "x" | "bx" | "x_field" => {
                    if let Some(val) = col.value.try_as_f64() {
                        x_field = Some(val);
                        num_meas = num_meas.max(1);
                    }
                }
                "y" | "by" | "y_field" => {
                    if let Some(val) = col.value.try_as_f64() {
                        y_field = Some(val);
                        num_meas = num_meas.max(2);
                    }
                }
                "z" | "bz" | "z_field" => {
                    if let Some(val) = col.value.try_as_f64() {
                        z_field = Some(val);
                        num_meas = num_meas.max(3);
                    }
                }
                _ => {}
            }
        }
        
        // Create covariance matrix (identity for now)
        let mut covariance = vec![0.0; (num_meas * num_meas) as usize];
        for i in 0..num_meas {
            covariance[(i * num_meas + i) as usize] = 1.0; // Identity matrix
        }
        
        AspnMagneticFieldMeasurement {
            header: AspnHeader {
                vendor_id: 0x23000000, // Twinleaf vendor ID
                device_id: ASPN_MEASUREMENT_MAGNETIC_FIELD,
                context_id: sample.stream.stream_id as u32,
                sequence_id,
            },
            time_of_validity: AspnTimestamp {
                elapsed_nsec: timestamp_ns,
            },
            num_meas,
            x_field_strength: x_field,
            y_field_strength: y_field,
            z_field_strength: z_field,
            covariance,
            error_model: 0, // NONE
            num_error_model_params: 0,
            error_model_params: vec![],
            num_integrity: 0,
        }
    }
    
    fn to_binary(&self) -> Vec<u8> {
        let mut data = Vec::new();
        
        // Header
        data.extend_from_slice(&self.header.vendor_id.to_le_bytes());
        data.extend_from_slice(&self.header.device_id.to_le_bytes());
        data.extend_from_slice(&self.header.context_id.to_le_bytes());
        data.extend_from_slice(&self.header.sequence_id.to_le_bytes());
        
        // Timestamp
        data.extend_from_slice(&self.time_of_validity.elapsed_nsec.to_le_bytes());
        
        // Measurement data
        data.push(self.num_meas);
        
        // Field strengths (optional fields)
        if let Some(x) = self.x_field_strength {
            data.push(1); // Present
            data.extend_from_slice(&x.to_le_bytes());
        } else {
            data.push(0); // Not present
        }
        
        if let Some(y) = self.y_field_strength {
            data.push(1); // Present
            data.extend_from_slice(&y.to_le_bytes());
        } else {
            data.push(0); // Not present
        }
        
        if let Some(z) = self.z_field_strength {
            data.push(1); // Present
            data.extend_from_slice(&z.to_le_bytes());
        } else {
            data.push(0); // Not present
        }
        
        // Covariance matrix
        for &val in &self.covariance {
            data.extend_from_slice(&val.to_le_bytes());
        }
        
        // Error model
        data.extend_from_slice(&self.error_model.to_le_bytes());
        data.extend_from_slice(&self.num_error_model_params.to_le_bytes());
        
        // Error model parameters
        for &val in &self.error_model_params {
            data.extend_from_slice(&val.to_le_bytes());
        }
        
        // Integrity
        data.push(self.num_integrity);
        
        data
    }
}

fn broadcast_to_client(mut stream: TcpStream, port: tio::proxy::Port) {
    use twinleaf::data::Device;
    let mut device = Device::new(port);
    let peer_addr = stream.peer_addr().unwrap();
    println!("ASPN Proxy: Connection from: {}", peer_addr);
    
    let mut sequence_id = 0u16;

    loop {
        let sample = match device.next() {
            Ok(sample) => sample,
            Err(_) => {
                eprintln!("Failed to parse sample");
                break;
            }
        };

        // Only process samples from stream ID 1
        if sample.stream.stream_id != 1 {
            continue;
        }

        // Create ASPN magnetic field measurement
        let aspn_measurement = AspnMagneticFieldMeasurement::new(&sample, sequence_id);
        let binary_data = aspn_measurement.to_binary();
        
        // Send message length first (4 bytes)
        let length = binary_data.len() as u32;
        if let Err(_) = stream.write_all(&length.to_le_bytes()) {
            break;
        }
        
        // Send the ASPN message
        if let Err(_) = stream.write_all(&binary_data) {
            break;
        }
        
        sequence_id = sequence_id.wrapping_add(1);
    }
    println!("ASPN Proxy: Disconnected: {}", peer_addr);
}

fn main() {
    let opts = tio_opts();
    let args: Vec<String> = env::args().collect();
    let (_matches, root, route) = tio_parseopts(&opts, &args);

    let proxy = tio::proxy::Interface::new(&root);

    println!("ASPN Proxy: Starting server on 0.0.0.0:7802");
    println!("ASPN Proxy: Sending binary ASPN magnetic field measurements");
    let listener = TcpListener::bind("0.0.0.0:7802").unwrap();
    for connection in listener.incoming() {
        if let Ok(stream) = connection {
            let device = proxy.device_full(route.clone()).unwrap();
            thread::spawn(move || broadcast_to_client(stream, device));
        }
    }
}
