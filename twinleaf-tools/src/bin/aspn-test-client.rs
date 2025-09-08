use std::io::Read;
use std::net::TcpStream;

/// Simple test client for ASPN proxy
fn main() {
    println!("ASPN Test Client: Connecting to localhost:7802");
    
    match TcpStream::connect("localhost:7802") {
        Ok(mut stream) => {
            println!("ASPN Test Client: Connected successfully");
            
            let mut message_count = 0;
            loop {
                // Read message length (4 bytes)
                let mut length_buf = [0u8; 4];
                if let Err(_) = stream.read_exact(&mut length_buf) {
                    println!("ASPN Test Client: Failed to read message length");
                    break;
                }
                
                let message_length = u32::from_le_bytes(length_buf) as usize;
                println!("ASPN Test Client: Message {} - Length: {} bytes", message_count + 1, message_length);
                
                // Read the message data
                let mut message_buf = vec![0u8; message_length];
                if let Err(_) = stream.read_exact(&mut message_buf) {
                    println!("ASPN Test Client: Failed to read message data");
                    break;
                }
                
                // Parse and display the ASPN message
                if message_length >= 24 { // Minimum size for header + timestamp + num_meas
                    let vendor_id = u32::from_le_bytes([message_buf[0], message_buf[1], message_buf[2], message_buf[3]]);
                    let device_id = u64::from_le_bytes([
                        message_buf[4], message_buf[5], message_buf[6], message_buf[7],
                        message_buf[8], message_buf[9], message_buf[10], message_buf[11]
                    ]);
                    let context_id = u32::from_le_bytes([message_buf[12], message_buf[13], message_buf[14], message_buf[15]]);
                    let sequence_id = u16::from_le_bytes([message_buf[16], message_buf[17]]);
                    let timestamp = i64::from_le_bytes([
                        message_buf[18], message_buf[19], message_buf[20], message_buf[21],
                        message_buf[22], message_buf[23], message_buf[24], message_buf[25]
                    ]);
                    let num_meas = message_buf[26];
                    
                    println!("  Vendor ID: 0x{:08X}", vendor_id);
                    println!("  Device ID: 0x{:016X}", device_id);
                    println!("  Context ID: {}", context_id);
                    println!("  Sequence ID: {}", sequence_id);
                    println!("  Timestamp: {} ns", timestamp);
                    println!("  Num Measurements: {}", num_meas);
                    
                    // Parse field strengths if present
                    let mut offset = 27;
                    if offset < message_length {
                        let x_present = message_buf[offset] != 0;
                        offset += 1;
                        if x_present && offset + 8 <= message_length {
                            let x_field = f64::from_le_bytes([
                                message_buf[offset], message_buf[offset+1], message_buf[offset+2], message_buf[offset+3],
                                message_buf[offset+4], message_buf[offset+5], message_buf[offset+6], message_buf[offset+7]
                            ]);
                            println!("  X Field Strength: {:.6} nT", x_field);
                            offset += 8;
                        }
                    }
                    
                    if offset < message_length {
                        let y_present = message_buf[offset] != 0;
                        offset += 1;
                        if y_present && offset + 8 <= message_length {
                            let y_field = f64::from_le_bytes([
                                message_buf[offset], message_buf[offset+1], message_buf[offset+2], message_buf[offset+3],
                                message_buf[offset+4], message_buf[offset+5], message_buf[offset+6], message_buf[offset+7]
                            ]);
                            println!("  Y Field Strength: {:.6} nT", y_field);
                            offset += 8;
                        }
                    }
                    
                    if offset < message_length {
                        let z_present = message_buf[offset] != 0;
                        offset += 1;
                        if z_present && offset + 8 <= message_length {
                            let z_field = f64::from_le_bytes([
                                message_buf[offset], message_buf[offset+1], message_buf[offset+2], message_buf[offset+3],
                                message_buf[offset+4], message_buf[offset+5], message_buf[offset+6], message_buf[offset+7]
                            ]);
                            println!("  Z Field Strength: {:.6} nT", z_field);
                        }
                    }
                }
                
                message_count += 1;
                if message_count >= 5 { // Limit to 5 messages for testing
                    println!("ASPN Test Client: Received {} messages, disconnecting", message_count);
                    break;
                }
                
                println!();
            }
        }
        Err(e) => {
            println!("ASPN Test Client: Failed to connect: {}", e);
        }
    }
}
