//! ASPN MQTT proxy tool.
//!
//! Reads Twinleaf sensor data and re-publishes it to an MQTT broker using
//! ASPN-compatible stream names and JSON payloads.

use crate::TioOpts;
use rumqttc::{Client, MqttOptions, QoS};
use serde_json::json;
use std::collections::HashMap;
use std::thread;
use std::time::Duration;
use twinleaf::data::Sample;
use twinleaf::device::Device;
use twinleaf::tio;

// ---------------------------------------------------------------------------
// Stream name translation table
// ---------------------------------------------------------------------------

/// Built-in Twinleaf → ASPN stream name mappings.
///
/// Each entry is `(twinleaf_name, aspn_name)`.  To add a new mapping, append
/// an entry here.
static DEFAULT_MAPPINGS: &[(&str, &str)] = &[
    ("field", "measurement_magnetic_field_magnitude"),
    ("vector", "measurement_magnetic_field"),
    ("bar", "measurement_barometer"),
];

/// Bidirectional lookup table between Twinleaf and ASPN stream names.
struct StreamNameMap {
    tl_to_aspn: HashMap<String, String>,
}

impl StreamNameMap {
    /// Create a map pre-populated with [`DEFAULT_MAPPINGS`].
    fn new() -> Self {
        let mut tl_to_aspn = HashMap::new();
        for &(tl, aspn) in DEFAULT_MAPPINGS {
            tl_to_aspn.insert(tl.to_string(), aspn.to_string());
        }
        Self { tl_to_aspn }
    }

    /// Look up the ASPN name for a Twinleaf stream name.
    fn to_aspn(&self, twinleaf_name: &str) -> Option<&str> {
        self.tl_to_aspn.get(twinleaf_name).map(|s| s.as_str())
    }

    /// Iterate over all mappings as `(twinleaf_name, aspn_name)`.
    fn iter(&self) -> impl Iterator<Item = (&str, &str)> {
        self.tl_to_aspn
            .iter()
            .map(|(k, v)| (k.as_str(), v.as_str()))
    }
}

// ---------------------------------------------------------------------------
// JSON serialisation
// ---------------------------------------------------------------------------

/// Format a [`Sample`] as a JSON string for MQTT publication.
fn sample_to_json(sample: &Sample, aspn_name: &str) -> String {
    let columns: Vec<serde_json::Value> = sample
        .columns
        .iter()
        .map(|col| {
            json!({
                "name": col.desc.name,
                "value": col.value.try_as_f64(),
                "units": col.desc.units,
            })
        })
        .collect();

    let msg = json!({
        "measurement": aspn_name,
        "timestamp": sample.timestamp_end(),
        "sample_number": sample.n,
        "device": sample.device.name,
        "serial": sample.device.serial_number,
        "stream": sample.stream.name,
        "segment_id": sample.segment.segment_id,
        "sampling_rate": sample.segment.sampling_rate,
        "columns": columns,
    });

    // serde_json::to_string should never fail for these types
    serde_json::to_string(&msg).unwrap_or_default()
}

// ---------------------------------------------------------------------------
// Entry point
// ---------------------------------------------------------------------------

pub fn run_aspn_proxy(
    tio: TioOpts,
    mqtt_host: String,
    mqtt_port: u16,
    client_id: String,
    topic_prefix: Option<String>,
    verbose: bool,
) -> Result<(), ()> {
    let name_map = StreamNameMap::new();

    if verbose {
        eprintln!("ASPN stream name mappings:");
        for (tl, aspn) in name_map.iter() {
            eprintln!("  {} -> {}", tl, aspn);
        }
    }

    // Connect to Twinleaf device
    let proxy = tio::proxy::Interface::new(&tio.root);
    let route = tio.parse_route();
    let port = proxy.device_full(route).map_err(|e| {
        eprintln!("Failed to connect to Twinleaf device: {:?}", e);
    })?;
    let mut device = Device::new(port);

    if verbose {
        eprintln!("Connected to Twinleaf device at {}", tio.root);
    }

    // Connect to MQTT broker
    let mut mqtt_opts = MqttOptions::new(&client_id, &mqtt_host, mqtt_port);
    mqtt_opts.set_keep_alive(Duration::from_secs(30));

    let (mqtt_client, mut mqtt_connection) = Client::new(mqtt_opts, 256);

    if verbose {
        eprintln!("Connecting to MQTT broker at {}:{}", mqtt_host, mqtt_port);
    }

    // Spin the MQTT event loop in a background thread
    thread::spawn(move || {
        for notification in mqtt_connection.iter() {
            match notification {
                Ok(_) => {}
                Err(e) => {
                    eprintln!("ASPN MQTT connection error: {}", e);
                }
            }
        }
    });

    eprintln!(
        "ASPN proxy running: {} -> mqtt://{}:{}",
        tio.root, mqtt_host, mqtt_port
    );

    // Main loop: read samples and publish
    loop {
        let sample = match device.next() {
            Ok(s) => s,
            Err(e) => {
                eprintln!("Twinleaf device error: {:?}", e);
                return Err(());
            }
        };

        // Translate stream name -> ASPN name
        let aspn_name = match name_map.to_aspn(&sample.stream.name) {
            Some(name) => name,
            None => continue, // no mapping for this stream, skip
        };

        let topic = match &topic_prefix {
            Some(prefix) => format!("{}/{}", prefix, aspn_name),
            None => aspn_name.to_string(),
        };

        let payload = sample_to_json(&sample, aspn_name);

        if verbose {
            eprintln!("-> {} ({} bytes)", topic, payload.len());
        }

        if let Err(e) = mqtt_client.publish(&topic, QoS::AtMostOnce, false, payload.as_bytes()) {
            eprintln!("ASPN MQTT publish error: {}", e);
        }
    }
}
