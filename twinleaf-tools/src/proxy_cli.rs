#[derive(Parser, Debug)]
#[command(
    name = "tio-proxy",
    version,
    about = "Multiplexes access to a sensor, exposing the functionality of tio::proxy via TCP"
)]
pub struct ProxyCli {
    /// Sensor URL (e.g., tcp://localhost, serial:///dev/ttyUSB0)
    /// Required unless --auto or --enum is specified
    sensor_url: Option<String>,

    /// TCP port to listen on for clients
    #[arg(short = 'p', long = "port", default_value = "7855")]
    port: u16,

    /// Kick off slow clients instead of dropping traffic
    #[arg(short = 'k', long)]
    kick_slow: bool,

    /// Sensor subtree to look at
    #[arg(short = 's', long = "subtree", default_value = "/")]
    subtree: String,

    /// Verbose output
    #[arg(short = 'v', long)]
    verbose: bool,

    /// Debugging output
    #[arg(short = 'd', long)]
    debug: bool,

    /// Timestamp format
    #[arg(short = 't', long = "timestamp", default_value = "%T%.3f ")]
    timestamp_format: String,

    /// Time limit for sensor reconnection attempts (seconds)
    #[arg(short = 'T', long = "timeout", default_value = "30")]
    reconnect_timeout: u64,

    /// Dump packet traffic except sample data/metadata or heartbeats
    #[arg(long)]
    dump: bool,

    /// Dump sample data traffic
    #[arg(long)]
    dump_data: bool,

    /// Dump sample metadata traffic
    #[arg(long)]
    dump_meta: bool,

    /// Dump heartbeat traffic
    #[arg(long)]
    dump_hb: bool,

    #[arg(short = 'a', long = "auto")]
    auto: bool,

    /// Enumerate all serial devices, then quit
    #[arg(short = 'e', long = "enumerate", name = "enum")]
    enumerate: bool,
}