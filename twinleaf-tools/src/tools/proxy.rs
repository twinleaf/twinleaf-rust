//! The `tio proxy` subcommand and its `stop`, `list` and `nmea` subcommands.
//!
//! [`run_proxy`] hosts the proxy of [`twinleaf::proxy`], which shares one or
//! more sensors over TCP. In a terminal it serves the sensors named on the
//! command line, or the one attached, as the default for every tool until
//! Ctrl-C. With `--mount`, each sensor hangs off a route prefix and clients
//! see the set as a single virtual hub.

mod list;
mod nmea;

pub use list::run_list;

use crate::tui::selector::PickedSubtree;
use crate::{MountArg, ProxyCli, ProxySubcommands};
#[cfg(feature = "mdns")]
use std::collections::BTreeMap;
use std::io;
use std::sync::Arc;
use std::time::Duration;
use twinleaf::device::discovery::{self, PortInterface};
use twinleaf::proxy::{self, DumpFlags, HostOptions, Layout, Listen, Mount, Proxy};
use twinleaf::{Connection, DeviceRoute, RecvError};

pub fn run_proxy(mut proxy_cli: ProxyCli) -> eyre::Result<()> {
    match proxy_cli.subcommands.take() {
        Some(ProxySubcommands::Stop) => stop(),
        Some(ProxySubcommands::List(cli)) => list::run_list(cli),
        Some(ProxySubcommands::Nmea { tio, tcp_port }) => nmea::run_nmea_proxy(tio, tcp_port),
        None => match proxy_cli.background {
            true => run_background(&proxy_cli),
            false => run_foreground(&proxy_cli),
        },
    }
}

/// Returns the options a terminal `tio list` starts from, which are those of
/// `tio proxy` with no arguments.
fn list_cli() -> ProxyCli {
    use clap::Parser;
    ProxyCli::parse_from(["tio-proxy"])
}

/// Holds the devices picked in `tio proxy list` as the default of the
/// machine's proxy for every tool until Ctrl-C, or until the proxy lets go.
///
/// # Errors
///
/// Returns an error if the proxy refuses the selection, the signal handler
/// cannot be installed, or the proxy stops first.
fn hold_selection(picked: Vec<PickedSubtree>) -> eyre::Result<()> {
    let cli = list_cli();
    crate::logging::release();
    let front = FrontDoor::picked(picked);
    let held = Connection::hold_default(&within_subtree(&front.layout, cli.subtree)?)?;
    let events = held.events();
    let closer = held.clone();
    ctrlc::set_handler(move || closer.close())?;
    front.print_startup(&cli);
    println!(
        "Using {} as the default while this runs. Ctrl-C to stop.",
        front.describe(cli.subtree)
    );
    loop {
        match events.recv() {
            Ok(_) | Err(RecvError::Lagged(_)) => {}
            Err(RecvError::Closed) => return Ok(()),
            Err(RecvError::Stopped) => return Err(eyre::eyre!("the proxy stopped")),
            Err(error) => return Err(eyre::eyre!("the proxy let the selection go: {error}")),
        }
    }
}

/// Serves the sensors the arguments name, or the one attached, from a terminal
/// until Ctrl-C.
fn run_foreground(cli: &ProxyCli) -> eyre::Result<()> {
    let requested = requested_layout(cli.sensor_url.as_deref(), &cli.mounts)?;
    let default = requested
        .as_ref()
        .map(|layout| within_subtree(layout, cli.subtree))
        .transpose()?;
    // Hosting comes first, so that a proxy already running is reported
    // rather than whatever detecting the device finds.
    let proxy = host(HostOptions {
        default,
        ..host_options(cli, "tio proxy")
    })?;
    let front = match requested {
        Some(layout) => FrontDoor::named(layout),
        None => {
            let url = match auto_detect_serial(&proxy) {
                Ok(url) => url,
                Err(error) => {
                    proxy.shutdown();
                    return Err(error);
                }
            };
            let front = FrontDoor {
                auto_detected: true,
                ..FrontDoor::named(whole_device(&url))
            };
            proxy.set_default(Some(within_subtree(&front.layout, cli.subtree)?));
            front
        }
    };
    serve(proxy, &front, cli)
}

/// Hosts the proxy in the background, as a tool starts it, until it idles
/// out or `tio proxy stop` stops it. Returns `Ok(())` without hosting if
/// another proxy already holds the port.
fn run_background(cli: &ProxyCli) -> eyre::Result<()> {
    let default = default_layout(cli)?;
    let hosted = Proxy::host(HostOptions {
        listen: Listen::Loopback,
        idle_exit: true,
        stoppable: true,
        default,
        ..host_options(cli, "a background tio proxy")
    });
    let proxy = match hosted {
        Ok(proxy) => proxy,
        Err(error) if error.kind() == io::ErrorKind::AddrInUse => {
            log::info!("{error}, so this one exits");
            return Ok(());
        }
        Err(error) => return Err(error.into()),
    };
    proxy.wait();
    Ok(())
}

/// Stops the background proxy that tools go through.
fn stop() -> eyre::Result<()> {
    proxy::stop(None)?;
    println!("Stopped the background proxy");
    Ok(())
}

/// Returns the options a terminal hosts the proxy with, from `-p`, `-T`, `-k`
/// and the `--dump` flags. `name` says who hosts it.
fn host_options(cli: &ProxyCli, name: &str) -> HostOptions {
    HostOptions {
        listen: Listen::AllInterfaces,
        port: cli.port,
        name: format!("{name} (pid {})", std::process::id()),
        reconnect_timeout: cli.reconnect_timeout.map(Duration::from_secs),
        kick_slow: cli.kick_slow,
        dump: DumpFlags {
            traffic: cli.dump,
            data: cli.dump_data,
            meta: cli.dump_meta,
            heartbeats: cli.dump_hb,
        },
        ..HostOptions::default()
    }
}

/// Hosts a proxy with `options`. When the port is taken, the error says
/// whether a proxy or something else, such as a 1.x `tio proxy`, holds it,
/// and suggests what to do.
fn host(options: HostOptions) -> eyre::Result<Proxy> {
    use color_eyre::Help;

    Proxy::host(options).map_err(|error| match error.kind() {
        io::ErrorKind::AddrInUse => eyre::Report::new(error)
            .suggestion("a proxy there already shares devices, so pass -r <url> to reach one")
            .suggestion(
                "'tio proxy stop' ends a proxy a tio command started, \
                 and names the terminal or app hosting any other",
            )
            .suggestion("or serve on another port with --port <N>"),
        _ => error.into(),
    })
}

/// Returns a layout of the whole device at `url`, seen at the root.
fn whole_device(url: &str) -> Layout {
    Layout {
        mounts: vec![Mount {
            url: url.to_string(),
            source: DeviceRoute::root(),
            prefix: DeviceRoute::root(),
        }],
    }
}

/// Returns the layout the arguments name, with each source as given before
/// `-s` applies, or `None` when they name no device.
fn requested_layout(
    sensor_url: Option<&str>,
    mount_args: &[MountArg],
) -> eyre::Result<Option<Layout>> {
    if let Some(url) = sensor_url {
        return Ok(Some(whole_device(url)));
    }
    if mount_args.is_empty() {
        return Ok(None);
    }
    let mut prefixes = std::collections::HashSet::new();
    for arg in mount_args {
        if !prefixes.insert(arg.prefix) {
            return Err(eyre::eyre!("duplicate mount prefix {}", arg.prefix));
        }
    }
    Ok(Some(Layout {
        mounts: mount_args
            .iter()
            .map(|arg| Mount {
                url: arg.locator.clone(),
                source: DeviceRoute::root(),
                prefix: arg.prefix,
            })
            .collect(),
    }))
}

/// Returns the layout the arguments name, as the proxy mounts it, or `None`
/// when they name no device.
fn default_layout(cli: &ProxyCli) -> eyre::Result<Option<Layout>> {
    requested_layout(cli.sensor_url.as_deref(), &cli.mounts)?
        .map(|layout| within_subtree(&layout, cli.subtree))
        .transpose()
}

/// Returns `layout` with each source taken within `subtree`, as the proxy
/// mounts it.
fn within_subtree(layout: &Layout, subtree: DeviceRoute) -> eyre::Result<Layout> {
    let mounts = layout
        .mounts
        .iter()
        .map(|mount| {
            Ok(Mount {
                source: mount.source.absolute_route(&subtree)?,
                ..mount.clone()
            })
        })
        .collect::<eyre::Result<_>>()?;
    Ok(Layout { mounts })
}

/// Returns the URL of the one Twinleaf serial device attached, for
/// `tio proxy` with no URL. With several attached, it prints each one with
/// the name read through `proxy`, and returns an error.
fn auto_detect_serial(proxy: &Proxy) -> eyre::Result<String> {
    use color_eyre::Help;

    let mut valid_urls = Vec::new();
    for dev in discovery::enumerate_serial(false) {
        if matches!(dev.interface, PortInterface::STM32 | PortInterface::FTDI) {
            valid_urls.push(dev.url);
        }
    }
    if valid_urls.is_empty() {
        return Err(eyre::eyre!("no sensors detected").suggestion(
            "specify a URL, or run 'tio proxy list' to discover devices on the network",
        ));
    }
    if valid_urls.len() > 1 {
        eprintln!("multiple sensors detected:");
        let query_timeout = Duration::from_millis(500);
        for url in &valid_urls {
            match discovery::query_name_via(&proxy.endpoint(), url, query_timeout) {
                Some(name) => eprintln!("  {}  {}", url, name),
                None => eprintln!("  {}  (no response)", url),
            }
        }
        return Err(eyre::eyre!("multiple sensors detected, cannot auto-select")
            .suggestion("pick one interactively with 'tio proxy list', or specify a URL")
            .suggestion("or mount each at a route prefix with --mount <url>=/N"));
    }
    Ok(valid_urls.swap_remove(0))
}

/// What a terminal serves as the default while it runs.
struct FrontDoor {
    /// The mounts as chosen, each source before `-s` applies.
    layout: Layout,
    /// The name of each mount's device, when the picker knew it.
    names: Vec<Option<String>>,
    /// Whether the one device was auto-detected.
    auto_detected: bool,
}

impl FrontDoor {
    /// Creates a `FrontDoor` of the mounts of `layout`, named on the command
    /// line.
    fn named(layout: Layout) -> FrontDoor {
        FrontDoor {
            names: vec![None; layout.mounts.len()],
            layout,
            auto_detected: false,
        }
    }

    /// Creates a `FrontDoor` of the subtrees picked in `tio proxy list`.
    fn picked(picked: Vec<PickedSubtree>) -> FrontDoor {
        let (mounts, names) = picked
            .into_iter()
            .map(
                |PickedSubtree {
                     device,
                     source,
                     prefix,
                 }| {
                    let mount = Mount {
                        url: device.url,
                        source,
                        prefix,
                    };
                    (mount, device.name)
                },
            )
            .unzip();
        FrontDoor {
            layout: Layout { mounts },
            names,
            auto_detected: false,
        }
    }

    /// Describes the sensors served, and the subtree when it is not the root.
    fn describe(&self, subtree: DeviceRoute) -> String {
        let sensors = self
            .layout
            .mounts
            .iter()
            .zip(&self.names)
            .map(|(mount, name)| {
                format!(
                    "{} ({} → {})",
                    name.as_deref().unwrap_or(&mount.url),
                    mount.source,
                    mount.prefix
                )
            })
            .collect::<Vec<_>>()
            .join(", ");
        match subtree.is_empty() {
            true => sensors,
            false => format!("{sensors} at {subtree}"),
        }
    }

    fn print_startup(&self, cli: &ProxyCli) {
        let mounts = &self.layout.mounts;
        match self.names.as_slice() {
            [Some(name)] => println!("tio proxy starting (selected {name}):"),
            _ => println!("tio proxy starting:"),
        }
        if mounts.len() == 1 && mounts[0].prefix.is_empty() {
            println!(
                "  Sensor: {}{}",
                mounts[0].url,
                if self.auto_detected {
                    " (auto-detected)"
                } else {
                    ""
                }
            );
            if !mounts[0].source.is_empty() {
                println!(
                    "  Mapping: {} → / (including descendants)",
                    mounts[0].source
                );
            }
        } else {
            println!("  Mounts:");
            for (mount, name) in mounts.iter().zip(&self.names) {
                println!(
                    "    {}  {} → {}  ({})",
                    mount.url,
                    mount.source,
                    mount.prefix,
                    name.as_deref().unwrap_or("device")
                );
            }
        }
        println!("  TCP port: {}", cli.port);
        println!("  Subtree: {}", cli.subtree);

        let flags = [
            ("verbose", cli.verbose),
            ("debug", cli.debug),
            ("kick-slow", cli.kick_slow),
            ("dump", cli.dump),
            ("dump-data", cli.dump_data),
            ("dump-meta", cli.dump_meta),
            ("dump-hb", cli.dump_hb),
        ];
        let enabled: Vec<&str> = flags
            .iter()
            .filter_map(|&(name, on)| on.then_some(name))
            .collect();
        if !enabled.is_empty() {
            println!("  Flags: {}", enabled.join(" "));
        }
        println!();
    }
}

/// Serves `front` through `proxy` as the default for every tool until
/// Ctrl-C, SIGTERM or SIGHUP. Each client then hears that the proxy stopped.
/// A second signal quits at once. A SIGHUP that was already ignored, as under
/// `nohup`, does not stop it.
fn serve(proxy: Proxy, front: &FrontDoor, cli: &ProxyCli) -> eyre::Result<()> {
    let proxy = Arc::new(proxy);
    stop_on_signals(proxy.clone())?;

    front.print_startup(cli);
    // The advertisement lasts until this returns, then says goodbye.
    #[cfg(feature = "mdns")]
    let _mdns = advertise_mdns(&proxy, cli);
    println!(
        "Using {} as the default while this runs. Ctrl-C to stop.",
        front.describe(cli.subtree)
    );

    proxy.wait();
    log::info!("tio proxy stopped");
    Ok(())
}

/// Stops `proxy` on the first SIGINT, SIGTERM or SIGHUP, and exits the
/// process at once with status 130 on the next. A SIGHUP that was already
/// ignored, as under `nohup`, stays ignored.
fn stop_on_signals(proxy: Arc<Proxy>) -> eyre::Result<()> {
    #[cfg(unix)]
    let hangup = hangup_disposition();
    let mut stopping = false;
    ctrlc::set_handler(move || {
        if std::mem::replace(&mut stopping, true) {
            eprintln!("Quitting at once.");
            std::process::exit(130);
        }
        eprintln!("Stopping. Press Ctrl-C again to quit at once.");
        // The handler must return to hear the next signal, and the stop
        // blocks until it is done.
        let stopper = proxy.clone();
        let stop = std::thread::Builder::new().name("tio proxy stop".to_string());
        if let Err(error) = stop.spawn(move || stopper.shutdown()) {
            log::warn!("Stopping the proxy in the signal handler, as {error}");
            proxy.shutdown();
        }
    })?;
    #[cfg(unix)]
    if let Some(ignored) = hangup.filter(|action| action.sa_sigaction == libc::SIG_IGN) {
        // SAFETY: the action restored is the one the kernel reported, which
        // ignores the signal and runs no code of this process.
        if unsafe { libc::sigaction(libc::SIGHUP, &ignored, std::ptr::null_mut()) } != 0 {
            log::warn!(
                "SIGHUP will stop tio proxy, though it was ignored: {}",
                io::Error::last_os_error()
            );
        }
    }
    Ok(())
}

/// Returns the current action for SIGHUP, or `None` if it cannot be read.
#[cfg(unix)]
fn hangup_disposition() -> Option<libc::sigaction> {
    // SAFETY: an all zero sigaction is a valid value to be overwritten, and a
    // null new action only reads the current one, changing nothing.
    unsafe {
        let mut current: libc::sigaction = std::mem::zeroed();
        (libc::sigaction(libc::SIGHUP, std::ptr::null(), &mut current) == 0).then_some(current)
    }
}

/// Advertises the proxy over mDNS when `--mdns` asks. The returned guard
/// keeps the advertisement live until dropped.
#[cfg(feature = "mdns")]
fn advertise_mdns(proxy: &Proxy, cli: &ProxyCli) -> Option<MdnsService> {
    if !cli.mdns {
        return None;
    }
    let host = gethostname::gethostname().to_string_lossy().into_owned();
    let instance = mdns_instance_name(proxy, &host);
    match advertise_tcp(&instance, &host, cli.port) {
        Some(service) => {
            log::info!("advertising over mDNS as \"{instance}\"");
            Some(service)
        }
        None => {
            log::warn!("could not advertise over mDNS");
            None
        }
    }
}

/// Returns the name other hosts see in `tio proxy list`, which counts the
/// sensor models the proxy serves by default, as in `VMR (x2)`. COMM and HUB
/// routing devices are not counted. Returns `tio-proxy (host)` when no
/// sensors are found.
#[cfg(feature = "mdns")]
fn mdns_instance_name(proxy: &Proxy, host: &str) -> String {
    if let Some(summary) = summarize_sensors(&discover_sensor_models(proxy)) {
        return summary;
    }
    let short = host.trim_end_matches('.').trim_end_matches(".local");
    format!("tio-proxy ({short})")
}

/// How long to listen for heartbeats to discover the connected sensors.
#[cfg(feature = "mdns")]
const MDNS_ROUTE_WINDOW: Duration = Duration::from_millis(500);

/// A live mDNS advertisement. Dropping it stops the responder and sends
/// goodbye packets, which tell other hosts the proxy is gone.
#[cfg(feature = "mdns")]
pub struct MdnsService {
    responder: mdns_sd::ServiceDaemon,
    fullname: String,
}

#[cfg(feature = "mdns")]
impl Drop for MdnsService {
    fn drop(&mut self) {
        let _ = self.responder.unregister(&self.fullname);
        let _ = self.responder.shutdown();
    }
}

/// Advertises a `_twinleaf._tcp` service on the local network, where
/// `tio proxy list` on other hosts finds it.
///
/// `instance` is the human-readable name shown to browsers, `host_name` is the
/// machine's name (a `.local.` suffix is added if missing), and `port` is the
/// TCP port being served. The host's addresses are filled in automatically.
/// Keep the returned guard alive for as long as the service should be
/// advertised. Returns `None` if the responder can't be set up.
#[cfg(feature = "mdns")]
fn advertise_tcp(instance: &str, host_name: &str, port: u16) -> Option<MdnsService> {
    use mdns_sd::{ServiceDaemon, ServiceInfo};
    use std::collections::HashMap;

    let host = {
        let base = host_name.trim_end_matches('.').trim_end_matches(".local");
        format!("{base}.local.")
    };

    let responder = ServiceDaemon::new().ok()?;
    let info = ServiceInfo::new(
        "_twinleaf._tcp.local.",
        instance,
        &host,
        (),
        port,
        None::<HashMap<String, String>>,
    )
    .ok()?
    // Publish the host's current addresses (and track interface changes).
    .enable_addr_auto();

    let fullname = info.get_fullname().to_string();
    responder.register(info).ok()?;
    Some(MdnsService {
        responder,
        fullname,
    })
}

/// Returns the `dev.name` of each device the proxy serves by default, read
/// over a connection to the proxy itself.
#[cfg(feature = "mdns")]
fn discover_sensor_models(proxy: &Proxy) -> Vec<String> {
    match twinleaf::Connection::open(&proxy.endpoint()) {
        Ok(connection) => connection
            .tree(DeviceRoute::root())
            .named_routes(MDNS_ROUTE_WINDOW)
            .into_iter()
            .filter_map(|nr| nr.name)
            .collect(),
        Err(error) => {
            log::warn!("could not connect to the proxy to name it over mDNS: {error}");
            Vec::new()
        }
    }
}

/// Summarizes device names as a sensor count for the mDNS instance name,
/// ignoring COMM and HUB routing devices. For example,
/// `["HUB-USB-TDS", "COMM-RS422", "VMR", "VMR"]` becomes `"VMR (x2)"`.
/// Several models are joined with `", "`. Returns `None` if no sensors
/// remain.
#[cfg(feature = "mdns")]
fn summarize_sensors(models: &[String]) -> Option<String> {
    let mut counts: BTreeMap<&str, usize> = BTreeMap::new();
    for model in models {
        if !is_infrastructure(model) {
            *counts.entry(model.as_str()).or_default() += 1;
        }
    }
    if counts.is_empty() {
        return None;
    }
    Some(
        counts
            .iter()
            .map(|(model, &n)| {
                if n > 1 {
                    format!("{model} (x{n})")
                } else {
                    (*model).to_string()
                }
            })
            .collect::<Vec<_>>()
            .join(", "),
    )
}

/// Returns `true` if `name` is a COMM or HUB routing device, not a sensor.
#[cfg(feature = "mdns")]
fn is_infrastructure(name: &str) -> bool {
    let upper = name.to_uppercase();
    upper.starts_with("COMM") || upper.starts_with("HUB")
}

#[cfg(all(test, feature = "mdns"))]
mod mdns_tests {
    use super::{is_infrastructure, summarize_sensors};

    fn models(names: &[&str]) -> Vec<String> {
        names.iter().map(|s| s.to_string()).collect()
    }

    #[test]
    fn excludes_comm_and_hub_routes() {
        assert!(is_infrastructure("COMM-RS422"));
        assert!(is_infrastructure("COMM-USB-RS422"));
        assert!(is_infrastructure("HUB-USB-TDS"));
        assert!(!is_infrastructure("VMR"));
        assert!(!is_infrastructure("AXIS"));
    }

    #[test]
    fn counts_duplicate_sensors() {
        // A hub with two VMRs -> "VMR (x2)", COMM/HUB left out.
        let got = summarize_sensors(&models(&["HUB-USB-TDS", "COMM-RS422", "VMR", "VMR"]));
        assert_eq!(got.as_deref(), Some("VMR (x2)"));
    }

    #[test]
    fn single_sensor_has_no_count_suffix() {
        let got = summarize_sensors(&models(&["HUB-USB-TDS", "COMM-RS422", "VMR"]));
        assert_eq!(got.as_deref(), Some("VMR"));
    }

    #[test]
    fn multiple_models_are_joined_sorted() {
        let got = summarize_sensors(&models(&["VMR", "VMR", "AXIS", "COMM-RS422"]));
        assert_eq!(got.as_deref(), Some("AXIS, VMR (x2)"));
    }

    #[test]
    fn no_sensors_yields_none() {
        assert_eq!(
            summarize_sensors(&models(&["HUB-USB-TDS", "COMM-RS422"])),
            None
        );
        assert_eq!(summarize_sensors(&[]), None);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn route(path: &str) -> DeviceRoute {
        path.parse().unwrap()
    }

    fn mount(url: &str, source: &str, prefix: &str) -> Mount {
        Mount {
            url: url.to_string(),
            source: route(source),
            prefix: route(prefix),
        }
    }

    fn mount_arg(locator: &str, prefix: &str) -> MountArg {
        MountArg {
            locator: locator.to_string(),
            prefix: route(prefix),
        }
    }

    #[test]
    fn no_url_and_no_mounts_name_no_layout() {
        assert_eq!(requested_layout(None, &[]).unwrap(), None);
    }

    #[test]
    fn a_url_is_the_whole_device_at_the_root() {
        let layout = requested_layout(Some("tcp://192.0.2.1"), &[]).unwrap();

        let mounts = vec![mount("tcp://192.0.2.1", "/", "/")];
        assert_eq!(layout, Some(Layout { mounts }));
    }

    #[test]
    fn mounts_keep_their_order_and_prefixes() {
        let args = [
            mount_arg("serial:///dev/ttyACM0", "/2"),
            mount_arg("tcp://192.0.2.1", "/1"),
        ];

        let layout = requested_layout(None, &args).unwrap();

        let mounts = vec![
            mount("serial:///dev/ttyACM0", "/", "/2"),
            mount("tcp://192.0.2.1", "/", "/1"),
        ];
        assert_eq!(layout, Some(Layout { mounts }));
    }

    #[test]
    fn a_prefix_mounted_twice_is_refused() {
        let args = [
            mount_arg("serial:///dev/ttyACM0", "/1"),
            mount_arg("tcp://192.0.2.1", "/1"),
        ];

        let error = requested_layout(None, &args).unwrap_err();

        assert!(error.to_string().contains("duplicate mount prefix /1"));
    }

    #[test]
    fn the_subtree_becomes_each_source() {
        let layout = whole_device("tcp://192.0.2.1");

        let hosted = within_subtree(&layout, route("/3")).unwrap();

        let mounts = vec![mount("tcp://192.0.2.1", "/3", "/")];
        assert_eq!(hosted, Layout { mounts });
    }

    #[test]
    fn a_picked_source_stays_as_picked_under_the_root() {
        let layout = Layout {
            mounts: vec![mount("tcp://192.0.2.1", "/2", "/1")],
        };

        let hosted = within_subtree(&layout, DeviceRoute::root()).unwrap();

        assert_eq!(hosted, layout);
    }
}
