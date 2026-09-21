use crate::{ProxyHelp, TioOpts};
use std::collections::HashSet;
use std::fs::File;
use std::io::Write;
use std::time::{Duration, Instant};
use twinleaf::device::record::{RecordMode, Recorder, Step};
use twinleaf::tio;
use twinleaf::Connection;
use twinleaf::DeviceRoute;

fn ensure_open<'a>(fo: &'a mut Option<File>, path: &str) -> eyre::Result<&'a mut File> {
    use eyre::WrapErr;
    if fo.is_none() {
        *fo = Some(
            File::create(path).wrap_err_with(|| format!("could not create log file {}", path))?,
        );
    }
    Ok(fo.as_mut().unwrap())
}

pub fn log(
    tio: &TioOpts,
    file: String,
    unbuffered: bool,
    raw: bool,
    depth: Option<usize>,
    duration: Option<Duration>,
) -> eyre::Result<()> {
    use eyre::WrapErr;
    use indicatif::{ProgressBar, ProgressStyle};
    use std::path::Path;

    let connection = Connection::open(&tio.root)?;
    let tree = connection.tree(tio.route);
    let tree = depth.map_or_else(|| tree.clone(), |depth| tree.to_depth(depth));

    let file_name = Path::new(&file)
        .file_name()
        .and_then(|s| s.to_str())
        .unwrap_or(&file)
        .to_string();

    let template = match duration {
        Some(d) => format!(
            "{{spinner}} [{{elapsed_precise}}/{}] {{decimal_bytes}} → {{msg}}",
            indicatif::FormattedDuration(d),
        ),
        None => "{spinner} [{elapsed_precise}] {decimal_bytes} → {msg}".to_string(),
    };
    let pb = crate::multi_progress().add(ProgressBar::new_spinner());
    pb.set_style(ProgressStyle::with_template(&template).unwrap());
    pb.enable_steady_tick(Duration::from_millis(100));

    let status = {
        let mut parts: Vec<String> = vec![file_name];
        if raw {
            parts.push("raw".into());
        }
        if unbuffered {
            parts.push("unbuf".into());
        }
        parts.join(" · ")
    };
    pb.set_message(status.clone());

    let mode = match raw {
        true => RecordMode::Raw,
        false => RecordMode::Described,
    };
    let out =
        File::create(&file).wrap_err_with(|| format!("could not create log file {}", file))?;
    let mut recorder = Recorder::new(&tree, out, mode);

    let deadline = duration.map(|duration| Instant::now() + duration);
    let outcome = loop {
        match recorder.step(deadline) {
            Ok(Step::Deadline) => break Ok(()),
            Ok(Step::Running) => {}
            Err(error) => break Err(error),
        }
        pb.set_position(recorder.bytes_written());
        pb.set_message(match recorder.samples_lost() {
            0 => status.clone(),
            lost => format!("{status} · ({lost} dropped)"),
        });
        if unbuffered {
            if let Err(error) = recorder.flush() {
                break Err(error);
            }
        }
    };
    pb.finish_and_clear();
    outcome.wrap_err_with(|| format!("stopped recording to {}", file))?;

    let lost = recorder.samples_lost();
    match recorder.bytes_written() {
        0 => log::info!("no data received"),
        bytes if lost == 0 => log::info!("wrote {} bytes to {}", bytes, file),
        bytes => log::info!(
            "wrote {} bytes to {} ({} samples dropped)",
            bytes,
            file,
            lost
        ),
    }
    Ok(())
}

pub fn log_metadata(tio: &TioOpts, file: String) -> eyre::Result<()> {
    use eyre::WrapErr;

    let connection = Connection::open(&tio.root)?;
    let route = tio.route;

    let device = connection.device(route);

    let meta = device
        .metadata()
        .wrap_err("failed to fetch device metadata")
        .with_proxy_help()?;

    let mut file_out: Option<File> = None;

    let write_packet = |fo: &mut Option<File>, pkt: tio::Packet| -> eyre::Result<()> {
        let f = ensure_open(fo, &file)?;
        f.write_all(pkt.as_bytes())
            .wrap_err_with(|| format!("failed to write {}", file))
    };

    for packet in meta.metadata_packets()? {
        write_packet(&mut file_out, packet)?;
    }
    Ok(())
}

pub fn meta_reroute(input: String, route: DeviceRoute, output: Option<String>) -> eyre::Result<()> {
    use eyre::{bail, WrapErr};

    let data = std::fs::read(&input).wrap_err_with(|| format!("could not read {}", input))?;

    let mut rest: &[u8] = &data;
    let mut routes: HashSet<DeviceRoute> = HashSet::new();
    let mut packet_count = 0usize;

    while !rest.is_empty() {
        let (pkt, len) = tio::Packet::from_slice_prefix(rest)
            .wrap_err_with(|| format!("could not parse packet in {}", input))?;
        rest = &rest[len..];
        packet_count += 1;

        if pkt.ptype() != twinleaf::proto::PacketType::METADATA {
            bail!(
                "{} does not look like a metadata file (found non-metadata packet)",
                input
            );
        }
        routes.insert(pkt.route());
    }

    if packet_count == 0 {
        bail!("{} contains no packets", input);
    }

    if routes.len() > 1 {
        let mut routes: Vec<_> = routes.into_iter().collect();
        routes.sort();
        eprintln!("{} contains multiple routes:", input);
        for route in routes.iter().take(5) {
            eprintln!("  {}", route);
        }
        if routes.len() > 5 {
            eprintln!("  ... and {} more", routes.len() - 5);
        }
        bail!("cannot reroute a file with multiple routes");
    }

    let new_route = route;
    let output_path = output.unwrap_or_else(|| {
        let base = input.strip_suffix(".tio").unwrap_or(&input);
        format!("{}_rerouted.tio", base)
    });
    if output_path == input {
        bail!("output path must be different from input");
    }

    let mut file =
        File::create(&output_path).wrap_err_with(|| format!("could not create {}", output_path))?;

    rest = &data;
    while !rest.is_empty() {
        let (pkt, len) = tio::Packet::from_slice_prefix(rest)
            .wrap_err_with(|| format!("could not parse packet in {}", input))?;
        rest = &rest[len..];
        file.write_all(pkt.with_route(new_route).as_bytes())
            .wrap_err_with(|| format!("failed to write {}", output_path))?;
    }

    Ok(())
}
