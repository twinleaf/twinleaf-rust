use super::progress::ByteProgress;
use twinleaf::data::LogReader;
use twinleaf::device::DeviceRoute;

fn fmt_hms(secs: f64) -> String {
    if !secs.is_finite() || secs <= 0.0 {
        return "0s".to_string();
    }
    let total = secs as u64;
    let h = total / 3600;
    let m = (total % 3600) / 60;
    let s = secs - (h * 3600 + m * 60) as f64;
    if h > 0 {
        format!("{}h{:02}m{:04.1}s", h, m, s)
    } else if m > 0 {
        format!("{}m{:04.1}s", m, s)
    } else {
        format!("{:.1}s", s)
    }
}

pub fn log_inspect(files: Vec<String>) -> eyre::Result<()> {
    for (i, path) in files.iter().enumerate() {
        if i > 0 {
            println!();
        }
        inspect_one_log(path)?;
    }
    Ok(())
}

fn inspect_one_log(path: &str) -> eyre::Result<()> {
    use console::style;
    use eyre::WrapErr;

    let input = LogReader::open(std::path::Path::new(path))
        .wrap_err_with(|| format!("could not mmap {}", path))?;
    let total_bytes = input.len() as u64;

    let mut progress = if total_bytes > 10 * 1024 * 1024 {
        Some(ByteProgress::new(total_bytes))
    } else {
        None
    };

    let index = input.scan_with_progress(DeviceRoute::root(), false, |position| {
        if let Some(progress) = &mut progress {
            progress.update(position as u64);
        }
    });
    let summary = index.summary();
    if let Some(error) = summary.error() {
        log::warn!(
            "{}: parse error at offset {} ({:?}); stopping",
            path,
            error.offset(),
            error.packet_error()
        );
    }

    if let Some(progress) = progress {
        progress.finish_and_clear(summary.bytes_scanned() as u64);
    }

    let size_mib = total_bytes as f64 / 1_048_576.0;
    let rule = style("─".repeat(50)).dim();
    let label = |s: &str| style(format!("{:12}", s)).bold().cyan();
    let unit = |s: &str| style(s.to_string()).dim();

    println!();
    println!("{rule}");
    println!(" {}", style("Log Inspection").bold());
    println!("{rule}");
    println!(" {} {}", label("File:"), path);
    println!(" {} {:.2} {}", label("Size:"), size_mib, unit("MiB"));
    println!(" {} {}", label("Packets:"), summary.packet_count());

    println!();
    println!(" {}", style("Devices:").bold().cyan());
    if summary.devices().is_empty() {
        println!("   (no device metadata seen)");
    } else {
        for (route, d) in summary.devices() {
            println!(
                "   • {}  {}  {}  {}",
                route,
                d.name,
                style(format!("fw {}", d.firmware_hash)).dim(),
                style(format!("serial {}", d.serial_number)).dim(),
            );
        }
    }

    println!();
    println!(" {}", style("Streams:").bold().cyan());
    if summary.streams().is_empty() {
        println!("   (no sample data seen)");
    } else {
        for (key, s) in summary.streams() {
            let rate_hz = s.rate_hz();
            let declared = if rate_hz > 0.0 {
                s.sample_count() as f64 / rate_hz
            } else {
                0.0
            };
            let observed = match (s.first_timestamp(), s.last_timestamp()) {
                (Some(a), Some(b)) => b - a,
                _ => 0.0,
            };
            let skew = if declared > 0.0 {
                (declared - observed).abs() / declared
            } else {
                0.0
            };
            let trailing = if skew > 0.05 && declared > 0.0 {
                format!(
                    "  {}",
                    style(format!(
                        "⚠ declared {} vs observed {}",
                        fmt_hms(declared),
                        fmt_hms(observed)
                    ))
                    .yellow()
                )
            } else {
                String::new()
            };
            println!(
                "   • {} {} {:<12} {:>6.0} {}  {:>4} {}  {:>10} {}{}",
                key.route,
                key.stream_id,
                s.metadata().name,
                rate_hz,
                unit("Hz"),
                s.columns().len(),
                unit("cols"),
                s.sample_count(),
                unit("samples"),
                trailing,
            );
            if !s.columns().is_empty() {
                let cols: Vec<String> = s
                    .columns()
                    .iter()
                    .map(|column| {
                        let data_type = column.data_type.type_name();
                        if column.units.is_empty() {
                            format!("{} {}", column.name, style(data_type).dim())
                        } else {
                            format!(
                                "{} {} {}",
                                column.name,
                                style(data_type).dim(),
                                style(&column.units).dim()
                            )
                        }
                    })
                    .collect();
                println!("        {}", cols.join(", "));
            }
        }
    }

    println!();
    let boundary_text = format!(
        "{} session changes, {} segment changes",
        summary.session_changes(),
        summary.segment_changes()
    );
    let styled_boundaries = if summary.session_changes() > 0 || summary.segment_changes() > 0 {
        style(boundary_text).yellow().to_string()
    } else {
        boundary_text
    };
    println!(" {} {}", label("Boundaries:"), styled_boundaries);
    println!("{rule}");

    Ok(())
}
