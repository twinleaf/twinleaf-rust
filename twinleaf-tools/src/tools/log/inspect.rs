use super::progress::ByteProgress;
use twinleaf::data::{BoundaryClass, LogFile, StreamSummary};
use twinleaf::DeviceRoute;
const BOUNDARY_CLASSES: [BoundaryClass; 5] = [
    BoundaryClass::Startup,
    BoundaryClass::Seamless,
    BoundaryClass::DataLoss,
    BoundaryClass::Reconfig,
    BoundaryClass::Anomaly,
];

fn class_label(class: BoundaryClass) -> &'static str {
    match class {
        BoundaryClass::Seamless => "seamless",
        BoundaryClass::Startup => "startup",
        BoundaryClass::DataLoss => "data loss",
        BoundaryClass::Reconfig => "reconfig",
        BoundaryClass::Anomaly => "anomaly",
    }
}

/// Formats a duration, or an offset that a backward timeline made negative.
fn fmt_hms(secs: f64) -> String {
    if !secs.is_finite() || secs == 0.0 {
        return "0s".to_string();
    }
    if secs < 0.0 {
        return format!("-{}", fmt_hms(-secs));
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

/// Warns when the samples a run declares cannot fit the time it spans.
fn skew_note(run: &StreamSummary) -> String {
    use console::style;

    // End-of-sample timestamps for N samples span N-1 sample intervals.
    let declared = declared_timestamp_span(run.sample_count(), run.rate_hz());
    let observed = match (run.first_timestamp(), run.last_timestamp()) {
        (Some(a), Some(b)) => b - a,
        _ => 0.0,
    };
    if !declared.is_finite() || declared <= 0.0 || (declared - observed).abs() / declared <= 0.05 {
        return String::new();
    }
    format!(
        "  {}",
        style(format!(
            "⚠ declared {} vs observed {}",
            fmt_hms(declared),
            fmt_hms(observed)
        ))
        .yellow()
    )
}

fn declared_timestamp_span(sample_count: u64, rate_hz: f64) -> f64 {
    sample_count.saturating_sub(1) as f64 / rate_hz
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

    let input = LogFile::open(std::path::Path::new(path))
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
        log::warn!("{}: {}; stopping", path, error);
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
    if summary.devices().len() == 0 {
        println!("   (no device metadata seen)");
    } else {
        for (route, device) in summary.devices() {
            println!(
                "   • {}  {}  {}  {}",
                route,
                device.name,
                style(format!("fw {}", device.firmware)).dim(),
                style(format!("serial {}", device.serial)).dim(),
            );
        }
    }

    println!();
    println!(" {}", style("Streams:").bold().cyan());
    if summary.streams().is_empty() {
        println!("   (no sample data seen)");
    } else {
        for (key, runs) in summary.streams() {
            let Some(s) = runs.first() else { continue };
            let samples: u64 = runs.iter().map(|run| run.sample_count()).sum();
            let trailing = if runs.len() > 1 {
                format!("  {}", style(format!("{} runs", runs.len())).yellow())
            } else {
                skew_note(s)
            };
            println!(
                "   • {} {} {:<12} {:>6.0} {}  {:>4} {}  {:>10} {}{}",
                key.route,
                key.stream_id,
                s.metadata().name,
                s.rate_hz(),
                unit("Hz"),
                s.columns().len(),
                unit("cols"),
                samples,
                unit("samples"),
                trailing,
            );
            if runs.len() > 1 {
                let base = s.first_timestamp().unwrap_or(0.0);
                for run in runs {
                    println!(
                        "        {} {:<9} {:>6.0} {} {:>10} {}  {} → {}{}",
                        style(format!("run {}", run.run())).dim(),
                        run.opened_by().map_or("—", class_label),
                        run.rate_hz(),
                        unit("Hz"),
                        run.sample_count(),
                        unit("samples"),
                        fmt_hms(run.first_timestamp().unwrap_or(base) - base),
                        fmt_hms(run.last_timestamp().unwrap_or(base) - base),
                        skew_note(run),
                    );
                }
            }
            if s.columns().len() != 0 {
                let cols: Vec<String> = s
                    .columns()
                    .map(|column| {
                        let data_type = column.data_type.to_string();
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
    let counts: Vec<String> = BOUNDARY_CLASSES
        .iter()
        .filter(|class| summary.boundaries(**class) > 0)
        .map(|class| format!("{} {}", summary.boundaries(*class), class_label(*class)))
        .collect();
    let disruptive = summary.boundaries(BoundaryClass::DataLoss)
        + summary.boundaries(BoundaryClass::Reconfig)
        + summary.boundaries(BoundaryClass::Anomaly);
    let boundaries = if counts.is_empty() {
        "none".to_string()
    } else if disruptive > 0 {
        style(counts.join(", ")).yellow().to_string()
    } else {
        counts.join(", ")
    };
    println!(" {} {}", label("Boundaries:"), boundaries);

    let anomalies = summary.boundaries(BoundaryClass::Anomaly);
    if anomalies > 0 {
        let note = format!(
            "{anomalies} timeline anomalies: timestamps in this log are untrustworthy (firmware or wire fault)"
        );
        log::warn!("{}: {}", path, note);
        println!(" {}", style(format!("⚠ {note}")).red().bold());
    }
    println!("{rule}");

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::declared_timestamp_span;

    #[test]
    fn timestamp_span_has_one_fewer_interval_than_samples() {
        assert_eq!(declared_timestamp_span(10, 2.0), 4.5);
        assert_eq!(declared_timestamp_span(1, 2.0), 0.0);
        assert_eq!(declared_timestamp_span(0, 2.0), 0.0);
    }
}
