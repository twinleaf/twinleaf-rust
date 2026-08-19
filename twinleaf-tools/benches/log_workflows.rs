//! End-to-end benchmarks for real-log processing workflows.
//!
//! The capture stays outside the repository. By default this runs decode-only,
//! inspect, CSV, and uncompressed HDF5 cases:
//!
//! cargo bench -p twinleaf-tools --features hdf5 --bench log_workflows -- \
//!     /path/to/log.tio [iterations]
//!
//! Pass "--case <decode|indexed-decode|inspect|csv|hdf5|hdf5-compressed>" to
//! select cases. Indexed decode includes both the scan and batch passes.
//! "--compress" adds the compressed HDF5 case to the default set.
//!
//! Reported medians are warm-cache macrobenchmarks: later iterations commonly
//! read the fixture from the operating system's page cache.

use clap::{Parser, ValueEnum};
use indicatif::ProgressDrawTarget;
use std::fs;
use std::hint::black_box;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};
use twinleaf::data::{LogReader, PacketParser};
use twinleaf::device::DeviceRoute;
use twinleaf_tools::tools::log::{log_csv, log_hdf, log_inspect};
use twinleaf_tools::{SplitLevel, SplitPolicy};

#[derive(Clone, Copy, Debug, Eq, PartialEq, ValueEnum)]
enum Workflow {
    Decode,
    IndexedDecode,
    Inspect,
    Csv,
    Hdf5,
    Hdf5Compressed,
}

impl Workflow {
    fn name(self) -> &'static str {
        match self {
            Self::Decode => "decode",
            Self::IndexedDecode => "indexed-decode",
            Self::Inspect => "inspect",
            Self::Csv => "csv",
            Self::Hdf5 => "hdf5",
            Self::Hdf5Compressed => "hdf5-compressed",
        }
    }
}

fn nonzero_usize(value: &str) -> Result<usize, String> {
    value
        .parse::<usize>()
        .ok()
        .filter(|value| *value != 0)
        .ok_or_else(|| "iterations must be a positive integer".to_string())
}

#[derive(Parser)]
struct Config {
    /// Real TIO log fixture. Omit when Cargo executes the bench as a test.
    input: Option<PathBuf>,
    /// Number of timed iterations per workflow.
    #[arg(default_value_t = 3, value_parser = nonzero_usize)]
    iterations: usize,
    /// Run only the selected workflow; may be repeated.
    #[arg(long = "case", value_enum)]
    workflows: Vec<Workflow>,
    /// Add compressed HDF5 to the default or selected workflows.
    #[arg(long)]
    compress: bool,
    /// Cargo appends this argument to custom benchmark harnesses.
    #[arg(long = "bench", hide = true)]
    _bench: bool,
}

#[derive(Default)]
struct RunStats {
    packets: u64,
    batches: u64,
    rows: u64,
    output_bytes: Option<u64>,
}

impl RunStats {
    fn summary(&self) -> String {
        let mut parts = Vec::new();
        if self.packets != 0 {
            parts.push(format!("{} packets", self.packets));
        }
        if self.batches != 0 {
            parts.push(format!("{} batches", self.batches));
        }
        if self.rows != 0 {
            parts.push(format!("{} rows", self.rows));
        }
        if let Some(bytes) = self.output_bytes {
            parts.push(format!("{:.2} MiB output", bytes as f64 / 1_048_576.0));
        }
        if parts.is_empty() {
            String::new()
        } else {
            format!(", {}", parts.join(", "))
        }
    }
}

fn path_string(path: &Path) -> String {
    path.to_string_lossy().into_owned()
}

/// Choose a stable CSV target before timing any workflow.
fn discover_csv_target(input: &Path) -> eyre::Result<String> {
    let mut input = LogReader::open(input)?;
    let mut parser = PacketParser::new(DeviceRoute::root(), false);

    while let Some(packet) = input.next_packet()? {
        let _ = parser.push_packet(&packet);
        if let Some(batch) = parser.pop_batch() {
            let stream_id = batch.stream().stream_id;
            return Ok(if batch.route().is_empty() {
                format!("/{stream_id}")
            } else {
                format!("{}/{stream_id}", batch.route())
            });
        }
    }

    Err(eyre::eyre!("log contains no decodable sample stream"))
}

fn run_decode(input: &Path) -> eyre::Result<RunStats> {
    let mut input = LogReader::open(input)?;
    let mut parser = PacketParser::new(DeviceRoute::root(), false);
    let mut stats = RunStats::default();

    while let Some(packet) = input.next_packet()? {
        stats.packets += 1;
        let _ = parser.push_packet(&packet);
        while let Some(batch) = parser.pop_batch() {
            stats.batches += 1;
            stats.rows += batch.len() as u64;
            black_box(batch);
        }
    }
    for batch in parser.finish() {
        stats.batches += 1;
        stats.rows += batch.len() as u64;
        black_box(batch);
    }
    black_box((stats.packets, stats.batches, stats.rows));
    Ok(stats)
}

fn run_indexed_decode(input: &Path) -> eyre::Result<RunStats> {
    let input = LogReader::open(input)?;
    let index = input.scan(DeviceRoute::root(), false);
    if let Some(error) = index.summary().error() {
        return Err(eyre::eyre!(error.to_string()));
    }

    let mut stats = RunStats {
        packets: index.summary().packet_count(),
        ..RunStats::default()
    };
    for batch in index.batches(65_536) {
        let batch = batch?;
        stats.batches += 1;
        stats.rows += batch.len() as u64;
        black_box(batch);
    }
    black_box((stats.packets, stats.batches, stats.rows));
    Ok(stats)
}

fn scratch_dir(workflow: Workflow, iteration: usize) -> PathBuf {
    std::env::temp_dir().join(format!(
        "twinleaf-log-workflows-{}-{}-{}",
        std::process::id(),
        workflow.name(),
        iteration
    ))
}

fn directory_bytes(path: &Path) -> eyre::Result<u64> {
    fs::read_dir(path)?.try_fold(0u64, |total, entry| {
        let entry = entry?;
        Ok(total + entry.metadata()?.len())
    })
}

fn run_once(
    workflow: Workflow,
    input: &Path,
    csv_target: &str,
    iteration: usize,
) -> eyre::Result<(Duration, RunStats)> {
    let input_string = path_string(input);
    let scratch = scratch_dir(workflow, iteration);
    let uses_output = matches!(
        workflow,
        Workflow::Csv | Workflow::Hdf5 | Workflow::Hdf5Compressed
    );
    if uses_output {
        if scratch.exists() {
            fs::remove_dir_all(&scratch)?;
        }
        fs::create_dir(&scratch)?;
    }

    let started = Instant::now();
    let result = match workflow {
        Workflow::Decode => run_decode(input),
        Workflow::IndexedDecode => run_indexed_decode(input),
        Workflow::Inspect => log_inspect(vec![input_string]).map(|_| RunStats::default()),
        Workflow::Csv => {
            let output_prefix = path_string(&scratch.join("output"));
            log_csv(
                vec![csv_target.to_string(), input_string],
                None,
                Some(output_prefix),
                true,
            )
            .map(|_| RunStats::default())
        }
        Workflow::Hdf5 | Workflow::Hdf5Compressed => {
            let output = path_string(&scratch.join("output.h5"));
            log_hdf(
                vec![input_string],
                Some(output),
                None,
                workflow == Workflow::Hdf5Compressed,
                false,
                SplitLevel::None,
                SplitPolicy::Continuous,
            )
            .map(|_| RunStats::default())
        }
    };
    let elapsed = started.elapsed();

    let result = result.and_then(|mut stats| {
        if uses_output {
            stats.output_bytes = Some(directory_bytes(&scratch)?);
        }
        Ok(stats)
    });
    let cleanup = if uses_output {
        fs::remove_dir_all(&scratch)
    } else {
        Ok(())
    };
    match result {
        Ok(stats) => {
            cleanup?;
            Ok((elapsed, stats))
        }
        Err(error) => {
            let _ = cleanup;
            Err(error)
        }
    }
}

fn run_workflow(
    workflow: Workflow,
    input: &Path,
    csv_target: &str,
    iterations: usize,
    input_bytes: u64,
) -> eyre::Result<()> {
    println!("\n=== {} ===", workflow.name());
    let mut elapsed = Vec::with_capacity(iterations);
    for iteration in 0..iterations {
        let (duration, stats) = run_once(workflow, input, csv_target, iteration)?;
        let mib_per_second = input_bytes as f64 / 1_048_576.0 / duration.as_secs_f64();
        println!(
            "{} iteration {}: {:.3}s, {:.2} MiB/s{}",
            workflow.name(),
            iteration + 1,
            duration.as_secs_f64(),
            mib_per_second,
            stats.summary(),
        );
        elapsed.push(duration);
    }

    elapsed.sort_unstable();
    let median = elapsed[elapsed.len() / 2];
    let mib_per_second = input_bytes as f64 / 1_048_576.0 / median.as_secs_f64();
    println!(
        "{} median: {:.3}s, {:.2} MiB/s ({} iteration{})",
        workflow.name(),
        median.as_secs_f64(),
        mib_per_second,
        iterations,
        if iterations == 1 { "" } else { "s" },
    );
    Ok(())
}

fn main() -> eyre::Result<()> {
    let mut config = Config::parse();
    let Some(input) = config.input.take() else {
        // "cargo test --all-targets" executes custom benchmark binaries without
        // fixture arguments. Real-log benchmarks are deliberately opt-in.
        eprintln!("skipping log workflow benchmarks: no log fixture supplied");
        return Ok(());
    };
    if config.workflows.is_empty() {
        config.workflows.extend([
            Workflow::Decode,
            Workflow::Inspect,
            Workflow::Csv,
            Workflow::Hdf5,
        ]);
    }
    if config.compress && !config.workflows.contains(&Workflow::Hdf5Compressed) {
        config.workflows.push(Workflow::Hdf5Compressed);
    }

    let input_bytes = fs::metadata(&input)?.len();
    let csv_target = discover_csv_target(&input)?;

    twinleaf_tools::init_logging();
    twinleaf_tools::multi_progress().set_draw_target(ProgressDrawTarget::hidden());

    println!("fixture: {}", input.display());
    println!("size: {:.2} MiB", input_bytes as f64 / 1_048_576.0);
    println!("CSV target: {csv_target}");

    for workflow in config.workflows {
        run_workflow(
            workflow,
            &input,
            &csv_target,
            config.iterations,
            input_bytes,
        )?;
    }
    Ok(())
}
