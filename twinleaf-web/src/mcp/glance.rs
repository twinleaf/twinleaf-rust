//! The glance: a few lines and a picture of signals over a span. In time,
//! each signal's rows summed up by the core's `Stats`, its gaps from the
//! run's own record, and a picture of the window the page reads at the
//! picture's width. As a spectrum, each signal's amplitude spectral density
//! as the hub estimates it over the span's rows since their last break,
//! its largest peaks and its noise floor.

use super::picture::{Axes, Picture, COLOURS, HEIGHT, WIDTH};
use super::reads::{self, num, retired, time, Failed};
use super::tools::{Glance, View};
use crate::hub::Live;
use twinleaf::data::csv::MergedColumn;
use twinleaf::data::ops::{largest_peaks, Stats};
use twinleaf::data::{ColumnOp, Covered};

/// The gaps of a signal a glance names, at most.
const GAPS: usize = 5;
/// The largest peaks of a spectrum a glance names.
const PEAKS: usize = 5;

/// The text and the PNG of a glance.
pub(crate) async fn glance(live: &Live, args: Glance) -> Result<(String, Vec<u8>), Failed> {
    let (ids, t0, t1) = reads::resolve(live, &args.signals, args.span).await?;
    let columns = reads::span_rows(live, ids.clone(), t0, t1).await?;
    match args.view {
        View::Time => time_view(live, ids, columns, t0, t1).await,
        View::Spectrum => spectrum_view(columns, t0, t1).await,
    }
}

/// The units after a number, or nothing for none.
fn after(units: &str) -> String {
    if units.is_empty() {
        String::new()
    } else {
        format!(" {units:?}")
    }
}

/// The rows a result covers, as `covered` says, after the count of the
/// span's rows: their count and the times of the first and the last, and
/// `done`, what the result did of them.
fn covering(covered: Option<Covered>, done: &str) -> String {
    match covered {
        Some(covered) => format!(
            ", {} from {} to {} s {done}",
            covered.rows,
            time(covered.first),
            time(covered.last)
        ),
        None => String::new(),
    }
}

/// Which colour of the picture is which of `columns`.
fn legend(columns: &[MergedColumn]) -> String {
    let named: Vec<String> = (columns.iter().enumerate())
        .map(|(place, column)| format!("{} {:?}", COLOURS[place % COLOURS.len()].0, column.name))
        .collect();
    named.join(", ")
}

async fn time_view(
    live: &Live,
    ids: Vec<u16>,
    columns: Vec<MergedColumn>,
    t0: f64,
    t1: f64,
) -> Result<(String, Vec<u8>), Failed> {
    let mut windows = Vec::new();
    let mut gaps_of: Vec<Vec<(f64, f64)>> = Vec::new();
    for &id in &ids {
        let read = (live.window(id, t0, t1, WIDTH).await)
            .map_err(retired)?
            .ok_or_else(|| retired(()))?;
        let window = tokio::task::spawn_blocking(move || read.window())
            .await
            .map_err(|error| error.to_string())?;
        windows.push(window);
        let gaps = (live.gaps(id, t0, t1).await)
            .map_err(retired)?
            .ok_or_else(|| retired(()))?;
        gaps_of.push(gaps.iter().map(|gap| (gap.before, gap.after)).collect());
    }
    let (columns, stats) = tokio::task::spawn_blocking(move || {
        let stats: Vec<(Stats, Option<Covered>)> = (columns.iter())
            .map(|column| {
                let mut stats = Stats::new();
                let (_, covered) = stats.feed_rows(&column.batches, column.column);
                (stats, covered)
            })
            .collect();
        (columns, stats)
    })
    .await
    .map_err(|error| error.to_string())?;
    let mut text = format!("{} to {} s\n", time(t0), time(t1));
    let (mut low, mut high) = (f64::INFINITY, f64::NEG_INFINITY);
    let mut runs_of = Vec::new();
    let read = columns.iter().zip(&stats).zip(&windows).zip(&gaps_of);
    for (((column, (stats, covered)), window), gaps) in read {
        // The window splits its points at each loss of samples, and where
        // the held history meets the buffer.
        let runs: Vec<Vec<(f64, f64)>> = (window.chunks.iter())
            .map(|chunk| {
                chunk
                    .x
                    .iter()
                    .copied()
                    .zip(chunk.y.iter().copied())
                    .collect()
            })
            .collect();
        let units = after(&column.units);
        let rows = column.rows();
        let summed = (stats.min().zip(stats.max())).zip(stats.mean().zip(stats.std_dev()));
        let name = &column.name;
        if rows == 0 {
            text += &format!("{name:?}: no rows\n");
        } else if let Some(((min, max), (mean, deviation))) = summed {
            let (min_text, max_text) = (num(min), num(max));
            let summed = covering(*covered, "summed");
            text += &format!("{name:?}: {rows} rows in the span{summed}");
            text += &format!(", min {min_text}{units}, max {max_text}{units}");
            let (mean, deviation) = (num(mean), num(deviation));
            text += &format!(", mean {mean}{units}, standard deviation {deviation}{units}");
            if gaps.is_empty() {
                text += ", no gaps\n";
            } else {
                let listed: Vec<String> = (gaps.iter().take(GAPS))
                    .map(|(a, b)| format!("{} to {} s", time(*a), time(*b)))
                    .collect();
                let n = gaps.len();
                let gaps_word = if n == 1 { "gap" } else { "gaps" };
                text += &format!(", {n} {gaps_word} at {}", listed.join(", "));
                if gaps.len() > GAPS {
                    text += &format!(" and {} more", gaps.len() - GAPS);
                }
                text.push('\n');
            }
            low = low.min(min);
            high = high.max(max);
        } else {
            text += &format!("{name:?}: {rows} rows, none of them a number\n");
        }
        runs_of.push(runs);
    }
    let axes = Axes::new((t0, t1), (low, high));
    let mut picture = Picture::new();
    for (place, runs) in runs_of.iter().enumerate() {
        picture.plot(&axes, place, runs);
    }
    for (start, _) in gaps_of.iter().flatten() {
        picture.gap(&axes, *start);
    }
    text += &format!(
        "picture {WIDTH}x{HEIGHT}, x {} to {} s, y {} to {}, {}",
        time(axes.x.0),
        time(axes.x.1),
        num(axes.y.0),
        num(axes.y.1),
        legend(&columns)
    );
    if gaps_of.iter().any(|gaps| !gaps.is_empty()) {
        text += ", dashed grey lines at the gaps";
    }
    Ok((text, picture.png()))
}

async fn spectrum_view(
    columns: Vec<MergedColumn>,
    t0: f64,
    t1: f64,
) -> Result<(String, Vec<u8>), Failed> {
    let (columns, spectra) = tokio::task::spawn_blocking(move || {
        let spectra: Vec<_> = (columns.iter()).map(crate::hub::spectrum_of_rows).collect();
        (columns, spectra)
    })
    .await
    .map_err(|error| error.to_string())?;
    let mut text = format!("{} to {} s\n", time(t0), time(t1));
    let mut runs_of = Vec::new();
    let mut x = (f64::INFINITY, f64::NEG_INFINITY);
    let mut y = (f64::INFINITY, f64::NEG_INFINITY);
    for (column, (spectrum, covered)) in columns.iter().zip(&spectra) {
        let units = if column.units.is_empty() {
            "1".to_string()
        } else {
            format!("{:?}", column.units)
        };
        let spectrum = match spectrum {
            Ok(spectrum) => spectrum,
            Err(why) => {
                text += &format!("{:?}: no spectrum, as {why}\n", column.name);
                runs_of.push(Vec::new());
                continue;
            }
        };
        let floor = (spectrum.noise_floor).map_or("none found".to_string(), |f| {
            format!("{} {units}/rtHz", num(f))
        });
        text += &format!(
            "{:?}: {} rows in the span{} at {} Hz, noise floor {floor}\n",
            column.name,
            column.rows(),
            covering(*covered, "estimated"),
            num(spectrum.sampling_hz)
        );
        let peaks: Vec<String> = (largest_peaks(&spectrum.points, PEAKS).into_iter())
            .map(|(f, d)| format!("{} Hz at {} {units}/rtHz", num(f), num(d)))
            .collect();
        if !peaks.is_empty() {
            text += &format!("  peaks {}\n", peaks.join(", "));
        }
        let run: Vec<(f64, f64)> = (spectrum.points.iter())
            .map(|&(f, d)| (f, d.log10()))
            .collect();
        for &(f, d) in &run {
            x = (x.0.min(f), x.1.max(f));
            y = (y.0.min(d), y.1.max(d));
        }
        runs_of.push(vec![run]);
    }
    if !x.0.is_finite() {
        return Err(text.trim_end().to_string());
    }
    let axes = Axes::new(x, y);
    let mut picture = Picture::new();
    for (place, runs) in runs_of.iter().enumerate() {
        picture.plot(&axes, place, runs);
    }
    text += &format!(
        "picture {WIDTH}x{HEIGHT}, x {} to {} Hz linear, y log10 of density {} to {}, {}",
        num(axes.x.0),
        num(axes.x.1),
        num(axes.y.0),
        num(axes.y.1),
        legend(&columns)
    );
    Ok((text, picture.png()))
}
