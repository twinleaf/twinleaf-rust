//! The glance: a few lines and a picture of signals over a span. In time,
//! each signal's rows, extremes and gaps, read from the window the page
//! reads at the picture's width. As a spectrum, each signal's amplitude
//! spectral density as the core's Welch estimate gives it over the span's
//! rows, its largest peaks and its noise floor.

use super::picture::{Axes, Picture, COLOURS, HEIGHT, WIDTH};
use super::reads::{self, num, retired, rows_of, time, Failed};
use super::tools::{Door, Glance, View};
use crate::hub::Live;
use twinleaf::data::csv::MergedColumn;
use twinleaf::data::ops::{NoSpectrum, Spectrum, Welch};
use twinleaf::data::ColumnOp;

/// The gaps of a signal a glance names, at most.
const GAPS: usize = 5;
/// The largest peaks of a spectrum a glance names.
const PEAKS: usize = 5;
/// The newest rows of a span a spectrum is estimated from, at most.
const SPECTRUM_ROWS: usize = 1 << 20;

/// The text and the PNG of a glance.
pub(crate) async fn glance(door: &Door, args: Glance) -> Result<(String, Vec<u8>), Failed> {
    let live = reads::live(door).await?;
    let (ids, t0, t1) = reads::resolve(&live, &args.signals, args.span).await?;
    let columns = reads::span_rows(&live, ids.clone(), t0, t1).await?;
    match args.view {
        View::Time => time_view(&live, ids, columns, t0, t1).await,
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
    for &id in &ids {
        let read = (live.window(id, t0, t1, WIDTH).await)
            .map_err(retired)?
            .ok_or_else(|| retired(()))?;
        let window = tokio::task::spawn_blocking(move || read.window())
            .await
            .map_err(|error| error.to_string())?;
        windows.push(window);
    }
    let mut text = format!("{} to {} s\n", time(t0), time(t1));
    let (mut low, mut high) = (f64::INFINITY, f64::NEG_INFINITY);
    let mut runs_of = Vec::new();
    let mut gaps_of = Vec::new();
    for (column, window) in columns.iter().zip(&windows) {
        // The window splits its points at each loss of samples.
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
        let values = runs.iter().flatten().map(|p| p.1).filter(|y| y.is_finite());
        let (min, max) = values.fold((f64::INFINITY, f64::NEG_INFINITY), |(a, b), y| {
            (a.min(y), b.max(y))
        });
        let gaps: Vec<(f64, f64)> = (runs.windows(2))
            .filter_map(|pair| Some((pair[0].last()?.0, pair[1].first()?.0)))
            .collect();
        let units = after(&column.units);
        let rows = rows_of(column);
        if rows == 0 {
            text += &format!("{}: no rows\n", column.name);
        } else {
            let (min_text, max_text) = (num(min), num(max));
            let name = &column.name;
            text += &format!("{name}: {rows} rows, min {min_text}{units}, max {max_text}{units}");
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
        }
        runs_of.push(runs);
        gaps_of.push(gaps);
    }
    text += "min and max are the envelope's, which holds every row's extremes. \
             Mean and standard deviation are not available yet.\n";
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
        let spectra: Vec<_> = columns.iter().map(spectrum_of).collect();
        (columns, spectra)
    })
    .await
    .map_err(|error| error.to_string())?;
    let mut text = format!("{} to {} s\n", time(t0), time(t1));
    let mut runs_of = Vec::new();
    let mut x = (f64::INFINITY, f64::NEG_INFINITY);
    let mut y = (f64::INFINITY, f64::NEG_INFINITY);
    for (column, spectrum) in columns.iter().zip(&spectra) {
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
            "{:?}: {} rows at {} Hz, noise floor {floor}\n",
            column.name,
            spectrum.sample_count,
            num(spectrum.sampling_hz)
        );
        let peaks: Vec<String> = (peaks(&spectrum.points).into_iter())
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

/// The Welch estimate of the newest rows of `column`, at most
/// SPECTRUM_ROWS, or why there is none, in words.
fn spectrum_of(column: &MergedColumn) -> Result<Spectrum, String> {
    let rate = (column.batches.first()).map_or(0.0, |batch| batch.metadata().rate_hz());
    let mut welch = Welch::new(rows_of(column).min(SPECTRUM_ROWS), rate);
    for batch in &column.batches {
        if let Some(values) = batch.column(column.column) {
            welch.update_batch(batch.timestamps(), values);
        }
    }
    welch.output().clone().map_err(|why| match why {
        NoSpectrum::TooFewSamples { have, need } => {
            format!("it has {have} rows and an estimate needs {need}")
        }
        NoSpectrum::InvalidSampleRate => format!("its rate {rate} Hz is no rate"),
        other => format!("{other:?}"),
    })
}

/// The PEAKS largest local maxima of a spectrum's `(frequency, density)`
/// bins, largest first.
fn peaks(points: &[(f64, f64)]) -> Vec<(f64, f64)> {
    let mut peaks: Vec<(f64, f64)> = (points.windows(3))
        .filter(|w| w[1].1 > w[0].1 && w[1].1 >= w[2].1)
        .map(|w| w[1])
        .collect();
    peaks.sort_by(|a, b| b.1.total_cmp(&a.1));
    peaks.truncate(PEAKS);
    peaks
}
