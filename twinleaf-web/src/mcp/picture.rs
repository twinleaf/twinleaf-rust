//! The picture a glance returns: a PNG of fixed size with no text. Each
//! signal is drawn in a colour of [`COLOURS`] as vertical runs through its
//! points, so a min/max envelope read at two points a pixel fills the band
//! between its extremes. A gap is a dashed grey line across the whole
//! height. Light grid lines split the picture in ten columns and four rows.
//! The axes and which colour is which signal are said in the text beside it.

/// The picture's size in pixels.
pub(super) const WIDTH: usize = 800;
pub(super) const HEIGHT: usize = 300;

/// The colour of each signal, by its place in the glance, and the name the
/// text gives it. A ninth signal takes the first colour again.
pub(super) const COLOURS: [(&str, [u8; 3]); 8] = [
    ("blue", [31, 119, 180]),
    ("orange", [255, 127, 14]),
    ("green", [44, 160, 44]),
    ("red", [214, 39, 40]),
    ("purple", [148, 103, 189]),
    ("brown", [140, 86, 75]),
    ("pink", [227, 119, 194]),
    ("olive", [188, 189, 34]),
];

const BACKGROUND: [u8; 3] = [255, 255, 255];
const GRID: [u8; 3] = [232, 232, 232];
const GAP: [u8; 3] = [110, 110, 110];

/// The ranges the picture spans, `x` left to right and `y` bottom to top.
#[derive(Clone, Copy, Debug, PartialEq)]
pub(super) struct Axes {
    pub(super) x: (f64, f64),
    pub(super) y: (f64, f64),
}

impl Axes {
    /// The axes over `x` and `y`. A range of one value is widened to a
    /// range around it, so its points lie on the middle line.
    pub(super) fn new(x: (f64, f64), y: (f64, f64)) -> Axes {
        let widen = |(low, high): (f64, f64)| {
            if high > low {
                (low, high)
            } else {
                let half = if low == 0.0 { 1.0 } else { low.abs() * 0.01 };
                (low - half, low + half)
            }
        };
        Axes {
            x: widen(x),
            y: widen(y),
        }
    }

    /// The column of `x`, kept within the picture.
    fn column(&self, x: f64) -> usize {
        let (low, high) = self.x;
        let at = (x - low) / (high - low) * (WIDTH - 1) as f64;
        at.round().clamp(0.0, (WIDTH - 1) as f64) as usize
    }

    /// The row of `y`, counted from the top, kept within the picture.
    fn row(&self, y: f64) -> f64 {
        let (low, high) = self.y;
        let at = (high - y) / (high - low) * (HEIGHT - 1) as f64;
        at.clamp(0.0, (HEIGHT - 1) as f64)
    }
}

/// The picture's pixels, row after row from the top.
pub(super) struct Picture {
    pixels: Vec<[u8; 3]>,
}

impl Picture {
    /// A blank picture with its grid.
    pub(super) fn new() -> Picture {
        let mut picture = Picture {
            pixels: vec![BACKGROUND; WIDTH * HEIGHT],
        };
        for k in 1..10 {
            let column = k * (WIDTH - 1) / 10;
            (0..HEIGHT).for_each(|row| picture.set(column, row, GRID));
        }
        for k in 1..4 {
            let row = k * (HEIGHT - 1) / 4;
            (0..WIDTH).for_each(|column| picture.set(column, row, GRID));
        }
        picture
    }

    fn set(&mut self, column: usize, row: usize, colour: [u8; 3]) {
        self.pixels[row * WIDTH + column] = colour;
    }

    /// The colour of the pixel at `column` and `row`, counted from the top.
    #[cfg(test)]
    pub(super) fn at(&self, column: usize, row: usize) -> [u8; 3] {
        self.pixels[row * WIDTH + column]
    }

    /// Draws the runs of points of the signal at `place` on `axes`. Within a
    /// run each point joins the one before it, and a run joins no other.
    /// A point that is not a number is passed over.
    pub(super) fn plot(&mut self, axes: &Axes, place: usize, runs: &[Vec<(f64, f64)>]) {
        let colour = COLOURS[place % COLOURS.len()].1;
        for run in runs {
            let mut last: Option<(usize, f64)> = None;
            for &(x, y) in run.iter().filter(|(x, y)| x.is_finite() && y.is_finite()) {
                let point = (axes.column(x), axes.row(y));
                let (from_column, from_row) = last.unwrap_or(point);
                self.join((from_column, from_row), point, colour);
                last = Some(point);
            }
        }
    }

    /// Fills, in each column from `from` to `to`, the rows between where
    /// the straight line between them enters the column and where it leaves.
    fn join(&mut self, from: (usize, f64), to: (usize, f64), colour: [u8; 3]) {
        let (c0, r0) = from;
        let (c1, r1) = to;
        let (left, right) = (c0.min(c1), c0.max(c1));
        let row_at = |column: usize| {
            if c0 == c1 {
                r1
            } else {
                r0 + (r1 - r0) * (column as f64 - c0 as f64) / (c1 as f64 - c0 as f64)
            }
        };
        let mut previous = if c0 <= c1 { r0 } else { r1 };
        for column in left..=right {
            let row = row_at(column);
            let (low, high) = if c0 == c1 {
                (r0.min(r1), r0.max(r1))
            } else {
                (previous.min(row), previous.max(row))
            };
            for r in low.round() as usize..=high.round() as usize {
                self.set(column, r, colour);
            }
            previous = row;
        }
    }

    /// Draws a gap at `x`, a dashed line across the whole height.
    pub(super) fn gap(&mut self, axes: &Axes, x: f64) {
        if !x.is_finite() {
            return;
        }
        let column = axes.column(x);
        for row in (0..HEIGHT).filter(|row| row % 6 < 4) {
            self.set(column, row, GAP);
        }
    }

    /// The picture as a PNG file's bytes.
    pub(super) fn png(&self) -> Vec<u8> {
        let mut bytes = Vec::new();
        let mut encoder = png::Encoder::new(&mut bytes, WIDTH as u32, HEIGHT as u32);
        encoder.set_color(png::ColorType::Rgb);
        encoder.set_depth(png::BitDepth::Eight);
        let data: Vec<u8> = self.pixels.iter().flatten().copied().collect();
        let mut writer = encoder
            .write_header()
            .expect("a header of a fixed size is written to memory");
        writer
            .write_image_data(&data)
            .expect("the pixels of a fixed size are written to memory");
        writer
            .finish()
            .expect("a PNG in memory is finished without fail");
        bytes
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const BLUE: [u8; 3] = COLOURS[0].1;
    const ORANGE: [u8; 3] = COLOURS[1].1;

    /// The rows of `column` in `colour`, from the top.
    fn rows_in(picture: &Picture, column: usize, colour: [u8; 3]) -> Vec<usize> {
        (0..HEIGHT)
            .filter(|&row| picture.at(column, row) == colour)
            .collect()
    }

    /// A ramp from the bottom left corner to the top right one lights the
    /// corners and, in every column, the row the line crosses it at.
    #[test]
    fn a_ramp_runs_corner_to_corner() {
        let axes = Axes::new((0.0, 10.0), (-1.0, 1.0));
        let mut picture = Picture::new();
        let ramp: Vec<(f64, f64)> = (0..=100)
            .map(|i| (i as f64 / 10.0, -1.0 + i as f64 / 50.0))
            .collect();
        picture.plot(&axes, 0, &[ramp]);
        assert_eq!(picture.at(0, HEIGHT - 1), BLUE);
        assert_eq!(picture.at(WIDTH - 1, 0), BLUE);
        for column in [0, 100, 399, 400, 700, WIDTH - 1] {
            let expected = (HEIGHT - 1) as f64 * (1.0 - column as f64 / (WIDTH - 1) as f64);
            let rows = rows_in(&picture, column, BLUE);
            assert!(!rows.is_empty(), "column {column} is blank");
            assert!(
                rows.iter().all(|&row| (row as f64 - expected).abs() <= 1.0),
                "column {column} lit {rows:?}, the line crosses at {expected}"
            );
        }
    }

    /// A min/max envelope, two points a column, fills the band between them.
    #[test]
    fn an_envelope_fills_its_band() {
        let axes = Axes::new((0.0, (WIDTH - 1) as f64), (0.0, (HEIGHT - 1) as f64));
        let mut picture = Picture::new();
        let envelope: Vec<(f64, f64)> = (0..WIDTH)
            .flat_map(|column| [(column as f64, 100.0), (column as f64, 200.0)])
            .collect();
        picture.plot(&axes, 1, &[envelope]);
        for column in [0, 333, WIDTH - 1] {
            let rows = rows_in(&picture, column, ORANGE);
            assert_eq!(rows, (99..=199).collect::<Vec<_>>(), "column {column}");
        }
    }

    /// Two runs on either side of a gap do not join, and the gap's line is
    /// drawn where it falls.
    #[test]
    fn a_gap_is_a_line_and_its_runs_do_not_join() {
        let axes = Axes::new((0.0, 10.0), (0.0, 1.0));
        let mut picture = Picture::new();
        let before = vec![(0.0, 0.0), (4.0, 0.0)];
        let after = vec![(6.0, 1.0), (10.0, 1.0)];
        picture.plot(&axes, 0, &[before, after]);
        picture.gap(&axes, 5.0);
        let gap = axes.column(5.0);
        assert_eq!(gap, 400);
        assert_eq!(picture.at(gap, 0), GAP);
        assert_eq!(picture.at(gap, 3), GAP);
        assert_eq!(picture.at(gap, 4), BACKGROUND, "the line is dashed");
        // Between the runs nothing of the signal is drawn.
        for column in axes.column(4.0) + 1..axes.column(6.0) {
            assert!(
                rows_in(&picture, column, BLUE).is_empty(),
                "column {column}"
            );
        }
        assert_eq!(rows_in(&picture, axes.column(2.0), BLUE), [HEIGHT - 1]);
        assert_eq!(rows_in(&picture, axes.column(8.0), BLUE), [0]);
    }

    /// An empty span draws only the grid, and the PNG is of the picture's size.
    #[test]
    fn an_empty_span_is_the_grid_alone() {
        let axes = Axes::new((0.0, 1.0), (0.0, 0.0));
        let mut picture = Picture::new();
        picture.plot(&axes, 0, &[]);
        picture.plot(&axes, 0, &[vec![]]);
        let colours: std::collections::BTreeSet<[u8; 3]> = picture.pixels.iter().copied().collect();
        assert_eq!(colours, [GRID, BACKGROUND].into_iter().collect());
        let png = picture.png();
        assert_eq!(&png[1..4], b"PNG");
        // The header chunk's width and height, big endian.
        assert_eq!(&png[16..24], &[0, 0, 3, 32, 0, 0, 1, 44]);
    }

    /// A range of one value is widened around it.
    #[test]
    fn a_flat_signal_lies_on_the_middle_line() {
        let axes = Axes::new((0.0, 1.0), (5.0, 5.0));
        assert_eq!(axes.y, (4.95, 5.05));
        assert!((axes.row(5.0) - (HEIGHT - 1) as f64 / 2.0).abs() < 1e-6);
    }
}
