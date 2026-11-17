//! The count, extremes, mean and standard deviation of a column's rows.

use crate::{ColumnArray, ColumnOp, Continuity};

/// The count, minimum, maximum, mean and standard deviation of the values
/// of the rows it is fed that are finite numbers. A value that is not is
/// passed over.
///
/// Its continuity is [`Monotonic`](Continuity::Monotonic), so it sums up
/// every row it is fed across a run's gaps. The mean and the deviation are
/// kept as each value comes, so a long span costs no more memory than a
/// short one. The deviation is the population's, the root of the mean
/// square of the values less their mean.
#[derive(Debug, Clone, PartialEq)]
pub struct Stats {
    count: u64,
    min: f64,
    max: f64,
    mean: f64,
    /// The sum of the squares of the values less their mean.
    squares: f64,
}

impl Default for Stats {
    fn default() -> Stats {
        Stats::new()
    }
}

impl Stats {
    /// Creates the statistics of no rows.
    pub fn new() -> Stats {
        Stats {
            count: 0,
            min: f64::INFINITY,
            max: f64::NEG_INFINITY,
            mean: 0.0,
            squares: 0.0,
        }
    }

    /// Returns how many finite values were fed.
    pub fn count(&self) -> u64 {
        self.count
    }

    /// Returns the least value fed, or `None` before any.
    pub fn min(&self) -> Option<f64> {
        (self.count > 0).then_some(self.min)
    }

    /// Returns the greatest value fed, or `None` before any.
    pub fn max(&self) -> Option<f64> {
        (self.count > 0).then_some(self.max)
    }

    /// Returns the mean of the values fed, or `None` before any.
    pub fn mean(&self) -> Option<f64> {
        (self.count > 0).then_some(self.mean)
    }

    /// Returns the standard deviation of the values fed, or `None` before
    /// any.
    pub fn std_dev(&self) -> Option<f64> {
        (self.count > 0).then(|| (self.squares / self.count as f64).sqrt())
    }

    /// Adds one value.
    fn push(&mut self, value: f64) {
        self.count += 1;
        self.min = self.min.min(value);
        self.max = self.max.max(value);
        let delta = value - self.mean;
        self.mean += delta / self.count as f64;
        self.squares += delta * (value - self.mean);
    }
}

impl ColumnOp for Stats {
    type Output = Stats;

    fn continuity(&self) -> Continuity {
        Continuity::Monotonic
    }

    fn reset(&mut self) {
        *self = Stats::new();
    }

    fn update_batch(&mut self, _timestamps: &[f64], values: &ColumnArray) {
        for &value in values.to_f64().iter() {
            if value.is_finite() {
                self.push(value);
            }
        }
    }

    fn output(&self) -> &Stats {
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fed(values: &[f64]) -> Stats {
        let mut stats = Stats::new();
        let times = vec![0.0; values.len()];
        stats.update_batch(&times, &ColumnArray::F64(values.to_vec().into()));
        stats
    }

    #[test]
    fn the_statistics_of_some_values_are_their_count_extremes_mean_and_deviation() {
        let stats = fed(&[2.0, 4.0, 4.0, 4.0, 5.0, 5.0, 7.0, 9.0]);
        assert_eq!(stats.count(), 8);
        assert_eq!((stats.min(), stats.max()), (Some(2.0), Some(9.0)));
        assert_eq!(stats.mean(), Some(5.0));
        assert_eq!(stats.std_dev(), Some(2.0));
    }

    #[test]
    fn no_values_have_no_statistics_and_one_that_is_no_number_is_passed_over() {
        let none = Stats::new();
        assert_eq!(none.count(), 0);
        assert_eq!(
            (none.min(), none.mean(), none.std_dev()),
            (None, None, None)
        );
        let stats = fed(&[f64::NAN, 1.0, f64::INFINITY, 3.0]);
        assert_eq!(stats.count(), 2);
        assert_eq!((stats.min(), stats.max()), (Some(1.0), Some(3.0)));
        assert_eq!((stats.mean(), stats.std_dev()), (Some(2.0), Some(1.0)));
    }

    /// Values fed in pieces sum up as fed at once, and far from zero the
    /// deviation keeps its digits.
    #[test]
    fn values_fed_in_pieces_sum_up_as_fed_at_once() {
        let values: Vec<f64> = (0..1000).map(|i| 1e9 + f64::from(i % 7)).collect();
        let whole = fed(&values);
        let mut pieces = Stats::new();
        for piece in values.chunks(33) {
            let times = vec![0.0; piece.len()];
            pieces.update_batch(&times, &ColumnArray::F64(piece.to_vec().into()));
        }
        assert_eq!(pieces.count(), whole.count());
        let close = |a: Option<f64>, b: Option<f64>| (a.unwrap() - b.unwrap()).abs() < 1e-6;
        assert!(close(pieces.mean(), whole.mean()));
        assert!(close(pieces.std_dev(), whole.std_dev()));
        let mean = values.iter().sum::<f64>() / 1000.0;
        let square = values.iter().map(|v| (v - mean).powi(2)).sum::<f64>() / 1000.0;
        assert!((whole.std_dev().unwrap() - square.sqrt()).abs() < 1e-6);
        assert_eq!(whole.continuity(), Continuity::Monotonic);
    }
}
