//! Reductions whose result does not depend on how work was scheduled.
//!
//! Floating-point addition is not associative, so a sum whose order follows
//! the scheduler changes by an ulp from run to run. Every reduction across
//! channels goes through a tree whose shape is fixed by the operand count.

/// Sums `values` in a balanced tree whose shape depends only on their count.
pub fn tree_sum(values: &[f64]) -> f64 {
    match values {
        [] => 0.0,
        [value] => *value,
        _ => {
            let (left, right) = values.split_at(values.len() / 2);
            tree_sum(left) + tree_sum(right)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::tree_sum;

    #[test]
    fn the_tree_pairs_operands_by_position() {
        // Chosen so that the grouping is visible in the result. Each half
        // absorbs its 1.0, so the tree gives 0, where a left-to-right sum
        // keeps the last 1.0.
        let values = [1e16, 1.0, -1e16, 1.0];
        assert_eq!(tree_sum(&values), (1e16 + 1.0) + (-1e16 + 1.0));
        assert_eq!(tree_sum(&values), 0.0);
        assert_eq!(values.iter().sum::<f64>(), 1.0);
        assert_eq!(tree_sum(&values[..3]), 1e16 + (1.0 + -1e16));
        assert_eq!(tree_sum(&[]), 0.0);
        assert_eq!(tree_sum(&[2.5]), 2.5);
    }
}
