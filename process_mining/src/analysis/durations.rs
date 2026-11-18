//! Summary statistics over a set of durations
//!
//! Unit-agnostic: the summary is in whatever unit the input values are (seconds, milliseconds, ...).

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// Distribution summary of a non-empty set of durations
///
/// Quantiles use the nearest-rank method (see [`quantile_sorted`]), so each is one of the input
/// values. The median is the usual one, averaging the two middle values for an even count.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct DurationSummary {
    /// Number of values
    pub count: usize,
    /// Smallest value
    pub min: f64,
    /// Largest value
    pub max: f64,
    /// Arithmetic mean
    pub mean: f64,
    /// Median
    pub median: f64,
    /// 10th percentile
    pub p10: f64,
    /// 25th percentile
    pub p25: f64,
    /// 75th percentile
    pub p75: f64,
    /// 90th percentile
    pub p90: f64,
    /// 95th percentile
    pub p95: f64,
    /// 99th percentile
    pub p99: f64,
}

/// Summarize `durations`, sorting the slice in place
///
/// Returns `None` for an empty slice. NaN values sort last (by [`f64::total_cmp`]) and so end up
/// in `max` and the upper quantiles; filter them out first if that is not wanted.
pub fn summarize(durations: &mut [f64]) -> Option<DurationSummary> {
    if durations.is_empty() {
        return None;
    }
    durations.sort_by(f64::total_cmp);
    let sorted = &*durations;
    let n = sorted.len();
    let mid = n / 2;
    let median = if n.is_multiple_of(2) {
        (sorted[mid - 1] + sorted[mid]) / 2.0
    } else {
        sorted[mid]
    };
    Some(DurationSummary {
        count: n,
        min: sorted[0],
        max: sorted[n - 1],
        mean: sorted.iter().sum::<f64>() / n as f64,
        median,
        p10: quantile_sorted(sorted, 0.1),
        p25: quantile_sorted(sorted, 0.25),
        p75: quantile_sorted(sorted, 0.75),
        p90: quantile_sorted(sorted, 0.9),
        p95: quantile_sorted(sorted, 0.95),
        p99: quantile_sorted(sorted, 0.99),
    })
}

/// Nearest-rank quantile `q` (in `0.0..=1.0`, clamped) of an ascending, non-empty slice
///
/// # Panics
/// If `sorted` is empty.
pub fn quantile_sorted(sorted: &[f64], q: f64) -> f64 {
    let rank = (q.clamp(0.0, 1.0) * sorted.len() as f64).ceil() as usize;
    sorted[rank.saturating_sub(1).min(sorted.len() - 1)]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn summarize_matches_hand_computed_values() {
        let mut values: Vec<f64> = (1..=10).rev().map(f64::from).collect();
        let s = summarize(&mut values).unwrap();
        assert_eq!(s.count, 10);
        assert_eq!((s.min, s.max, s.mean, s.median), (1.0, 10.0, 5.5, 5.5));
        assert_eq!((s.p10, s.p25, s.p75, s.p90), (1.0, 3.0, 8.0, 9.0));
        assert_eq!((s.p95, s.p99), (10.0, 10.0));
        assert_eq!(values[0], 1.0, "sorted in place");

        let s = summarize(&mut [4.0, 2.0, 9.0]).unwrap();
        assert_eq!((s.min, s.median, s.max), (2.0, 4.0, 9.0));
        assert!(summarize(&mut []).is_none());
    }

    #[test]
    fn quantile_clamps_and_handles_one_value() {
        assert_eq!(quantile_sorted(&[7.0], 0.5), 7.0);
        assert_eq!(quantile_sorted(&[1.0, 2.0], -1.0), 1.0);
        assert_eq!(quantile_sorted(&[1.0, 2.0], 2.0), 2.0);
        assert_eq!(quantile_sorted(&[1.0, 2.0], 0.0), 1.0);
    }
}
