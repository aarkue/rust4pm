use super::graph::EntityRef;
use super::paths::Connection;
use hashbrown::{HashMap, HashSet};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// Quality metrics for a path schema, computed from its instance connections.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct SchemaMetrics {
    /// Number of distinct (source, target) pairs connected.
    pub support: usize,
    /// Fraction of source-type instances with at least one connection.
    pub coverage: f64,
    /// Inverse average fan-out: `1 / (avg distinct targets per connected source)`. High = discriminating.
    pub selectivity: f64,
    /// Total number of connections.
    pub path_count: usize,
    /// Number of distinct source entities with at least one connection.
    pub sources_with_paths: usize,
    /// Total number of source entities of this type.
    pub total_sources: usize,
    /// Fraction of target-type instances reached.
    pub reach: f64,
    /// Inverse average fan-in: `|distinct targets| / support`. High = each target reached by few sources.
    pub exclusivity: f64,
}

impl std::fmt::Display for SchemaMetrics {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "support={}, coverage={:.3}, selectivity={:.3}, reach={:.3}, exclusivity={:.3}, paths={}",
            self.support, self.coverage, self.selectivity, self.reach, self.exclusivity, self.path_count
        )
    }
}

/// Compute schema metrics from a set of connections.
pub fn compute_metrics(
    connections: &[Connection],
    total_sources: usize,
    total_targets: usize,
) -> SchemaMetrics {
    if connections.is_empty() {
        return SchemaMetrics {
            support: 0,
            coverage: 0.0,
            selectivity: 0.0,
            path_count: 0,
            sources_with_paths: 0,
            total_sources,
            reach: 0.0,
            exclusivity: 0.0,
        };
    }

    let mut targets_per_source: HashMap<EntityRef, HashSet<EntityRef>> = HashMap::new();
    let mut distinct_targets: HashSet<EntityRef> = HashSet::new();
    for c in connections {
        targets_per_source
            .entry(c.source)
            .or_default()
            .insert(c.target);
        distinct_targets.insert(c.target);
    }

    let support: usize = targets_per_source.values().map(HashSet::len).sum();
    let sources_with_paths = targets_per_source.len();
    let num_distinct_targets = distinct_targets.len();

    // After the empty-connections early return, support >= sources_with_paths >= 1, so the
    // inverse average fan-out is always well-defined.
    let selectivity = sources_with_paths as f64 / support as f64;

    SchemaMetrics {
        support,
        coverage: fraction(sources_with_paths, total_sources),
        selectivity,
        path_count: connections.len(),
        sources_with_paths,
        total_sources,
        reach: fraction(num_distinct_targets, total_targets),
        exclusivity: fraction(num_distinct_targets, support),
    }
}

/// Build [`SchemaMetrics`] from aggregate counts, for streaming discovery that never
/// materializes the connection list. `support` is the number of distinct (source, target)
/// pairs and `distinct_targets` the number of distinct reached targets; `path_count` equals
/// `support`, as the streaming summary only tracks deduplicated pairs.
pub fn metrics_from_counts(
    support: usize,
    sources_with_paths: usize,
    total_sources: usize,
    distinct_targets: usize,
    total_targets: usize,
) -> SchemaMetrics {
    let selectivity = if support > 0 {
        sources_with_paths as f64 / support as f64
    } else {
        0.0
    };
    SchemaMetrics {
        support,
        coverage: fraction(sources_with_paths, total_sources),
        selectivity,
        path_count: support,
        sources_with_paths,
        total_sources,
        reach: fraction(distinct_targets, total_targets),
        exclusivity: fraction(distinct_targets, support),
    }
}

/// Throughput summary `(min, max, mean, median)` from precomputed durations (seconds).
pub fn throughput_from_durations(durations: Vec<f64>) -> Option<(f64, f64, f64, f64)> {
    summarize_durations(durations)
}

/// Throughput durations (seconds, millisecond precision) for every event-to-event connection.
pub fn throughput_durations(connections: &[Connection]) -> Vec<f64> {
    connections
        .iter()
        .filter_map(|c| match (c.source_time, c.target_time) {
            (Some(st), Some(tt)) => {
                Some(tt.signed_duration_since(st).num_milliseconds() as f64 / 1000.0)
            }
            _ => None,
        })
        .collect()
}

/// Throughput summary `(min, max, mean, median)` over event-to-event connections.
pub fn compute_throughput_times(connections: &[Connection]) -> Option<(f64, f64, f64, f64)> {
    summarize_durations(throughput_durations(connections))
}

fn fraction(numerator: usize, denominator: usize) -> f64 {
    if denominator > 0 {
        numerator as f64 / denominator as f64
    } else {
        0.0
    }
}

fn summarize_durations(mut durations: Vec<f64>) -> Option<(f64, f64, f64, f64)> {
    if durations.is_empty() {
        return None;
    }
    durations.sort_by(|a, b| a.total_cmp(b));
    let min = durations[0];
    let max = *durations.last().unwrap();
    let mean = durations.iter().sum::<f64>() / durations.len() as f64;
    let mid = durations.len() / 2;
    let median = if durations.len() % 2 == 0 {
        (durations[mid - 1] + durations[mid]) / 2.0
    } else {
        durations[mid]
    };
    Some((min, max, mean, median))
}
