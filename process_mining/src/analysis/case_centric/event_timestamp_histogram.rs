//! Aggregated Event Timestamp Histogram
//!
//! Bins event timestamps and groups counts by activity, useful for
//! visualizing event distributions over time.

use std::collections::{HashMap, HashSet};

use itertools::Itertools;
use macros_process_mining::register_binding;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::{core::event_data::case_centric::XESEditableAttribute, EventLog};

const DEFAULT_ACTIVITY_KEY: &str = "concept:name";
const DEFAULT_TIMESTAMP_KEY: &str = "time:timestamp";
const DEFAULT_NUM_BINS: usize = 100;

/// Aggregated event counts per timestamp bin, grouped by activity.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct AggregatedEventTimestamps {
    /// Event counts per bin timestamp (millis) per activity name.
    pub events_per_timestamp: HashMap<i64, HashMap<String, usize>>,
    /// All distinct activity names found in the log.
    pub activities: Vec<String>,
    /// Width of each bin in milliseconds, which is also the spacing of the bin centers.
    ///
    /// Bins are keyed by their center, so one spans
    /// `[center - bin_width_ms / 2, center + bin_width_ms / 2)`. `0` means there are no bins.
    pub bin_width_ms: i64,
}

/// Options for [`event_timestamp_histogram`].
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct EventTimestampOptions {
    /// Number of time bins to aggregate events into.
    pub num_bins: usize,
    /// Event attribute key used to identify the activity name.
    pub activity_key: String,
    /// Event attribute key used to extract the timestamp.
    pub timestamp_key: String,
}

impl Default for EventTimestampOptions {
    fn default() -> Self {
        Self {
            num_bins: DEFAULT_NUM_BINS,
            activity_key: DEFAULT_ACTIVITY_KEY.to_string(),
            timestamp_key: DEFAULT_TIMESTAMP_KEY.to_string(),
        }
    }
}

/// Aggregate event timestamps into a fixed number of bins, grouped by activity.
///
/// Events without a valid timestamp are skipped; events without a recognized
/// activity are counted under `"UNKNOWN"`. With `num_bins` set to `0`, there are no bins.
#[register_binding]
pub fn event_timestamp_histogram(
    log: &EventLog,
    #[bind(default)] options: EventTimestampOptions,
) -> AggregatedEventTimestamps {
    let EventTimestampOptions {
        num_bins,
        activity_key,
        timestamp_key,
    } = options;
    let timestamps_with_act: Vec<_> = log
        .traces
        .iter()
        .flat_map(|t| {
            t.events.iter().flat_map(|e| {
                let time = e
                    .attributes
                    .get_by_key(&timestamp_key)
                    .and_then(|a| a.value.try_as_date())?;
                let act = e
                    .attributes
                    .get_by_key(&activity_key)
                    .and_then(|a| a.value.try_as_string().cloned())
                    .unwrap_or_else(|| "UNKNOWN".to_string());
                Some((time.timestamp_millis(), act))
            })
        })
        .collect();
    let activities: HashSet<_> = timestamps_with_act.iter().map(|(_, a)| a).collect();
    let Some((&min, &max)) = timestamps_with_act
        .iter()
        .map(|(t, _)| t)
        .minmax()
        .into_option()
        .filter(|_| num_bins > 0)
    else {
        return AggregatedEventTimestamps {
            events_per_timestamp: HashMap::default(),
            activities: activities.into_iter().cloned().collect(),
            bin_width_ms: 0,
        };
    };
    // Bin centers are whole milliseconds, so rounding the width up keeps it equal to the center
    // spacing even when the span is shorter than the number of bins.
    let bin_width_ms = (((max - min) as f64 / num_bins as f64).ceil() as i64).max(1);
    let mut ev_counts: HashMap<i64, HashMap<String, usize>> = HashMap::new();
    for (timestamp, act) in &timestamps_with_act {
        // Clamped in `i64`, as `num_bins` comes from the caller and need not fit
        let bin_index =
            ((timestamp - min) / bin_width_ms).min(i64::try_from(num_bins - 1).unwrap_or(i64::MAX));
        *ev_counts
            .entry(min + bin_index * bin_width_ms + bin_width_ms / 2)
            .or_default()
            .entry(act.clone())
            .or_default() += 1;
    }
    AggregatedEventTimestamps {
        events_per_timestamp: ev_counts,
        activities: activities.into_iter().cloned().collect(),
        bin_width_ms,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn zero_bins_give_no_bins() {
        let log = crate::event_log!(["a", "b"], ["a", "c"]);
        let options = EventTimestampOptions {
            num_bins: 0,
            ..Default::default()
        };
        let result = event_timestamp_histogram(&log, options);
        assert!(result.events_per_timestamp.is_empty());
        assert_eq!(result.bin_width_ms, 0);
    }

    #[test]
    fn huge_num_bins_allocate_no_bins_up_front() {
        let log = crate::event_log!(["a", "b"], ["a", "c"]);
        let options = EventTimestampOptions {
            num_bins: usize::MAX,
            ..Default::default()
        };
        let result = event_timestamp_histogram(&log, options);
        assert_eq!(result.bin_width_ms, 1);
    }
}
