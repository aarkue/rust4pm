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
}

/// Options for [`get_event_timestamps`].
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
/// activity are counted under `"UNKNOWN"`.
#[register_binding]
pub fn get_event_timestamps(
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
    else {
        return AggregatedEventTimestamps {
            events_per_timestamp: HashMap::default(),
            activities: activities.into_iter().cloned().collect(),
        };
    };
    let bin_size = (max - min) as f64 / num_bins as f64;
    let date_bins: Vec<_> = (0..num_bins)
        .map(|bin_index| (min as f64 + (bin_index as f64 + 0.5) * bin_size).round() as i64)
        .collect();
    let mut ev_counts: HashMap<i64, HashMap<String, usize>> = HashMap::new();
    for (timestamp, act) in &timestamps_with_act {
        let bin_index = (((timestamp - min) as f64 / bin_size).floor() as usize).min(num_bins - 1);
        *ev_counts
            .entry(date_bins[bin_index])
            .or_default()
            .entry(act.clone())
            .or_default() += 1;
    }
    AggregatedEventTimestamps {
        events_per_timestamp: ev_counts,
        activities: activities.into_iter().cloned().collect(),
    }
}
