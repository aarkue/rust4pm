//! Dotted Chart Analysis
//!
//! Provides functionality for generating configurable multi-axis dotted chart
//! visualizations from event logs.

use std::collections::HashMap;

use chrono::{DateTime, FixedOffset};

use itertools::Itertools;
use macros_process_mining::register_binding;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::{
    core::event_data::case_centric::{Event, Trace, XESEditableAttribute},
    EventLog,
};

const DEFAULT_TIMESTAMP_KEY: &str = "time:timestamp";

/// Extract the timestamp from an event using the given attribute key.
fn get_event_time<'a>(event: &'a Event, timestamp_key: &str) -> Option<&'a DateTime<FixedOffset>> {
    event
        .attributes
        .get_by_key(timestamp_key)
        .and_then(|a| a.value.try_as_date())
}

/// X-axis mode for the dotted chart.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub enum DottedChartXAxis {
    /// Absolute event timestamp (in milliseconds).
    Time,
    /// Elapsed time since the first event in the case (in milliseconds).
    TimeSinceCaseStart,
    /// Relative position within the case duration (`0.0`..`1.0`).
    TimeRelativeToCaseDuration,
    /// Event index within the case (starting at `0`).
    StepNumberSinceCaseStart,
}

impl DottedChartXAxis {
    /// Compute the x-axis value for an event.
    pub fn get_value(
        &self,
        trace: &Trace,
        event: &Event,
        event_index: usize,
        timestamp_key: &str,
    ) -> f64 {
        match self {
            DottedChartXAxis::Time => get_event_time(event, timestamp_key)
                .map(|t| t.timestamp_millis() as f64)
                .unwrap_or_default(),
            DottedChartXAxis::TimeSinceCaseStart => {
                let first_time = trace
                    .events
                    .first()
                    .and_then(|e| get_event_time(e, timestamp_key));
                let event_time = get_event_time(event, timestamp_key);
                match (first_time, event_time) {
                    (Some(first), Some(current)) => (*current - first).num_milliseconds() as f64,
                    _ => 0.0,
                }
            }
            DottedChartXAxis::StepNumberSinceCaseStart => event_index as f64,
            DottedChartXAxis::TimeRelativeToCaseDuration => {
                let first_time = trace
                    .events
                    .first()
                    .and_then(|e| get_event_time(e, timestamp_key));
                let last_time = trace
                    .events
                    .last()
                    .and_then(|e| get_event_time(e, timestamp_key));
                let event_time = get_event_time(event, timestamp_key);
                match (first_time, last_time, event_time) {
                    (Some(first), Some(last), Some(current)) => {
                        let case_duration = (*last - *first).num_milliseconds() as f64;
                        if case_duration.abs() < f64::EPSILON {
                            0.0
                        } else {
                            (*current - *first).num_milliseconds() as f64 / case_duration
                        }
                    }
                    _ => 0.0,
                }
            }
        }
    }
}

/// Y-axis mode for the dotted chart.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub enum DottedChartYAxis {
    /// Group by case (using `concept:name` trace attribute).
    Case,
    /// Group by resource (using `org:resource` event attribute).
    Resource,
    /// Group by a custom event attribute.
    EventAttribute(String),
    /// Group by a custom case (trace) attribute.
    CaseAttribute(String),
}

impl DottedChartYAxis {
    /// Compute the y-axis label for an event.
    pub fn get_value(&self, trace: &Trace, event: &Event) -> String {
        let attr = match self {
            DottedChartYAxis::Case => trace.attributes.get_by_key("concept:name"),
            DottedChartYAxis::Resource => event.attributes.get_by_key("org:resource"),
            DottedChartYAxis::EventAttribute(attr_name) => event.attributes.get_by_key(attr_name),
            DottedChartYAxis::CaseAttribute(attr_name) => trace.attributes.get_by_key(attr_name),
        };
        attr.and_then(|a| a.value.try_as_string())
            .cloned()
            .unwrap_or_else(|| "UNKNOWN".to_string())
    }
}

/// Color-axis mode for the dotted chart.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub enum DottedChartColorAxis {
    /// Color by activity (using `concept:name` event attribute).
    Activity,
    /// Color by resource (using `org:resource` event attribute).
    Resource,
    /// Color by case (using `concept:name` trace attribute).
    Case,
    /// Color by a custom event attribute.
    EventAttribute(String),
    /// Color by a custom case (trace) attribute.
    CaseAttribute(String),
}

impl DottedChartColorAxis {
    /// Compute the color-axis label for an event.
    pub fn get_value(&self, trace: &Trace, event: &Event) -> String {
        let attr = match self {
            DottedChartColorAxis::Activity => event.attributes.get_by_key("concept:name"),
            DottedChartColorAxis::Resource => event.attributes.get_by_key("org:resource"),
            DottedChartColorAxis::Case => trace.attributes.get_by_key("concept:name"),
            DottedChartColorAxis::EventAttribute(attr_name) => {
                event.attributes.get_by_key(attr_name)
            }
            DottedChartColorAxis::CaseAttribute(attr_name) => {
                trace.attributes.get_by_key(attr_name)
            }
        };
        attr.and_then(|a| a.value.try_as_string())
            .cloned()
            .unwrap_or_else(|| "UNKNOWN".to_string())
    }
}

/// Result of [`get_dotted_chart`], containing the plotted points grouped by color.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct DottedChartData {
    /// Points grouped by color-axis value.
    pub dots_per_color: HashMap<String, DottedChartPoints>,
    /// Ordered list of y-axis labels (index corresponds to [`DottedChartPoints::y`] values).
    pub y_values: Vec<String>,
}

/// A series of (x, y) coordinates for one color group in a dotted chart.
#[derive(Debug, Default, Clone, Serialize, Deserialize, JsonSchema)]
pub struct DottedChartPoints {
    /// X-axis values (interpretation depends on [`DottedChartXAxis`]).
    pub x: Vec<f64>,
    /// Y-axis indices into [`DottedChartData::y_values`].
    pub y: Vec<usize>,
}

/// Options for [`get_dotted_chart`].
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct DottedChartOptions {
    /// X-axis mode.
    pub x_axis: DottedChartXAxis,
    /// Y-axis mode.
    pub y_axis: DottedChartYAxis,
    /// Color-axis mode.
    pub color_axis: DottedChartColorAxis,
    /// Event attribute key used to extract the timestamp.
    pub timestamp_key: String,
}

impl Default for DottedChartOptions {
    fn default() -> Self {
        Self {
            x_axis: DottedChartXAxis::Time,
            y_axis: DottedChartYAxis::Case,
            color_axis: DottedChartColorAxis::Activity,
            timestamp_key: DEFAULT_TIMESTAMP_KEY.to_string(),
        }
    }
}

#[register_binding(stringify_error)]
/// Generate dotted chart data from an event log.
///
/// Traces are sorted by the timestamp of their first event. Each event
/// produces one dot with coordinates determined by the configured axes.
pub fn get_dotted_chart(
    xes: &EventLog,
    #[bind(default)] options: &DottedChartOptions,
) -> Result<DottedChartData, String> {
    let DottedChartOptions {
        x_axis,
        y_axis,
        color_axis,
        timestamp_key,
    } = options;
    let mut y_values: HashMap<String, usize> = HashMap::default();
    let mut data_per_color: HashMap<String, DottedChartPoints> = HashMap::default();

    xes.traces
        .iter()
        .sorted_by_cached_key(|t: &&Trace| {
            t.events
                .first()
                .and_then(|e| get_event_time(e, timestamp_key))
                .cloned()
        })
        .for_each(|t| {
            t.events.iter().enumerate().for_each(|(e_index, e)| {
                let color_value = color_axis.get_value(t, e);
                let points = data_per_color.entry(color_value).or_default();

                let y_key = y_axis.get_value(t, e);
                let y_index = match y_values.get(&y_key) {
                    Some(&idx) => idx,
                    None => {
                        let next = y_values.len();
                        y_values.insert(y_key, next);
                        next
                    }
                };

                points.y.push(y_index);
                points
                    .x
                    .push(x_axis.get_value(t, e, e_index, timestamp_key));
            })
        });

    Ok(DottedChartData {
        dots_per_color: data_per_color,
        y_values: y_values
            .into_iter()
            .sorted_by_key(|(_, i)| *i)
            .map(|(v, _)| v)
            .collect(),
    })
}
