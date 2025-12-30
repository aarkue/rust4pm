//! Conversion of Event Data from/to polars `DataFrame`s
//!
//! 🔐 Requires the `dataframes` feature to be enabled.
use std::{collections::HashSet, time::Instant};

use chrono::DateTime;
use polars::prelude::*;
use rayon::prelude::{IntoParallelRefIterator, ParallelIterator};

use crate::core::{
    event_data::case_centric::{
        constants::PREFIXED_TRACE_ID_NAME,
        event_log_struct::{
            Attribute, AttributeValue, Attributes, Event, Trace, XESEditableAttribute,
        },
    },
    EventLog,
};

///
/// Prefix to attribute keys for trace-level attributes (e.g., when "flattening" the log to a [`DataFrame`])
///
pub const TRACE_PREFIX: &str = "case:";

///
/// Convert a attribute ([Attribute]) to an [`AnyValue`]
///
/// Used for converting values and data types to the `DataFrame` equivalent
///
/// The UTC timezone argument is used to correctly convert to `AnyValue::Datetime` with UTC timezone
///
fn attribute_to_any_value<'a>(from_option: Option<&Attribute>) -> AnyValue<'a> {
    match from_option {
        Some(from) => {
            let x = attribute_value_to_any_value(&from.value);
            x
        }
        None => AnyValue::Null,
    }
}

///
/// Convert a attribute ([`AttributeValue`]) to an [`AnyValue`]
///
/// Used for converting values and data types to the `DataFrame` equivalent
///
/// The UTC timezone argument is used to correctly convert to `AnyValue::Datetime` with UTC timezone
///
fn attribute_value_to_any_value<'a>(from: &AttributeValue) -> AnyValue<'a> {
    match from {
        AttributeValue::String(v) => AnyValue::StringOwned(v.into()),
        AttributeValue::Date(v) => {
            // Fallback for testing:
            // return AnyValue::StringOwned(v.to_string().into());
            AnyValue::Datetime(
                v.timestamp_nanos_opt().unwrap(),
                polars::prelude::TimeUnit::Nanoseconds,
                None,
            )
        }
        AttributeValue::Int(v) => AnyValue::Int64(*v),
        AttributeValue::Float(v) => AnyValue::Float64(*v),
        AttributeValue::Boolean(v) => AnyValue::Boolean(*v),
        AttributeValue::ID(v) => {
            let s = v.to_string();
            AnyValue::StringOwned(s.into())
        }
        // TODO: Add proper List/Container support
        AttributeValue::List(l) => AnyValue::StringOwned(format!("{l:?}").into()),
        AttributeValue::Container(c) => AnyValue::StringOwned(format!("{c:?}").into()),
        AttributeValue::None() => AnyValue::Null,
    }
}

///
/// Convert an [`EventLog`] to a Polars [`DataFrame`]
///
/// Flattens event log and adds trace-level attributes to events with prefixed attribute key (see [`TRACE_PREFIX`])
///
/// Note: This function is only available if the `dataframes` feature is enabled.
///
pub fn convert_log_to_dataframe(
    log: &EventLog,
    print_debug: bool,
) -> Result<DataFrame, PolarsError> {
    if print_debug {
        println!("Starting converting log to DataFrame");
    }
    let mut now = Instant::now();
    let all_attributes: HashSet<String> = log
        .traces
        .par_iter()
        .flat_map(|t| {
            let trace_attrs: HashSet<String> = t
                .attributes
                .iter()
                .map(|a| TRACE_PREFIX.to_string() + a.key.as_str())
                .collect();
            let m: HashSet<String> = t
                .events
                .iter()
                .flat_map(|e| {
                    e.attributes
                        .iter()
                        .map(|a| a.key.clone())
                        .collect::<Vec<String>>()
                })
                .collect();
            [trace_attrs, m]
        })
        .flatten()
        .collect();
    if print_debug {
        println!("Gathering all attributes took {:.2?}", now.elapsed());
    }
    now = Instant::now();
    let x: Vec<Series> = all_attributes
        .par_iter()
        .map(|k: &String| {
            let mut entries: Vec<AnyValue<'_>> = log
                .traces
                .iter()
                .flat_map(|t| -> Vec<AnyValue<'_>> {
                    if k.starts_with(TRACE_PREFIX) {
                        let trace_k: String = k.chars().skip(TRACE_PREFIX.len()).collect();
                        vec![
                            attribute_to_any_value(
                                t.attributes
                                    .get_by_key_or_global(&trace_k, &log.global_trace_attrs),
                            );
                            t.events.len()
                        ]
                    } else {
                        t.events
                            .iter()
                            .map(|e| {
                                attribute_to_any_value(
                                    e.attributes
                                        .get_by_key_or_global(k, &log.global_event_attrs),
                                )
                            })
                            .collect()
                    }
                })
                .collect();

            #[allow(clippy::mutable_key_type)]
            let mut unique_dtypes: HashSet<DataType> = entries.iter().map(|v| v.dtype()).collect();
            unique_dtypes.remove(&DataType::Null);
            if unique_dtypes.len() > 1 {
                eprintln!(
                    "Warning: Attribute {k} contains values of different dtypes ({unique_dtypes:?})"
                );
                if unique_dtypes
                    == vec![DataType::Float64, DataType::Int64]
                        .into_iter()
                        .collect()
                {
                    entries = entries
                        .into_iter()
                        .map(|val| match val {
                            AnyValue::Int64(n) => AnyValue::Float64(n as f64),
                            x => x,
                        })
                        .collect();
                } else {
                    entries = entries
                        .into_iter()
                        .map(|val| match val {
                            AnyValue::Null => AnyValue::Null,
                            AnyValue::String(s) => AnyValue::String(s),
                            x => AnyValue::StringOwned(x.to_string().into()),
                        })
                        .collect();
                }
            }
            Series::new(k.into(), entries)
        })
        .collect();
    if print_debug {
        println!(
            "Creating a Series for every Attribute took {:.2?}",
            now.elapsed()
        );
    }
    now = Instant::now();
    let df = unsafe {
        DataFrame::new_no_checks(
            x.first().map(|e| e.len()).unwrap_or_default(),
            x.into_iter().map(|s| Column::Series(s.into())).collect(),
        )
    };
    if print_debug {
        println!(
            "Constructing DF from Attribute Series took {:.2?}",
            now.elapsed()
        );
    }
    Ok(df)
}

fn any_value_to_attribute_value(from: &AnyValue<'_>) -> AttributeValue {
    match from {
        AnyValue::Null => AttributeValue::None(),
        AnyValue::Boolean(v) => AttributeValue::Boolean(*v),
        AnyValue::String(v) => AttributeValue::String(v.to_string()),
        AnyValue::StringOwned(v) => AttributeValue::String(v.to_string()),
        AnyValue::UInt8(v) => AttributeValue::Int((*v).into()),
        AnyValue::UInt16(v) => AttributeValue::Int((*v).into()),
        AnyValue::UInt32(v) => AttributeValue::Int((*v).into()),
        // // AnyValue::UInt64(v) => AttributeValue::Int((*v).into()),
        AnyValue::Int8(v) => AttributeValue::Int((*v).into()),
        AnyValue::Int16(v) => AttributeValue::Int((*v).into()),
        AnyValue::Int32(v) => AttributeValue::Int((*v).into()),
        AnyValue::Int64(v) => AttributeValue::Int(*v),
        AnyValue::Float32(v) => AttributeValue::Float((*v).into()),
        AnyValue::Float64(v) => AttributeValue::Float(*v),
        AnyValue::Datetime(ns, _, _) => {
            // Convert nanos to micros; tz is not used!
            let d: DateTime<_> = DateTime::from_timestamp_micros(ns / 1000)
                .unwrap()
                .fixed_offset();
            AttributeValue::Date(d)
        }
        x => AttributeValue::String(format!("{x:?}")),
    }
}

/// Convert Polars [`DataFrame`] to [`EventLog`]
///
///  - Extracts attributes as Strings (converting other formats using debug format macro)
///  - Assumes valid `EventLog` structure of `DataFrame` (i.e., assuming that [`PREFIXED_TRACE_ID_NAME`] is present)
///
/// Note: This function is only available if the `dataframes` feature is enabled.
///
pub fn convert_dataframe_to_log(df: &DataFrame) -> Result<EventLog, PolarsError> {
    let groups = df.partition_by_stable([PREFIXED_TRACE_ID_NAME], true)?;
    let columns = df.get_column_names();
    let mut log = EventLog {
        attributes: Attributes::default(),
        traces: vec![],
        classifiers: None,
        extensions: None,
        global_trace_attrs: None,
        global_event_attrs: None,
    };
    let traces: Vec<Trace> = groups
        .par_iter()
        .map(|g| {
            let mut trace_attributes: HashSet<Attribute> = HashSet::new();
            let events: Vec<Event> = (0..g.height())
                .map(|i| {
                    let mut event_attributes: HashSet<Attribute> = HashSet::new();
                    columns
                        .iter()
                        .zip(g.get_row(i).unwrap().0.iter())
                        .for_each(|(c, v)| {
                            let value = any_value_to_attribute_value(v);
                            match value {
                                AttributeValue::None() => {
                                    // Skip!
                                }
                                value => {
                                    if c.starts_with(TRACE_PREFIX) {
                                        // e.g.,
                                        let (_, c) = c.split_once(TRACE_PREFIX).unwrap();
                                        trace_attributes
                                            .insert(Attribute::new(c.to_string(), value));
                                    } else {
                                        event_attributes
                                            .insert(Attribute::new(c.to_string(), value));
                                    }
                                }
                            }
                        });

                    Event {
                        attributes: event_attributes.into_iter().collect(),
                    }
                })
                .collect();
            Trace {
                attributes: trace_attributes.into_iter().collect(),
                events,
            }
        })
        .collect();
    log.traces = traces;
    Ok(log)
}

#[cfg(test)]
mod df_xes_tests {
    use std::time::Instant;

    use crate::{
        core::event_data::case_centric::{
            dataframe::convert_log_to_dataframe,
            xes::import_xes::{import_xes_path, XESImportOptions},
        },
        test_utils::get_test_data_path,
    };

    #[test]
    fn basic_xes() {
        let now = Instant::now();
        let now_total = Instant::now();
        let path = get_test_data_path()
            .join("xes")
            .join("Road_Traffic_Fine_Management_Process.xes.gz");
        let log = import_xes_path(
            path,
            XESImportOptions {
                ..Default::default()
            },
        )
        .unwrap();
        let num_events = log.traces.iter().map(|t| t.events.len()).sum::<usize>();
        println!(
            "Got log with {} traces in {:?}",
            log.traces.len(),
            now.elapsed()
        );
        assert_eq!(log.traces.len(), 150_370);
        assert_eq!(num_events, 561_470);
        let now = Instant::now();
        let converted_log = convert_log_to_dataframe(&log, true).unwrap();
        println!(
            "Converted to DF with shape {:?} in {:?}",
            converted_log.shape(),
            now.elapsed()
        );
        println!("Total: {:?}\n\n", now_total.elapsed());
        assert_eq!(converted_log.shape(), (num_events, 16));
    }
}
