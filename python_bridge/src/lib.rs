use chrono::DateTime;
use chrono::NaiveDateTime;
use pm_rust::add_start_end_acts;
use pm_rust::alphappp::full::alphappp_discover_petri_net;
use pm_rust::alphappp::full::AlphaPPPConfig;
use pm_rust::event_log::activity_projection::EventLogActivityProjection;
use pm_rust::event_log::constants::PREFIXED_TRACE_ID_NAME;
use pm_rust::event_log::constants::TRACE_PREFIX;
use pm_rust::event_log::event_log_struct::EventLogClassifier;
use pm_rust::event_log::event_log_struct::EventLogExtension;
use pm_rust::event_log::import_xes::import_xes_file;
use pm_rust::json_to_petrinet;
use pm_rust::petri_net::petri_net_struct::PetriNet;
use pm_rust::petrinet_to_json;
use pm_rust::Attribute;
use pm_rust::AttributeAddable;
use pm_rust::AttributeValue;
use pm_rust::Attributes;
use pm_rust::Event;
use pm_rust::EventLog;
use pm_rust::Trace;
use pm_rust::Utc;
use polars::prelude::AnyValue;
use polars::prelude::DataFrame;
use polars::prelude::DataType;
use polars::prelude::NamedFrom;
use polars::prelude::PolarsError;
use polars::prelude::SerReader;
use polars::series::Series;
use pyo3::exceptions::PyTypeError;
use pyo3::prelude::*;
use pyo3::Python;
use pyo3_polars::PyDataFrame;
use rayon::prelude::IntoParallelRefIterator;
use rayon::prelude::ParallelIterator;
use serde::Deserialize;
use serde::Serialize;
use std::collections::HashSet;
use std::io::Cursor;
use std::time::Instant;

fn any_value_to_attribute_value(from: &AnyValue) -> AttributeValue {
    match from {
        AnyValue::Null => AttributeValue::None(),
        AnyValue::Boolean(v) => AttributeValue::Boolean(*v),
        AnyValue::Utf8(v) => AttributeValue::String(v.to_string()),
        AnyValue::UInt8(v) => AttributeValue::Int((*v).into()),
        AnyValue::UInt16(v) => AttributeValue::Int((*v).into()),
        AnyValue::UInt32(v) => AttributeValue::Int((*v).into()),
        // // AnyValue::UInt64(v) => AttributeValue::Int((*v).into()),
        AnyValue::Int8(v) => AttributeValue::Int((*v).into()),
        AnyValue::Int16(v) => AttributeValue::Int((*v).into()),
        AnyValue::Int32(v) => AttributeValue::Int((*v).into()),
        AnyValue::Int64(v) => AttributeValue::Int((*v).into()),
        AnyValue::Float32(v) => AttributeValue::Float((*v).into()),
        AnyValue::Float64(v) => AttributeValue::Float((*v).into()),
        AnyValue::Datetime(ns, _, _) => {
            // Convert nanos to micros; tz is not used!
            let d: DateTime<Utc> = NaiveDateTime::from_timestamp_micros(ns / 1000)
                .unwrap()
                .and_utc();
            AttributeValue::Date(d)
        }
        AnyValue::Utf8Owned(v) => AttributeValue::String(v.to_string()),
        x => AttributeValue::String(format!("{:?}", x)),
    }
}

/**
Convert Polars DataFrame to PyBridgeEventLog
- Extracts attributes as Strings (converting other formats using debug format macro)
- Assumes valid EventLog structure of DataFrame (i.e., assuming that [PREFIXED_TRACE_ID_NAME] is present)
*/
fn convert_df_to_log(df: &DataFrame) -> Result<EventLog, PolarsError> {
    let groups = df.partition_by_stable([PREFIXED_TRACE_ID_NAME], true)?;
    let columns = df.get_column_names();
    let mut log = EventLog {
        attributes: Attributes::default(),
        traces: vec![],
        classifiers: None,
        extensions: None,
    };
    let traces: Vec<Trace> = groups
        .par_iter()
        .map(|g| {
            let mut trace_attributes: Attributes = Attributes::new();
            let events: Vec<Event> = (0..g.height())
                .into_iter()
                .map(|i| {
                    let mut event_attributes: Attributes = Attributes::new();
                    columns
                        .iter()
                        .zip(g.get_row(i).unwrap().0.iter())
                        .for_each(|(c, v)| {
                            if c.starts_with(TRACE_PREFIX) {
                                // e.g.,
                                let (_, c) = c.split_once(TRACE_PREFIX).unwrap();
                                trace_attributes.add_to_attributes(
                                    c.to_string(),
                                    any_value_to_attribute_value(v),
                                );
                            } else {
                                event_attributes.add_to_attributes(
                                    c.to_string(),
                                    any_value_to_attribute_value(v),
                                );
                            }
                        });

                    Event {
                        attributes: event_attributes,
                    }
                })
                .collect();
            return Trace {
                attributes: trace_attributes,
                events,
            };
        })
        .collect();
    log.traces = traces;
    return Ok(log);
}

///
/// Convert a attribute ([Attribute]) to an [AnyValue]
///
/// Used for converting values and data types to the DataFrame equivalent
///
/// The UTC timezone argument is used to correctly convert to AnyValue::Datetime with UTC timezone
///
fn attribute_to_any_value<'a>(
    from_option: Option<&Attribute>,
    utc_tz: &'a Option<String>,
) -> AnyValue<'a> {
    match from_option {
        Some(from) => {
            let x = attribute_value_to_any_value(&from.value, utc_tz);
            x
        }
        None => AnyValue::Null,
    }
}

///
/// Convert a attribute ([AttributeValue]) to an [AnyValue]
///
/// Used for converting values and data types to the DataFrame equivalent
///
/// The UTC timezone argument is used to correctly convert to AnyValue::Datetime with UTC timezone
///
fn attribute_value_to_any_value<'a>(
    from: &AttributeValue,
    utc_tz: &'a Option<String>,
) -> AnyValue<'a> {
    match from {
        AttributeValue::String(v) => AnyValue::Utf8Owned(v.into()),
        AttributeValue::Date(v) => {
            return AnyValue::Datetime(
                v.timestamp_nanos_opt().unwrap(),
                polars::prelude::TimeUnit::Nanoseconds,
                utc_tz,
            )
        }
        AttributeValue::Int(v) => AnyValue::Int64(*v),
        AttributeValue::Float(v) => AnyValue::Float64(*v),
        AttributeValue::Boolean(v) => AnyValue::Boolean(*v),
        AttributeValue::ID(v) => {
            let s = v.to_string();
            AnyValue::Utf8Owned(s.into())
        }
        // TODO: Add proper List/Container support
        AttributeValue::List(l) => AnyValue::Utf8Owned(format!("{:?}", l).into()),
        AttributeValue::Container(c) => AnyValue::Utf8Owned(format!("{:?}", c).into()),
        AttributeValue::None() => AnyValue::Null,
    }
}
///
/// Convert an [EventLog] to a Polars [DataFrame]
///
/// Flattens event log and adds trace-level attributes to events with prefixed attribute key (see [TRACE_PREFIX])
///
fn convert_log_to_df(log: &EventLog) -> Result<DataFrame, PolarsError> {
    println!("Starting converting log to DataFrame");
    let mut now = Instant::now();
    let all_attributes: HashSet<String> = log
        .traces
        .par_iter()
        .flat_map(|t| {
            let trace_attrs: HashSet<String> = t
                .attributes
                .keys()
                .map(|k| TRACE_PREFIX.to_string() + k.as_str())
                .collect();
            let m: HashSet<String> = t
                .events
                .iter()
                .flat_map(|e| {
                    e.attributes
                        .keys()
                        .map(|k| k.clone())
                        .collect::<Vec<String>>()
                })
                .collect();
            return [trace_attrs, m];
        })
        .flatten()
        .collect();
    println!("Gathering all attributes took {:.2?}", now.elapsed());
    let utc_tz = Some("UTC".to_string());
    now = Instant::now();
    let x: Vec<Series> = all_attributes
        .par_iter()
        .map(|k| {
            let mut entries: Vec<AnyValue> = log
                .traces
                .iter()
                .map(|t| -> Vec<AnyValue> {
                    if k.starts_with(TRACE_PREFIX) {
                        let trace_k: String = k.chars().skip(TRACE_PREFIX.len()).collect();
                        vec![
                            attribute_to_any_value(t.attributes.get(&trace_k), &utc_tz);
                            t.events.len()
                        ]
                    } else {
                        t.events
                            .iter()
                            .map(|e| attribute_to_any_value(e.attributes.get(k), &utc_tz))
                            .collect()
                    }
                })
                .flatten()
                .collect();

            let mut unique_dtypes: HashSet<DataType> = entries.iter().map(|v| v.dtype()).collect();
            unique_dtypes.remove(&DataType::Unknown);
            if unique_dtypes.len() > 1 {
                eprintln!(
                    "Warning: Attribute {} contains values of different dtypes ({:?})",
                    k, unique_dtypes
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
                            AnyValue::Utf8Owned(s) => AnyValue::Utf8Owned(s),
                            x => AnyValue::Utf8Owned(x.to_string().into()),
                        })
                        .collect();
                }
            }
            Series::new(k, &entries)
        })
        .collect();

    println!(
        "Creating a Series for every Attribute took {:.2?}",
        now.elapsed()
    );
    now = Instant::now();
    let df = DataFrame::new(x).unwrap();
    println!(
        "Constructing DF from Attribute Series took {:.2?}",
        now.elapsed()
    );
    return Ok(df);
}

///
/// Import a XES event log
///
/// Returns a tuple of a Polars [DataFrame] for the event data and a json-encoding of  all log attributes/extensions/classifiers
///
#[pyfunction]
fn import_xes(path: String) -> PyResult<(PyDataFrame, String)> {
    println!("Starting XES Import");
    let start_now = Instant::now();
    let mut now = Instant::now();
    let log = import_xes_file(&path);
    println!("Importing XES Log took {:.2?}", now.elapsed());
    now = Instant::now();
    // add_start_end_acts(&mut log);
    let converted_log = convert_log_to_df(&log).unwrap();
    println!("Finished Converting Log; Took {:.2?}", now.elapsed());
    #[derive(Debug, Serialize, Deserialize)]
    struct OtherLogData {
        pub attributes: Attributes,
        pub extensions: Option<Vec<EventLogExtension>>,
        pub classifiers: Option<Vec<EventLogClassifier>>,
    }
    let other_data = OtherLogData {
        attributes: log.attributes,
        extensions: log.extensions,
        classifiers: log.classifiers,
    };
    println!("Total duration: {:.2?}", start_now.elapsed());
    Ok((
        PyDataFrame(converted_log),
        serde_json::to_string(&other_data).unwrap(),
    ))
}

#[pyfunction]
fn test_df_pandas(df_serialized: String, format: String) -> PyResult<PyDataFrame> {
    let df = match format.as_str() {
        "json" => polars::prelude::JsonReader::new(Cursor::new(df_serialized))
            .finish()
            .or(Err(PyErr::new::<PyTypeError, _>(
                "Failed to parse JSON DataFrame.",
            ))),
        "csv" => polars::prelude::CsvReader::new(Cursor::new(df_serialized))
            .finish()
            .or(Err(PyErr::new::<PyTypeError, _>(
                "Failed to parse CSV DataFrame.",
            ))),
        _ => Err(PyErr::new::<PyTypeError, _>(
            "No valid DF format passed. Valid formats are 'json' and 'csv'.",
        )),
    }?;
    match convert_df_to_log(&df) {
        Ok(mut log) => {
            add_start_end_acts(&mut log);
            Ok(PyDataFrame(convert_log_to_df(&log).unwrap()))
        }
        Err(e) => Err(PyErr::new::<PyTypeError, _>(format!(
            "Could not convert to EventLog: {}",
            e.to_string()
        ))),
    }
}

#[pyfunction]
fn discover_net_alphappp(pydf: PyDataFrame, alphappp_config: String) -> PyResult<(String, String)> {
    let mut now = Instant::now();
    let df: DataFrame = pydf.into();
    let log = convert_df_to_log(&df).unwrap();
    let log_proj: EventLogActivityProjection = (&log).into();
    println!("Converting Log took {:.2?}", now.elapsed());
    now = Instant::now();
    let config: AlphaPPPConfig = AlphaPPPConfig::from_json(&alphappp_config);
    println!("Discovering net took {:.2?}", now.elapsed());
    let (net, dur) = alphappp_discover_petri_net(&log_proj, config);
    Ok((petrinet_to_json(&net), dur.to_json()))
}

#[pyfunction]
fn test_petrinet(net_json: String) -> PyResult<String> {
    let mut net: PetriNet = json_to_petrinet(&net_json);
    // add_sample_transition(&mut net);
    Ok(petrinet_to_json(&net))
}

#[pyfunction]
fn polars_df_to_log(pydf: PyDataFrame) -> PyResult<PyDataFrame> {
    let df: DataFrame = pydf.into();
    match convert_df_to_log(&df) {
        Ok(mut log) => {
            add_start_end_acts(&mut log);
            Ok(PyDataFrame(convert_log_to_df(&log).unwrap()))
        }
        Err(e) => Err(PyErr::new::<PyTypeError, _>(format!(
            "Could not convert to EventLog: {}",
            e.to_string()
        ))),
    }
}

#[pymodule]
fn rust_bridge_pm_py(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(polars_df_to_log, m)?)?;
    m.add_function(wrap_pyfunction!(test_df_pandas, m)?)?;
    m.add_function(wrap_pyfunction!(import_xes, m)?)?;
    m.add_function(wrap_pyfunction!(test_petrinet, m)?)?;
    m.add_function(wrap_pyfunction!(discover_net_alphappp, m)?)?;
    Ok(())
}
