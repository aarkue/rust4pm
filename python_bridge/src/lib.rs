
use polars::prelude::DataFrame;
use polars::prelude::SerReader;
use process_mining::alphappp::full::alphappp_discover_petri_net;
use process_mining::alphappp::full::AlphaPPPConfig;
use process_mining::convert_log_to_dataframe;
use process_mining::event_log::activity_projection::add_start_end_acts;
use process_mining::event_log::activity_projection::EventLogActivityProjection;
use process_mining::event_log::dataframe::convert_dataframe_to_log;
use process_mining::event_log::event_log_struct::EventLogClassifier;
use process_mining::event_log::event_log_struct::EventLogExtension;
use process_mining::event_log::import_xes::import_xes_file;
use process_mining::event_log::import_xes::XESImportOptions;
use process_mining::event_log::Attributes;
use process_mining::json_to_petrinet;
use process_mining::petri_net::petri_net_struct::PetriNet;
use process_mining::petrinet_to_json;
use pyo3::exceptions::PyTypeError;
use pyo3::prelude::*;
use pyo3_polars::PyDataFrame;
use serde::Deserialize;
use serde::Serialize;
use std::io::Cursor;
use std::time::Instant;

/// Import a XES event log
///
/// Returns a tuple of a Polars [`DataFrame`] for the event data and a json-encoding of  all log attributes/extensions/classifiers
///
#[pyfunction]
#[pyo3(signature = (path, options=None))]
fn import_xes_rs(path: String, options: Option<&str>) -> PyResult<(PyDataFrame, String)> {
    println!("Starting XES Import");
    let start_now = Instant::now();
    let mut now = Instant::now();
    let options = options
        .map(|options_json| serde_json::from_str::<XESImportOptions>(options_json).unwrap())
        .unwrap_or_default();
    let log = import_xes_file(&path, options).unwrap();
    println!("Importing XES Log took {:.2?}", now.elapsed());
    now = Instant::now();
    // add_start_end_acts(&mut log);
    let converted_log = convert_log_to_dataframe(&log,false).unwrap();
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
    match convert_dataframe_to_log(&df) {
        Ok(mut log) => {
            add_start_end_acts(&mut log);
            Ok(PyDataFrame(convert_log_to_dataframe(&log,true).unwrap()))
        }
        Err(e) => Err(PyErr::new::<PyTypeError, _>(format!(
            "Could not convert to EventLog: {}",
            e
        ))),
    }
}

#[pyfunction]
fn discover_net_alphappp(pydf: PyDataFrame, alphappp_config: String) -> PyResult<(String, String)> {
    let mut now = Instant::now();
    let df: DataFrame = pydf.into();
    let log = convert_dataframe_to_log(&df).unwrap();
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
    let net: PetriNet = json_to_petrinet(&net_json);
    Ok(petrinet_to_json(&net))
}

#[pyfunction]
fn polars_df_to_log(pydf: PyDataFrame) -> PyResult<PyDataFrame> {
    let df: DataFrame = pydf.into();
    match convert_dataframe_to_log(&df) {
        Ok(mut log) => {
            add_start_end_acts(&mut log);
            Ok(PyDataFrame(convert_log_to_dataframe(&log,false).unwrap()))
        }
        Err(e) => Err(PyErr::new::<PyTypeError, _>(format!(
            "Could not convert to EventLog: {}",
            e
        ))),
    }
}

#[pymodule]
fn rust_bridge_pm_py(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(polars_df_to_log, m)?)?;
    m.add_function(wrap_pyfunction!(test_df_pandas, m)?)?;
    m.add_function(wrap_pyfunction!(import_xes_rs, m)?)?;
    m.add_function(wrap_pyfunction!(test_petrinet, m)?)?;
    m.add_function(wrap_pyfunction!(discover_net_alphappp, m)?)?;
    Ok(())
}
