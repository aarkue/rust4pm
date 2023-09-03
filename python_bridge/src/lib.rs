use std::collections::HashMap;
use std::io::Cursor;
use std::time::Instant;

use pm_rust::add_start_end_acts;
use pm_rust::export_log_to_byte_vec;
use pm_rust::export_log_to_string;
use pm_rust::import_log_from_byte_array;
use pm_rust::import_log_from_str;
use pm_rust::EventLog;
use pm_rust::EventLogActivityProjection;
use pm_rust::Trace;
use pm_rust::TRACE_ID_NAME;
use pm_rust::{loop_sum_sqrt, Event};
use polars::prelude::AnyValue;
use polars::prelude::DataFrame;
use polars::prelude::PolarsError;
use polars::prelude::SerReader;
use pyo3::exceptions::PyTypeError;
use pyo3::prelude::*;
use pyo3::types::PyBytes;
use pyo3::Python;
use pyo3_polars::PyDataFrame;
use pythonize::depythonize;
use pythonize::pythonize;
use rayon::prelude::IntoParallelRefIterator;
use rayon::prelude::ParallelIterator;

#[pyclass]
#[derive(Debug, Default, Clone)]
pub struct PyBridgeAttributes {
    pub attributes: HashMap<String, String>,
}

#[pymethods]
impl PyBridgeAttributes {
    fn set(&mut self, key: String, value: String) -> PyResult<()> {
        self.attributes.insert(key, value);
        Ok(())
    }

    fn get(&self, key: String, default: Option<String>) -> PyResult<Option<String>> {
        match self.attributes.get(&key) {
            Some(value) => Ok(Some(value.clone())),
            None => Ok(default),
        }
    }
}

#[pyclass]
#[derive(Debug, Clone)]
pub struct PyBridgeEvent {
    #[pyo3(get)]
    pub attributes: PyBridgeAttributes,
}
impl PyBridgeEvent {
    pub fn new(attrs: HashMap<String, String>) -> Self {
        // let mut attr: PyBridgeAttributes = PyBridgeAttributes::default();
        // attr.attributes.insert(ACTIVITY_NAME.to_string(), activity);
        PyBridgeEvent {
            attributes: attrs.into(),
        }
    }
}

#[pymethods]
impl PyBridgeEvent {
    #[new]
    fn __new__(py: Python<'_>, attrs: HashMap<String, String>) -> PyResult<Self> {
        Ok(PyBridgeEvent::new(attrs))
    }
}

#[pyclass]
#[derive(Debug, Default, Clone)]
pub struct PyBridgeTrace {
    #[pyo3(get)]
    pub attributes: PyBridgeAttributes,
    #[pyo3(get, set)]
    pub events: Vec<PyBridgeEvent>,
}
#[pymethods]
impl PyBridgeTrace {
    #[new]
    fn new(trace_id: String) -> Self {
        let mut trace = Self::default();
        trace
            .attributes
            .set(TRACE_ID_NAME.into(), trace_id)
            .unwrap();
        return trace;
    }
    fn insert_event(&mut self, at_index: usize, event: PyBridgeEvent) {
        self.events.insert(at_index, event);
    }
    fn append_event(&mut self, event: PyBridgeEvent) {
        self.events.push(event);
    }
    fn remove_event_at(&mut self, at_index: usize) -> PyResult<()> {
        self.events.remove(at_index);
        Ok(())
    }
}

#[pyclass]
#[derive(Debug, Clone)]
pub struct PyBridgeEventLog {
    #[pyo3(get)]
    pub attributes: PyBridgeAttributes,
    #[pyo3(get)]
    pub traces: Vec<PyBridgeTrace>,
}

#[pymethods]
impl PyBridgeEventLog {
    #[new]
    fn new(py: Python<'_>) -> PyResult<Self> {
        let vec: Vec<PyBridgeTrace> = vec![];
        Ok(Self {
            attributes: PyBridgeAttributes::default(),
            traces: vec,
        })
    }
    fn insert_trace(&mut self, at_index: usize, trace: PyBridgeTrace) {
        self.traces.insert(at_index, trace);
    }
    fn append_trace(&mut self, trace: PyBridgeTrace) {
        self.traces.push(trace);
    }
    fn remove_trace_at(&mut self, at_index: usize) -> PyResult<()> {
        self.traces.remove(at_index);
        Ok(())
    }
}

impl Into<Event> for PyBridgeEvent {
    fn into(self) -> Event {
        Event {
            attributes: self.attributes.attributes,
        }
    }
}

impl Into<Trace> for PyBridgeTrace {
    fn into(self) -> Trace {
        Trace {
            attributes: self.attributes.attributes,
            events: self.events.into_iter().map(|e| e.into()).collect(),
        }
    }
}

impl Into<EventLog> for PyBridgeEventLog {
    fn into(self) -> EventLog {
        EventLog {
            attributes: self.attributes.attributes,
            traces: self.traces.into_iter().map(|t| t.into()).collect(),
        }
    }
}

impl From<HashMap<String, String>> for PyBridgeAttributes {
    fn from(value: HashMap<String, String>) -> Self {
        PyBridgeAttributes { attributes: value }
    }
}

impl From<Event> for PyBridgeEvent {
    fn from(value: Event) -> Self {
        PyBridgeEvent {
            attributes: value.attributes.into(),
        }
    }
}

impl From<Trace> for PyBridgeTrace {
    fn from(value: Trace) -> Self {
        PyBridgeTrace {
            attributes: value.attributes.into(),
            events: value.events.into_iter().map(|e| e.into()).collect(),
        }
    }
}

impl From<EventLog> for PyBridgeEventLog {
    fn from(value: EventLog) -> Self {
        PyBridgeEventLog {
            attributes: value.attributes.into(),
            traces: value.traces.into_iter().map(|t| t.into()).collect(),
        }
    }
}

#[pyfunction]
fn get_event(act: String) -> pyo3::Py<PyAny> {
    let ev = Event::new(act);
    Python::with_gil(|py| {
        let obj: Py<PyAny> = pythonize(py, &ev).unwrap();
        obj
    })
}

#[pyfunction]
fn test_bridge_log(pylog: PyBridgeEventLog) -> PyResult<PyBridgeEventLog> {
    let mut log: EventLog = pylog.into();
    add_start_end_acts(&mut log);
    let export_log: PyBridgeEventLog = log.into();
    println!("Added start/end acts!");
    Ok(export_log)
}

#[pyfunction]
fn test_df_pandas(json_df: String) -> PyResult<PyBridgeEventLog> {
    println!("Called test_df_pandas!");
    let df = polars::prelude::JsonReader::new(Cursor::new(json_df))
        .finish()
        .unwrap();
    match convert_df_to_log(&df) {
        Ok(log) => {
            let mut log: EventLog = log.into();
            add_start_end_acts(&mut log);
            Ok(log.into())
        }
        Err(e) => Err(PyErr::new::<PyTypeError, _>(format!(
            "Could not convert to EventLog: {}",
            e.to_string()
        ))),
    }
}

/**
Convert Polars DataFrame to PyBridgeEventLog
- Extracts attributes as Strings (converting other formats using debug format macro)
- Assumes valid EventLog structure of DataFrame (i.e., assuming that [TRACE_ID_NAME] is present)
*/
fn convert_df_to_log(df: &DataFrame) -> Result<PyBridgeEventLog, PolarsError> {
    let groups = df.partition_by_stable([TRACE_ID_NAME], true)?;
    let columns = df.get_column_names();
    let mut log = PyBridgeEventLog {
        attributes: PyBridgeAttributes::default(),
        traces: vec![],
    };

    let traces: Vec<PyBridgeTrace> = groups
        .par_iter()
        .map(|g| {
            let events: Vec<PyBridgeEvent> = (0..g.height())
                .into_iter()
                .map(|i| {
                    let attributes: HashMap<String, String> = match g.get_row(i) {
                        Ok(val) => columns
                            .iter()
                            .zip(val.0.iter())
                            .map(|(c, v)| {
                                return (
                                    c.to_string(),
                                    match v {
                                        AnyValue::Utf8(x) => x.to_string(),
                                        o => {
                                            format!("{:?}", o)
                                        }
                                    },
                                );
                            })
                            .collect(),
                        Err(_) => HashMap::default(),
                    };

                    PyBridgeEvent {
                        attributes: attributes.into(),
                    }
                })
                .collect();
            let trace_id = match events.get(0) {
                Some(ev) => ev
                    .attributes
                    .attributes
                    .get(TRACE_ID_NAME.into())
                    .unwrap_or(&"__NO_TRACE_ID__".to_string())
                    .clone(),
                None => "__NO_TRACE_ID__".to_string(),
            };
            let mut trace = PyBridgeTrace::new(trace_id);
            trace.events = events;
            return trace;
        })
        .collect();
    log.traces = traces;
    return Ok(log);
}

#[pyfunction]
fn polars_df_to_log(pydf: PyDataFrame) -> PyResult<PyBridgeEventLog> {
    let df: DataFrame = pydf.into();
    match convert_df_to_log(&df) {
        Ok(log) => Ok(log),
        Err(e) => Err(PyErr::new::<PyTypeError, _>(format!(
            "Could not convert to EventLog: {}",
            e.to_string()
        ))),
    }
}

#[pyfunction]
fn test_event_log(py: Python<'_>, log_py: Py<PyAny>) -> PyResult<Py<PyAny>> {
    let mut now = Instant::now();
    let mut log: EventLog = depythonize(log_py.as_ref(py)).unwrap();
    println!("Time until struct ready: {:.2?}", now.elapsed());
    now = Instant::now();
    log.attributes.insert(
        "name".to_string(),
        "Transformed Rust Log from byte[]".into(),
    );
    println!("Time until into EventLog: {:.2?}", now.elapsed());
    now = Instant::now();
    add_start_end_acts(&mut log);
    println!("Time until start/end added: {:.2?}", now.elapsed());
    now = Instant::now();
    let log_projection: EventLogActivityProjection<usize> = log.into();
    let log_again: EventLog = log_projection.into();
    println!("Time until into/from: {:.2?}", now.elapsed());
    now = Instant::now();
    let x: Py<PyAny> = pythonize(py, &log_again).unwrap();
    println!("Export to python: {:.2?}", now.elapsed());
    Ok(x)
}

#[pyfunction]
fn test_event_log_str(log: String) -> PyResult<String> {
    let mut log: EventLog = import_log_from_str(log);
    //  Python::with_gil(|py| {
    //     let mut log: EventLog = depythonize(log.as_ref(py)).unwrap();
    add_start_end_acts(&mut log);
    //     // let log_projection: EventLogActivityProjection<usize> = log.into();
    //     // println!("Projection with activities {:?}",log_projection.activities);
    //     // let log: EventLog = log_projection.into();
    //     let back: Py<PyAny> = pythonize(py,&log).unwrap();
    //     back
    Ok(export_log_to_string(&log))
    // })
}

#[pyfunction]
fn test_event_log_bytes(py: Python<'_>, log_bytes: Py<PyBytes>) -> PyResult<&PyBytes> {
    let mut now = Instant::now();
    let bytes: &[u8] = log_bytes.extract(py).unwrap();
    println!("Got {:?} bytes in {:.2?}", bytes.len(), now.elapsed());
    now = Instant::now();
    let mut log: EventLog = import_log_from_byte_array(&bytes);

    println!("Time until struct ready: {:.2?}", now.elapsed());
    now = Instant::now();
    log.attributes.insert(
        "name".to_string(),
        "Transformed Rust Log from byte[]".into(),
    );
    println!("Time until into EventLog: {:.2?}", now.elapsed());
    now = Instant::now();
    add_start_end_acts(&mut log);
    println!("Time until start/end added: {:.2?}", now.elapsed());
    now = Instant::now();
    let log_projection: EventLogActivityProjection<usize> = log.into();
    let log_again: EventLog = log_projection.into();
    println!("Time until into/from: {:.2?}", now.elapsed());
    now = Instant::now();
    let export_vec = export_log_to_byte_vec(&log_again);
    println!("ExportVec to byte array: {:.2?}", now.elapsed());
    let py_bytes = PyBytes::new(py, &export_vec);
    Ok(py_bytes)
}

#[pyfunction]
fn get_result_map(a: usize, b: usize) -> PyResult<HashMap<String, f32>> {
    let mut map: HashMap<String, f32> = HashMap::new();
    map.insert("Result".into(), loop_sum_sqrt(a, b));
    Ok(map)
}

#[pymodule]
fn rust_bridge_pm_py(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(get_event, m)?)?;
    m.add_function(wrap_pyfunction!(get_result_map, m)?)?;
    m.add_function(wrap_pyfunction!(test_event_log, m)?)?;
    m.add_function(wrap_pyfunction!(test_event_log_str, m)?)?;
    m.add_function(wrap_pyfunction!(test_event_log_bytes, m)?)?;
    m.add_function(wrap_pyfunction!(polars_df_to_log, m)?)?;
    m.add_function(wrap_pyfunction!(test_bridge_log, m)?)?;
    m.add_function(wrap_pyfunction!(test_df_pandas, m)?)?;
    m.add_class::<PyBridgeEvent>()?;
    m.add_class::<PyBridgeTrace>()?;
    m.add_class::<PyBridgeEventLog>()?;
    Ok(())
}
