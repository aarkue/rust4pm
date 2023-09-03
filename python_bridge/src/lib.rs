use std::collections::HashMap;

use pyo3::prelude::*;
use pyo3::Python;
use pm_rust::{loop_sum_sqrt, Event};
use pythonize::pythonize;
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
        let obj: Py<PyAny> = pythonize(py,&ev).unwrap();
        obj
    })
}

#[pyfunction]
fn get_result_map(a: usize, b: usize) -> PyResult<HashMap<String,f32>>{
    let mut map : HashMap<String,f32> = HashMap::new();
    map.insert("Result".into(), loop_sum_sqrt(a,b));
    Ok(map)
}

#[pymodule]
fn rust_bridge_pm_py(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(get_event, m)?)?;
    m.add_function(wrap_pyfunction!(get_result_map, m)?)?;
    Ok(())
}