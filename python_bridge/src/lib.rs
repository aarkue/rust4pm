use std::collections::HashMap;

use pyo3::prelude::*;
use pyo3::Python;
use pm_rust::{loop_sum_sqrt, Event};
use pythonize::{depythonize, pythonize};
/// Formats the sum of two numbers as string.
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