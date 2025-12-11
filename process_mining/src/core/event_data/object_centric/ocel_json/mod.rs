//! OCEL 2.0 JSON Format Import/Export
use std::{
    fs::File,
    io::{BufReader, BufWriter},
    path::Path,
};

use crate::core::event_data::object_centric::ocel_struct::OCEL;

///
/// Serialize [`OCEL`] as a JSON [`String`]
///
/// [`serde_json`] can also be used to convert [`OCEL`] to other targets (e.g., `serde_json::to_writer`)
///
pub fn ocel_to_json(ocel: &OCEL) -> String {
    serde_json::to_string(ocel).unwrap()
}

///
/// Import [`OCEL`] from a JSON [`String`]
///
/// [`serde_json`] can also be used to import [`OCEL`] from other targets (e.g., `serde_json::from_reader`)
///
pub fn json_to_ocel(ocel_json: &str) -> OCEL {
    serde_json::from_str(ocel_json).unwrap()
}

///
/// Import [`OCEL`] from a JSON file given by a filepath
///
/// See also [`import_ocel_json_from_slice`].
///
pub fn import_ocel_json_from_path<P: AsRef<std::path::Path>>(
    path: P,
) -> Result<OCEL, std::io::Error> {
    let reader: BufReader<File> = BufReader::new(File::open(path)?);
    Ok(serde_json::from_reader(reader)?)
}

///
/// Import [`OCEL`] from a JSON byte slice
///
/// See also [`import_ocel_json_from_path`].
///
pub fn import_ocel_json_from_slice(slice: &[u8]) -> Result<OCEL, std::io::Error> {
    Ok(serde_json::from_slice(slice)?)
}

///
/// Export [`OCEL`] to a JSON file at the specified path
///
/// To import an OCEL .json file see [`import_ocel_json_from_path`] instead.
///
pub fn export_ocel_json_path<P: AsRef<Path>>(ocel: &OCEL, path: P) -> Result<(), std::io::Error> {
    let writer: BufWriter<File> = BufWriter::new(File::create(path)?);
    Ok(serde_json::to_writer(writer, ocel)?)
}

///
/// Export [`OCEL`] to JSON in a byte array ([`Vec<u8>`])
///
/// To import an OCEL .json file see [`import_ocel_json_from_path`] instead.
///
pub fn export_ocel_json_to_vec(ocel: &OCEL) -> Result<Vec<u8>, std::io::Error> {
    Ok(serde_json::to_vec(ocel)?)
}
