use std::{io::Read, path::Path};

use crate::{
    core::{
        event_data::object_centric::ocel_json::import_ocel_json_slice,
        io::{Exportable, Importable},
    },
    test_utils::get_test_data_path,
    OCEL,
};

fn gunzip(bytes: &[u8]) -> Vec<u8> {
    let mut out = Vec::new();
    flate2::read::GzDecoder::new(bytes)
        .read_to_end(&mut out)
        .unwrap();
    out
}

fn load_sample_ocel() -> OCEL {
    let path = get_test_data_path()
        .join("ocel")
        .join("pm4py-ocel20_example.jsonocel");
    let bytes = std::fs::read(&path).unwrap();
    import_ocel_json_slice(&bytes).unwrap()
}

fn assert_gzip_magic(bytes: &[u8]) {
    assert!(
        bytes.len() >= 2 && bytes[0] == 0x1f && bytes[1] == 0x8b,
        "output does not start with gzip magic bytes"
    );
}

#[test]
fn test_ocel_json_gz_roundtrip() {
    let ocel = load_sample_ocel();
    let bytes = ocel.export_to_bytes("json.gz").unwrap();
    assert_gzip_magic(&bytes);
    let ocel2 = OCEL::import_from_bytes(&bytes, "json.gz").unwrap();
    assert_eq!(ocel, ocel2);
}

#[test]
fn test_ocel_xml_gz_roundtrip() {
    let ocel = load_sample_ocel();
    let plain_bytes = ocel.export_to_bytes("xml").unwrap();
    let gz_bytes = ocel.export_to_bytes("xml.gz").unwrap();
    assert_gzip_magic(&gz_bytes);
    assert_eq!(gunzip(&gz_bytes), plain_bytes);
    let gz_imported = OCEL::import_from_bytes(&gz_bytes, "xml.gz").unwrap();
    assert_eq!(ocel.events.len(), gz_imported.events.len());
    assert_eq!(ocel.objects.len(), gz_imported.objects.len());
}

#[test]
fn test_ocel_csv_gz_roundtrip() {
    let ocel = load_sample_ocel();
    let plain_bytes = ocel.export_to_bytes("ocel.csv").unwrap();
    let gz_bytes = ocel.export_to_bytes("ocel.csv.gz").unwrap();
    assert_gzip_magic(&gz_bytes);
    assert_eq!(gunzip(&gz_bytes), plain_bytes);
    let gz_imported = OCEL::import_from_bytes(&gz_bytes, "ocel.csv.gz").unwrap();
    assert_eq!(ocel.events.len(), gz_imported.events.len());
}

#[test]
fn test_ocel_gz_infer_format() {
    let cases = [
        ("log.json.gz", "json.gz"),
        ("log.jsonocel.gz", "jsonocel.gz"),
        ("log.xml.gz", "xml.gz"),
        ("log.xmlocel.gz", "xmlocel.gz"),
        ("log.ocel.csv.gz", "ocel.csv.gz"),
        ("log.csv.gz", "ocel.csv.gz"),
    ];
    for (path, expected) in cases {
        assert_eq!(
            <OCEL as Importable>::infer_format(Path::new(path)).as_deref(),
            Some(expected),
            "import infer_format({path})"
        );
        assert_eq!(
            <OCEL as Exportable>::infer_format(Path::new(path)).as_deref(),
            Some(expected),
            "export infer_format({path})"
        );
    }
}
