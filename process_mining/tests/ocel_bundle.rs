//! `.ocel.zip` as an ordinary OCEL format: read and written through `Importable`/`Exportable`,
//! with no connector, exactly as `.jsonocel` and `.ocel.csv` are.

#![cfg(feature = "ocel-bundle")]

use std::collections::BTreeMap;
use std::path::Path;

use process_mining::core::event_data::object_centric::ocel_bundle::{
    encode_type_name, import_ocel_bundle, import_ocel_bundle_from_bytes,
};
use process_mining::core::event_data::object_centric::ocel_json::import_ocel_json_path;
use process_mining::core::event_data::object_centric::{OCELAttributeValue, OCEL};
use process_mining::core::io::{Exportable, Importable};

fn running_example() -> OCEL {
    import_ocel_json_path(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/test_data/ocel/ocel2-p2p.json"
    ))
    .expect("read the running example")
}

/// Every attribute observation, as `id/name@time=value`, so a round trip is compared exactly
/// rather than by counts.
///
/// A `time`-typed attribute is rendered by its instant rather than by its stored text: this log
/// keeps some of them as unparsed strings (`2023-10-24 09:30:10.235452`, no timezone), and a
/// round trip through the container reads them back as real timestamps. That normalisation is
/// the reader honouring the declared type, not a difference in what the log says.
fn observations(ocel: &OCEL) -> Vec<String> {
    let time_typed: std::collections::HashSet<(&str, &str)> = ocel
        .object_types
        .iter()
        .flat_map(|t| {
            t.attributes
                .iter()
                .filter(|a| a.value_type == "time")
                .map(move |a| (t.name.as_str(), a.name.as_str()))
        })
        .collect();

    let mut out: Vec<String> = ocel
        .objects
        .iter()
        .flat_map(|o| {
            let time_typed = &time_typed;
            o.attributes.iter().map(move |a| {
                let value = if time_typed.contains(&(o.object_type.as_str(), a.name.as_str())) {
                    match &a.value {
                        OCELAttributeValue::Time(t) => t.to_rfc3339(),
                        OCELAttributeValue::String(s) => {
                            process_mining::core::event_data::timestamp_utils::parse_timestamp(
                                s, None, false,
                            )
                            .map_or_else(|_| s.clone(), |t| t.to_rfc3339())
                        }
                        other => other.to_string(),
                    }
                } else {
                    a.value.to_string()
                };
                format!("{}/{}@{}={}", o.id, a.name, a.time.to_rfc3339(), value)
            })
        })
        .collect();
    out.sort();
    out
}

fn relations(ocel: &OCEL) -> (Vec<String>, Vec<String>) {
    let mut e2o: Vec<String> = ocel
        .events
        .iter()
        .flat_map(|e| {
            e.relationships
                .iter()
                .map(move |r| format!("{} {} {}", e.id, r.object_id, r.qualifier))
        })
        .collect();
    let mut o2o: Vec<String> = ocel
        .objects
        .iter()
        .flat_map(|o| {
            o.relationships
                .iter()
                .map(move |r| format!("{} {} {}", o.id, r.object_id, r.qualifier))
        })
        .collect();
    e2o.sort();
    o2o.sort();
    (e2o, o2o)
}

/// The formats a caller sees, and what each produces: `{csv, parquet} x {archive, directory}`.
fn cases() -> Vec<(&'static str, &'static str)> {
    let mut out = vec![
        ("csv/archive", "log.ocel.zip"),
        ("csv/directory", "container"),
    ];
    if cfg!(feature = "ocel-bundle-parquet") {
        out.push(("parquet/archive", "log-parquet.zip"));
        out.push(("parquet/directory", "container-parquet"));
    }
    out
}

#[test]
fn a_bundle_round_trips_through_the_import_export_traits() {
    let source = running_example();
    assert!(!source.events.is_empty() && !source.objects.is_empty());

    for (what, name) in cases() {
        let dir = tempfile::tempdir().expect("tempdir");
        let target = dir.path().join(name);
        // A directory has no extension to infer from, so it has to exist before the format can
        // be read off it.
        if what.ends_with("directory") {
            std::fs::create_dir_all(&target).expect("mkdir");
        }

        // `export_to_path` picks the format from the path, the same call any other OCEL format
        // takes.
        source
            .export_to_path(&target)
            .unwrap_or_else(|e| panic!("{what}: export: {e}"));

        let back =
            OCEL::import_from_path(&target).unwrap_or_else(|e| panic!("{what}: import: {e}"));

        assert_eq!(back.events.len(), source.events.len(), "{what}: events");
        assert_eq!(back.objects.len(), source.objects.len(), "{what}: objects");
        assert_eq!(
            back.event_types.len(),
            source.event_types.len(),
            "{what}: event types"
        );
        assert_eq!(
            back.object_types.len(),
            source.object_types.len(),
            "{what}: object types"
        );

        let (e2o, o2o) = relations(&source);
        let (back_e2o, back_o2o) = relations(&back);
        assert_eq!(back_e2o, e2o, "{what}: e2o");
        assert_eq!(back_o2o, o2o, "{what}: o2o");

        let before = observations(&source);
        let after = observations(&back);
        if what.starts_with("parquet") {
            // Parquet has a real null, so absent and empty-string are different values and the
            // object/object-change split loses and invents nothing.
            assert_eq!(after, before, "{what}: attributes");
        } else {
            // An empty cell is a missing value, so an attribute whose value is the empty
            // string cannot survive. This log has 295 of them.
            let lost: Vec<&String> = before.iter().filter(|o| !after.contains(o)).collect();
            let invented: Vec<&String> = after.iter().filter(|o| !before.contains(o)).collect();
            assert!(invented.is_empty(), "{what}: invented {invented:?}");
            assert!(
                lost.iter().all(|o| o.ends_with('=')),
                "{what}: CSV may only lose empty-string values, lost {:?}",
                lost.iter()
                    .filter(|o| !o.ends_with('='))
                    .collect::<Vec<_>>()
            );
            assert!(!lost.is_empty(), "{what}: this log has empty-string values");
        }
    }
}

/// Every advertised export format must actually export through `export_to_bytes`, not only
/// through `export_to_path`. A host offering a download rather than a file dialog only ever calls
/// the byte path, and a format listed but not handled there fails the moment a user clicks
/// Export.
#[test]
fn every_advertised_export_format_works_through_the_byte_path() {
    let source = running_example();
    for format in <OCEL as Exportable>::known_export_formats() {
        let ext = format.extension;
        // These two are genuinely path-only: both write a database file the driver opens by name.
        if ext.ends_with("sqlite") || ext.ends_with("duckdb") {
            continue;
        }
        let bytes = source
            .export_to_bytes(&ext)
            .unwrap_or_else(|e| panic!("{ext}: {e}"));
        assert!(!bytes.is_empty(), "{ext}: produced no bytes");
    }
}

/// Every column chunk a Parquet container holds is compressed.
///
/// `parquet`'s writers default to `Compression::UNCOMPRESSED`, and this exporter deliberately
/// stores its Parquet entries in the ZIP rather than deflating them, so that they stay
/// seekable. With the default that combination compressed nothing anywhere, and a real 406k-event
/// log exported to 117 MB of Parquet against 37 MB of CSV.
///
/// The codec is asserted rather than the file size: which storage comes out smaller depends on
/// the log. Parquet's per-file footer and per-column dictionary are fixed overhead, so a log with
/// many small tables (this fixture) can be larger as Parquet even fully compressed, while a log
/// with large tables is substantially smaller.
#[cfg(feature = "ocel-bundle-parquet")]
#[test]
fn every_parquet_column_chunk_is_compressed() {
    use parquet::file::reader::{FileReader, SerializedFileReader};

    let bytes = running_example()
        .export_to_bytes("ocel-parquet.zip")
        .expect("export");
    let mut archive = zip::ZipArchive::new(std::io::Cursor::new(bytes)).expect("archive");

    let mut checked = 0;
    for i in 0..archive.len() {
        let mut entry = archive.by_index(i).expect("entry");
        if !entry.name().ends_with(".parquet") {
            continue;
        }
        let mut buf = Vec::new();
        std::io::Read::read_to_end(&mut entry, &mut buf).expect("read");
        let reader = SerializedFileReader::new(bytes::Bytes::from(buf)).expect("parquet");
        for group in reader.metadata().row_groups() {
            for column in group.columns() {
                assert_ne!(
                    column.compression(),
                    parquet::basic::Compression::UNCOMPRESSED,
                    "{}: {} is uncompressed",
                    entry.name(),
                    column.column_path()
                );
                checked += 1;
            }
        }
    }
    assert!(checked > 0, "the archive had column chunks to check");
}

/// A directory container is opened by picking its manifest. A file dialog cannot select a
/// directory on every platform, so `ocel-meta.json` is the handle a person actually has.
#[test]
fn picking_the_manifest_opens_the_directory_it_sits_in() {
    let source = running_example();
    let dir = tempfile::tempdir().expect("tempdir");
    let container = dir.path().join("container");
    std::fs::create_dir_all(&container).expect("mkdir");
    source.export_to_path(&container).expect("export");

    let manifest = container.join("ocel-meta.json");
    assert!(manifest.is_file(), "the export wrote a manifest");

    // Through the trait, as a host importing a picked file does. It must not be read as the
    // `.json` its name ends with.
    let back = OCEL::import_from_path(&manifest).expect("import via manifest");
    assert_eq!(back.events.len(), source.events.len());
    assert_eq!(back.objects.len(), source.objects.len());
}

/// Both names an export can produce are names an import accepts. The reader ignores which
/// storage a container uses, since the manifest says, but the filenames still have to
/// round-trip.
#[test]
fn every_name_export_writes_is_a_name_import_accepts() {
    let import: Vec<String> = <OCEL as Importable>::known_import_formats()
        .into_iter()
        .map(|e| e.extension)
        .collect();
    for exported in <OCEL as Exportable>::known_export_formats() {
        if !exported.extension.ends_with("zip") {
            continue;
        }
        assert!(
            import.contains(&exported.extension),
            "export writes .{} but import does not accept it: {import:?}",
            exported.extension
        );
    }
}

/// The route a build with no filesystem takes: bytes in, bytes out, no path anywhere, through
/// both entry points a caller has.
///
/// A directory container needs a filesystem and an archive on disk is expanded into a temp
/// directory, so on `wasm32` this is the only bundle path that works.
#[test]
fn a_container_round_trips_through_bytes_alone() {
    type ImportFromBytes = fn(&[u8], &str) -> OCEL;
    let routes: [(&str, ImportFromBytes); 2] = [
        ("the format registry", |bytes, format| {
            OCEL::import_from_bytes(bytes, format).expect("import")
        }),
        ("the bundle reader", |bytes, _| {
            import_ocel_bundle_from_bytes(bytes).expect("import")
        }),
    ];

    let source = running_example();
    let mut formats = vec!["ocel.zip"];
    if cfg!(feature = "ocel-bundle-parquet") {
        formats.push("ocel-parquet.zip");
    }
    for format in formats {
        let bytes = source
            .export_to_bytes(format)
            .unwrap_or_else(|e| panic!("{format}: export: {e}"));
        for (what, import) in routes {
            let back = import(&bytes, format);
            assert_eq!(
                back.events.len(),
                source.events.len(),
                "{format} via {what}: events"
            );
            assert_eq!(
                back.objects.len(),
                source.objects.len(),
                "{format} via {what}: objects"
            );
        }
    }
}

/// A `.zip` is offered wherever the other OCEL formats are, so a file picker and an OS file
/// association both list it.
#[test]
fn the_bundled_format_is_advertised_alongside_the_others() {
    let import: Vec<String> = <OCEL as Importable>::known_import_formats()
        .into_iter()
        .map(|e| e.extension)
        .collect();
    assert!(import.contains(&"ocel.zip".to_string()), "{import:?}");
    assert!(
        import.contains(&"json".to_string()),
        "the others are still there"
    );

    let export: Vec<String> = <OCEL as Exportable>::known_export_formats()
        .into_iter()
        .map(|e| e.extension)
        .collect();
    assert!(export.contains(&"ocel.zip".to_string()), "{export:?}");
    #[cfg(feature = "ocel-bundle-parquet")]
    assert!(
        export.contains(&"ocel-parquet.zip".to_string()),
        "Parquet storage needs a format string of its own: {export:?}"
    );
}

/// An empty CSV cell is a missing value, which is what the format says. Pinned here because the
/// blueprint-based route over the same layout cannot tell the two apart cheaply and does not.
#[test]
fn an_empty_csv_cell_is_a_missing_value_not_an_empty_string() {
    use process_mining::core::event_data::object_centric::OCELAttributeType;
    use process_mining::core::event_data::object_centric::{
        OCELObject, OCELObjectAttribute, OCELType, OCELTypeAttribute,
    };

    let ocel = OCEL {
        event_types: Vec::new(),
        object_types: vec![OCELType {
            name: "order".to_string(),
            attributes: vec![
                OCELTypeAttribute::new("note", &OCELAttributeType::String),
                OCELTypeAttribute::new("count", &OCELAttributeType::Integer),
            ],
        }],
        events: Vec::new(),
        // Declares two attributes, carries one.
        objects: vec![OCELObject {
            id: "o1".to_string(),
            object_type: "order".to_string(),
            attributes: vec![OCELObjectAttribute {
                name: "count".to_string(),
                value: OCELAttributeValue::Integer(3),
                time: chrono::DateTime::from_timestamp_nanos(0).into(),
            }],
            relationships: Vec::new(),
        }],
    };

    let dir = tempfile::tempdir().expect("tempdir");
    let target = dir.path().join("log.ocel.zip");
    ocel.export_to_path(&target).expect("export");
    let back = OCEL::import_from_path(&target).expect("import");

    let names: Vec<&str> = back.objects[0]
        .attributes
        .iter()
        .map(|a| a.name.as_str())
        .collect();
    assert_eq!(names, ["count"], "the absent attribute stays absent");
    assert_eq!(
        back.objects[0].attributes[0].value,
        OCELAttributeValue::Integer(3)
    );
}

// The running-example fixture is written out here rather than committed, so the test states the
// format's layout in one place and a change to the naming rules has to be made deliberately.

/// `(type name, attribute columns, rows)` for the running example's event tables.
const EVENTS: &[(&str, &str, &[&str])] = &[
    (
        "Create Purchase Requisition",
        "pr_creator",
        &["e1,2022-01-09T15:00:00+00:00,Mike"],
    ),
    (
        "Approve Purchase Requisition",
        "pr_approver",
        &["e2,2022-01-09T16:30:00+00:00,Tania"],
    ),
    (
        "Create Purchase Order",
        "po_creator",
        &[
            "e3,2022-01-10T09:15:00+00:00,Mike",
            "e10,2022-02-02T17:00:00+00:00,Mario",
        ],
    ),
    (
        "Change PO Quantity",
        "po_editor",
        &["e4,2022-01-13T12:00:00+00:00,Mike"],
    ),
    (
        "Insert Invoice",
        "invoice_inserter",
        &[
            "e5,2022-01-14T12:00:00+00:00,Luke",
            "e6,2022-01-16T11:00:00+00:00,Luke",
            "e9,2022-02-02T09:00:00+00:00,Mario",
        ],
    ),
    (
        "Insert Payment",
        "payment_inserter",
        &[
            "e7,2022-01-30T23:00:00+00:00,Robot",
            "e8,2022-01-31T22:00:00+00:00,Robot",
            "e13,2022-02-28T23:00:00+00:00,Robot",
        ],
    ),
    (
        "Set Payment Block",
        "invoice_blocker",
        &["e11,2022-02-03T07:30:00+00:00,Sam"],
    ),
    (
        "Remove Payment Block",
        "invoice_block_rem",
        &["e12,2022-02-03T23:30:00+00:00,Mario"],
    ),
];

/// An object type's fixture: its name, its attribute columns with their declared types, its
/// object rows, and its change rows.
type ObjectFixture = (
    &'static str,
    &'static [(&'static str, &'static str)],
    &'static [&'static str],
    &'static [&'static str],
);

const OBJECTS: &[ObjectFixture] = &[
    (
        "Purchase Requisition",
        &[("pr_product", "string"), ("pr_quantity", "integer")],
        &["PR1,Cows,500"],
        &[],
    ),
    (
        "Purchase Order",
        &[("po_product", "string"), ("po_quantity", "integer")],
        &["PO1,Cows,500", "PO2,Notebooks,1"],
        &["PO1,2022-01-13T12:00:00+00:00,po_quantity,,600"],
    ),
    (
        "Invoice",
        &[("is_blocked", "string")],
        &["R1,No", "R2,No", "R3,No"],
        &[
            "R3,2022-02-03T07:30:00+00:00,is_blocked,Yes",
            "R3,2022-02-03T23:30:00+00:00,is_blocked,No",
        ],
    ),
    ("Payment", &[], &["P1", "P2", "P3"], &[]),
];

const E2O: &str = "\
e1,PR1,Regular placement of PR
e2,PR1,Regular approval of PR
e3,PR1,Created order from PR
e3,PO1,Created order with identifier
e4,PO1,Change of quantity
e5,PO1,Invoice created starting from the PO
e5,R1,Invoice created with identifier
e6,PO1,Invoice created starting from the PO
e6,R2,Invoice created with identifier
e7,R1,Payment for the invoice
e7,P1,Payment inserted with identifier
e8,R2,Payment for the invoice
e8,P2,Payment inserted with identifier
e9,R3,Invoice created with identifier
e10,R3,Purchase order created with maverick buying from
e10,PO2,Purchase order created with identifier
e11,R3,Payment block due to unethical maverick buying
e12,R3,Payment block removed
e13,R3,Payment for the invoice
e13,P3,Payment inserted with identifier
";

const O2O: &str = "\
PR1,PO1,PO from PR
PO1,R1,Invoice from PO
PO1,R2,Invoice from PO
R1,P1,Payment from invoice
R2,P2,Payment from invoice
PO2,R3,Maverick buying
R3,P3,Payment from invoice
";

/// Write the running example as a CSV container under `root`, exactly as the format lays it out.
fn write_container(root: &Path) {
    let mut files: BTreeMap<String, String> = BTreeMap::new();
    let mut event_types = Vec::new();
    let mut object_types = Vec::new();

    for (ty, attr, rows) in EVENTS {
        let file = format!("events/event_{}.csv", encode_type_name(ty));
        files.insert(
            file.clone(),
            format!("ocel_id,ocel_time,{attr}\n{}\n", rows.join("\n")),
        );
        event_types.push(format!(
            r#""{ty}": {{ "file": "{file}", "attributes": [{{ "name": "{attr}", "type": "string" }}] }}"#
        ));
    }

    for (ty, attrs, rows, changes) in OBJECTS {
        let enc = encode_type_name(ty);
        let file = format!("objects/object_{enc}.csv");
        let changes_file = format!("object_changes/object_changes_{enc}.csv");
        let names: Vec<&str> = attrs.iter().map(|(n, _)| *n).collect();

        let mut header = "ocel_id".to_string();
        for n in &names {
            header.push(',');
            header.push_str(n);
        }
        files.insert(file.clone(), format!("{header}\n{}\n", rows.join("\n")));

        let mut change_header = "ocel_id,ocel_time,ocel_changed_field".to_string();
        for n in &names {
            change_header.push(',');
            change_header.push_str(n);
        }
        // An object type with no changes still gets its table: header only, no rows.
        files.insert(
            changes_file.clone(),
            if changes.is_empty() {
                format!("{change_header}\n")
            } else {
                format!("{change_header}\n{}\n", changes.join("\n"))
            },
        );

        let decls: Vec<String> = attrs
            .iter()
            .map(|(n, t)| format!(r#"{{ "name": "{n}", "type": "{t}" }}"#))
            .collect();
        object_types.push(format!(
            r#""{ty}": {{ "file": "{file}", "changesFile": "{changes_file}", "attributes": [{}] }}"#,
            decls.join(", ")
        ));
    }

    files.insert(
        "relations/e2o.csv".to_string(),
        format!("ocel_event_id,ocel_object_id,ocel_qualifier\n{E2O}"),
    );
    files.insert(
        "relations/o2o.csv".to_string(),
        format!("ocel_source_id,ocel_target_id,ocel_qualifier\n{O2O}"),
    );
    files.insert(
        "ocel-meta.json".to_string(),
        format!(
            r#"{{
  "ocelVersion": "2.0",
  "bundleFormatVersion": "1.0",
  "storageFormat": "csv",
  "eventTypes": {{ {} }},
  "objectTypes": {{ {} }},
  "relations": {{ "e2o": "relations/e2o.csv", "o2o": "relations/o2o.csv" }}
}}"#,
            event_types.join(",\n"),
            object_types.join(",\n")
        ),
    );

    for (rel, contents) in files {
        let path = root.join(&rel);
        std::fs::create_dir_all(path.parent().expect("has a parent")).expect("mkdir");
        std::fs::write(&path, contents).expect("write");
    }
}

/// Zip up `root`, with every entry written using `method`.
fn zip_dir(root: &Path, method: zip::CompressionMethod) -> Vec<u8> {
    let mut buf = Vec::new();
    {
        let mut w = zip::ZipWriter::new(std::io::Cursor::new(&mut buf));
        let opts: zip::write::FileOptions<'_, ()> =
            zip::write::FileOptions::default().compression_method(method);
        let mut stack = vec![root.to_path_buf()];
        while let Some(dir) = stack.pop() {
            for entry in std::fs::read_dir(&dir).expect("read_dir") {
                let path = entry.expect("entry").path();
                if path.is_dir() {
                    stack.push(path);
                    continue;
                }
                let rel = path.strip_prefix(root).expect("under root");
                w.start_file(rel.to_string_lossy().replace('\\', "/"), opts)
                    .expect("start_file");
                std::io::Write::write_all(&mut w, &std::fs::read(&path).expect("read"))
                    .expect("write entry");
            }
        }
        w.finish().expect("finish");
    }
    buf
}

/// Every observation of `attr` on `object_id`, oldest first, as `time=value`.
fn attr_history(ocel: &OCEL, object_id: &str, attr: &str) -> Vec<String> {
    let ob = ocel
        .objects
        .iter()
        .find(|o| o.id == object_id)
        .unwrap_or_else(|| panic!("no object '{object_id}'"));
    let mut vals: Vec<(&chrono::DateTime<chrono::FixedOffset>, &OCELAttributeValue)> = ob
        .attributes
        .iter()
        .filter(|a| a.name == attr)
        .map(|a| (&a.time, &a.value))
        .collect();
    vals.sort_by_key(|(t, _)| **t);
    vals.iter()
        .map(|(t, v)| format!("{}={}", t.to_rfc3339(), v))
        .collect()
}

/// The whole of the running example, asserted on the exact object and change-table semantics
/// rather than on counts alone.
fn assert_running_example(ocel: &OCEL) {
    assert_eq!(ocel.events.len(), 13, "e1..e13");
    assert_eq!(ocel.objects.len(), 9, "PR1, PO1-2, R1-3, P1-3");
    assert_eq!(
        ocel.events
            .iter()
            .map(|e| e.relationships.len())
            .sum::<usize>(),
        20,
        "every e2o row"
    );
    assert_eq!(
        ocel.objects
            .iter()
            .map(|o| o.relationships.len())
            .sum::<usize>(),
        7,
        "every o2o row"
    );

    // An object table's values are initial ones, held from the epoch; a change row adds another
    // value at its own instant. The unchanged attribute keeps exactly one value.
    assert_eq!(
        attr_history(ocel, "PO1", "po_quantity"),
        [
            "1970-01-01T00:00:00+00:00=500",
            "2022-01-13T12:00:00+00:00=600"
        ]
    );
    assert_eq!(
        attr_history(ocel, "PO1", "po_product"),
        ["1970-01-01T00:00:00+00:00=Cows"],
        "a change to po_quantity must not record anything for po_product"
    );
    assert_eq!(
        attr_history(ocel, "R3", "is_blocked"),
        [
            "1970-01-01T00:00:00+00:00=No",
            "2022-02-03T07:30:00+00:00=Yes",
            "2022-02-03T23:30:00+00:00=No"
        ]
    );
}

/// The storage formats this build can write. Reading Parquet needs the same feature.
fn storages() -> Vec<process_mining::core::event_data::object_centric::ocel_bundle::StorageFormat> {
    use process_mining::core::event_data::object_centric::ocel_bundle::StorageFormat;
    let mut out = vec![StorageFormat::Csv];
    if cfg!(feature = "ocel-bundle-parquet") {
        out.push(StorageFormat::Parquet);
    }
    out
}

#[test]
fn a_directory_container_imports_the_running_example() {
    let dir = tempfile::tempdir().expect("tempdir");
    write_container(dir.path());

    let ocel = import_ocel_bundle(dir.path()).expect("import");
    assert_running_example(&ocel);
}

/// Every combination the format allows: {directory, archive} x {CSV, Parquet}, and for an
/// archive both compression methods, since the format names neither and a container this crate
/// did not write may use either.
///
/// The Parquet-in-a-deflated-archive cell is the one no other test reaches: this crate's own
/// exporter stores Parquet entries uncompressed so a reader can seek them, so only a
/// third-party writer produces it.
#[test]
fn every_layout_and_storage_combination_imports_to_the_same_log() {
    use process_mining::core::event_data::object_centric::ocel_bundle::{
        export_ocel_bundle, BundleExportOptions, ContainerLayout,
    };

    // The running example, read from the hand-written CSV fixture, is what every combination
    // below is re-encoded from, so a difference is the encoding's, not the data's.
    let fixture = tempfile::tempdir().expect("tempdir");
    write_container(fixture.path());
    let source = import_ocel_bundle(fixture.path()).expect("read the fixture");
    assert_running_example(&source);

    for storage in storages() {
        let work = tempfile::tempdir().expect("tempdir");
        let as_dir = work.path().join("container");
        std::fs::create_dir_all(&as_dir).expect("mkdir");
        export_ocel_bundle(
            &source,
            &as_dir,
            BundleExportOptions {
                layout: ContainerLayout::Directory,
                storage,
            },
        )
        .unwrap_or_else(|e| panic!("{storage:?}: export: {e}"));

        let from_dir = import_ocel_bundle(&as_dir)
            .unwrap_or_else(|e| panic!("{storage:?}/directory: import: {e}"));
        assert_running_example(&from_dir);

        for method in [
            zip::CompressionMethod::Stored,
            zip::CompressionMethod::Deflated,
        ] {
            let what = format!("{storage:?}/archive/{method:?}");
            let bytes = zip_dir(&as_dir, method);
            let archive = work.path().join("log.ocel.zip");
            std::fs::write(&archive, &bytes).expect("write archive");

            let from_archive =
                import_ocel_bundle(&archive).unwrap_or_else(|e| panic!("{what}: import: {e}"));
            assert_running_example(&from_archive);

            // The same archive with no path to expand beside it.
            let from_memory = import_ocel_bundle_from_bytes(&bytes)
                .unwrap_or_else(|e| panic!("{what}: import from bytes: {e}"));
            assert_running_example(&from_memory);

            std::fs::remove_file(&archive).expect("clean up");
        }
    }
}

/// The archive this crate writes stores Parquet entries uncompressed, so a reader can seek
/// within them without expanding the archive, and deflates CSV, which is read start to finish
/// anyway. The format itself names neither method, so both are a writer's choice.
#[test]
fn a_written_archive_stores_parquet_entries_and_deflates_csv_ones() {
    use process_mining::core::event_data::object_centric::ocel_bundle::{
        export_ocel_bundle, BundleExportOptions, ContainerLayout, StorageFormat,
    };

    let fixture = tempfile::tempdir().expect("tempdir");
    write_container(fixture.path());
    let source = import_ocel_bundle(fixture.path()).expect("read the fixture");

    for storage in storages() {
        let want = match storage {
            StorageFormat::Csv => zip::CompressionMethod::Deflated,
            StorageFormat::Parquet => zip::CompressionMethod::Stored,
        };
        let dir = tempfile::tempdir().expect("tempdir");
        let archive = dir.path().join("log.ocel.zip");
        export_ocel_bundle(
            &source,
            &archive,
            BundleExportOptions {
                layout: ContainerLayout::Archive,
                storage,
            },
        )
        .expect("export");

        let file = std::fs::File::open(&archive).expect("open");
        let mut zip = zip::ZipArchive::new(file).expect("read archive");
        let mut tables = 0;
        for i in 0..zip.len() {
            let entry = zip.by_index(i).expect("entry");
            let name = entry.name().to_string();
            if name == "ocel-meta.json" {
                continue;
            }
            tables += 1;
            assert_eq!(entry.compression(), want, "{storage:?}: {name}");
        }
        assert!(tables > 0, "{storage:?}: the archive has tables");
    }
}

/// A manifest is untrusted input, and its declared paths are joined onto the container root.
#[test]
fn a_manifest_naming_a_path_outside_the_container_reads_nothing() {
    let dir = tempfile::tempdir().expect("tempdir");
    let root = dir.path();
    std::fs::write(root.join("secret.csv"), "ocel_id\nleaked\n").expect("write");
    std::fs::create_dir_all(root.join("container")).expect("mkdir");
    std::fs::write(
        root.join("container/ocel-meta.json"),
        r#"{
          "ocelVersion": "2.0",
          "bundleFormatVersion": "1.0",
          "storageFormat": "csv",
          "eventTypes": {},
          "objectTypes": {
            "Escaped": { "file": "../secret.csv", "attributes": [] }
          },
          "relations": { "e2o": "relations/e2o.csv", "o2o": "relations/o2o.csv" }
        }"#,
    )
    .expect("write manifest");

    let err = import_ocel_bundle(root.join("container")).expect_err("must not escape");
    let message = err.to_string();
    assert!(message.contains("../secret.csv"), "{message}");
    assert!(
        message.contains("must stay inside the container"),
        "{message}"
    );
}

/// A directory with no manifest is not a container, and the error says which file was looked for.
#[test]
fn a_path_that_is_not_a_container_is_rejected_with_a_message_naming_it() {
    let dir = tempfile::tempdir().expect("tempdir");
    let err = import_ocel_bundle(dir.path()).expect_err("no manifest");
    assert!(err.to_string().contains("ocel-meta.json"), "{err}");
}

/// One column of a Parquet fixture table, for a container this crate did not write.
#[cfg(feature = "ocel-bundle-parquet")]
enum Col<'a> {
    /// `BYTE_ARRAY` with logical type `STRING`. An empty string is written as null, which is
    /// how the format spells a missing attribute value in Parquet storage.
    Text(&'a [&'a str]),
    /// `INT64`.
    Int(&'a [i64]),
    /// `INT64` with logical type `TIMESTAMP(MICROS, isAdjustedToUTC=true)`, as the format
    /// requires: an exporter must not write timestamps as strings in Parquet storage.
    Time(&'a [&'a str]),
}

/// Write `columns` as a Parquet file. Fixed columns are `required`, attribute columns
/// `optional`, per the format's Parquet mapping.
#[cfg(feature = "ocel-bundle-parquet")]
fn write_parquet(path: &Path, columns: &[(&str, bool, Col<'_>)]) {
    use parquet::data_type::{ByteArray, ByteArrayType, Int64Type};
    use parquet::file::properties::WriterProperties;
    use parquet::file::writer::SerializedFileWriter;
    use parquet::schema::parser::parse_message_type;
    use std::sync::Arc;

    let fields: Vec<String> = columns
        .iter()
        .map(|(name, required, col)| {
            let rep = if *required { "REQUIRED" } else { "OPTIONAL" };
            match col {
                Col::Text(_) => format!("{rep} BYTE_ARRAY {name} (STRING);"),
                Col::Int(_) => format!("{rep} INT64 {name};"),
                Col::Time(_) => format!("{rep} INT64 {name} (TIMESTAMP(MICROS,true));"),
            }
        })
        .collect();
    let schema = Arc::new(
        parse_message_type(&format!("message row {{ {} }}", fields.join(" "))).expect("schema"),
    );

    std::fs::create_dir_all(path.parent().expect("has a parent")).expect("mkdir");
    let file = std::fs::File::create(path).expect("create");
    let mut writer =
        SerializedFileWriter::new(file, schema, Arc::new(WriterProperties::new())).expect("writer");
    let mut group = writer.next_row_group().expect("row group");
    for (_, required, col) in columns {
        let mut w = group.next_column().expect("column").expect("some column");
        // An optional column needs definition levels: 1 where a value is present, 0 for null.
        match col {
            Col::Text(values) => {
                let present: Vec<ByteArray> = values
                    .iter()
                    .filter(|v| !v.is_empty())
                    .map(|v| ByteArray::from(*v))
                    .collect();
                let defs: Vec<i16> = values.iter().map(|v| i16::from(!v.is_empty())).collect();
                w.typed::<ByteArrayType>()
                    .write_batch(&present, (!required).then_some(&defs), None)
                    .expect("write");
            }
            Col::Int(values) => {
                let defs: Vec<i16> = values.iter().map(|_| 1).collect();
                w.typed::<Int64Type>()
                    .write_batch(values, (!required).then_some(&defs), None)
                    .expect("write");
            }
            Col::Time(values) => {
                let micros: Vec<i64> = values
                    .iter()
                    .map(|v| {
                        chrono::DateTime::parse_from_rfc3339(v)
                            .expect("rfc3339")
                            .timestamp_micros()
                    })
                    .collect();
                let defs: Vec<i16> = micros.iter().map(|_| 1).collect();
                w.typed::<Int64Type>()
                    .write_batch(&micros, (!required).then_some(&defs), None)
                    .expect("write");
            }
        }
        w.close().expect("close column");
    }
    group.close().expect("close group");
    writer.close().expect("close writer");
}

/// The other storage format. Its point is that attribute types come from the file's own schema
/// rather than from the manifest, so a timestamp arrives as an instant and an integer as an
/// integer, which is also what makes a Parquet container compilable to SQL views.
#[cfg(feature = "ocel-bundle-parquet")]
#[test]
fn a_parquet_container_imports_with_types_taken_from_the_files() {
    let dir = tempfile::tempdir().expect("tempdir");
    let root = dir.path();

    write_parquet(
        &root.join("events/event_Change%20PO%20Quantity.parquet"),
        &[
            ("ocel_id", true, Col::Text(&["e4"])),
            ("ocel_time", true, Col::Time(&["2022-01-13T12:00:00+00:00"])),
            ("po_editor", false, Col::Text(&["Mike"])),
        ],
    );
    write_parquet(
        &root.join("objects/object_Purchase%20Order.parquet"),
        &[
            ("ocel_id", true, Col::Text(&["PO1"])),
            ("po_product", false, Col::Text(&["Cows"])),
            ("po_quantity", false, Col::Int(&[500])),
        ],
    );
    write_parquet(
        &root.join("object_changes/object_changes_Purchase%20Order.parquet"),
        &[
            ("ocel_id", true, Col::Text(&["PO1"])),
            ("ocel_time", true, Col::Time(&["2022-01-13T12:00:00+00:00"])),
            ("ocel_changed_field", true, Col::Text(&["po_quantity"])),
            ("po_product", false, Col::Text(&[""])),
            ("po_quantity", false, Col::Int(&[600])),
        ],
    );
    write_parquet(
        &root.join("relations/e2o.parquet"),
        &[
            ("ocel_event_id", true, Col::Text(&["e4"])),
            ("ocel_object_id", true, Col::Text(&["PO1"])),
            ("ocel_qualifier", true, Col::Text(&["Change of quantity"])),
        ],
    );
    write_parquet(
        &root.join("relations/o2o.parquet"),
        &[
            ("ocel_source_id", true, Col::Text(&[])),
            ("ocel_target_id", true, Col::Text(&[])),
            ("ocel_qualifier", true, Col::Text(&[])),
        ],
    );
    std::fs::write(
        root.join("ocel-meta.json"),
        r#"{
          "ocelVersion": "2.0",
          "bundleFormatVersion": "1.0",
          "storageFormat": "parquet",
          "eventTypes": {
            "Change PO Quantity": { "file": "events/event_Change%20PO%20Quantity.parquet",
              "attributes": [{ "name": "po_editor", "type": "string" }] }
          },
          "objectTypes": {
            "Purchase Order": { "file": "objects/object_Purchase%20Order.parquet",
              "changesFile": "object_changes/object_changes_Purchase%20Order.parquet",
              "attributes": [{ "name": "po_product", "type": "string" },
                             { "name": "po_quantity", "type": "integer" }] }
          },
          "relations": { "e2o": "relations/e2o.parquet", "o2o": "relations/o2o.parquet" }
        }"#,
    )
    .expect("write manifest");

    let ocel = import_ocel_bundle(root).expect("import");
    assert_eq!(ocel.events.len(), 1);
    assert_eq!(ocel.objects.len(), 1);
    assert_eq!(
        attr_history(&ocel, "PO1", "po_quantity"),
        [
            "1970-01-01T00:00:00+00:00=500",
            "2022-01-13T12:00:00+00:00=600"
        ],
        "the INT64 attribute stays an integer through a change"
    );
    assert_eq!(
        attr_history(&ocel, "PO1", "po_product"),
        ["1970-01-01T00:00:00+00:00=Cows"],
        "the null cell in the change row records nothing"
    );
    assert_eq!(
        ocel.events[0].time.to_rfc3339(),
        "2022-01-13T12:00:00+00:00",
        "TIMESTAMP(MICROS) arrives as an instant, not as text to be parsed"
    );
}
