//! What survives a round trip through the bundled container and the flat CSV, and what the
//! formats say may not.
//!
//! The bundle stores names and values as they are and carries a schema, so a round trip through
//! it is exact. The flat CSV has three characters that structure a cell, no schema at all, and
//! rules about whitespace, so it is exact only where the format says it is.

use std::collections::BTreeMap;

use process_mining::core::event_data::object_centric::ocel_csv::{
    export_ocel_csv_to_string, import_ocel_csv,
};
use process_mining::core::event_data::object_centric::{
    OCELAttributeValue, OCELEvent, OCELEventAttribute, OCELObject, OCELObjectAttribute,
    OCELRelationship, OCELType, OCELTypeAttribute, OCEL,
};

#[cfg(feature = "ocel-bundle")]
use process_mining::core::event_data::object_centric::ocel_bundle::{
    export_ocel_bundle, import_ocel_bundle, BundleExportOptions, ContainerLayout, StorageFormat,
};

/// A one-event, one-object log with `text` woven through every name, id and qualifier.
fn log_named(text: &str) -> OCEL {
    let event_type = format!("ev {text}");
    let object_type = format!("ob {text}");
    OCEL {
        event_types: vec![OCELType {
            name: event_type.clone(),
            attributes: vec![OCELTypeAttribute {
                name: format!("ea {text}"),
                value_type: "string".into(),
            }],
        }],
        object_types: vec![OCELType {
            name: object_type.clone(),
            attributes: vec![OCELTypeAttribute {
                name: format!("oa {text}"),
                value_type: "string".into(),
            }],
        }],
        events: vec![OCELEvent {
            id: format!("e {text}"),
            event_type,
            time: "2024-01-01T10:00:00+00:00".parse().unwrap(),
            attributes: vec![OCELEventAttribute {
                name: format!("ea {text}"),
                value: OCELAttributeValue::String(format!("val {text}")),
            }],
            relationships: vec![OCELRelationship {
                object_id: format!("o {text}"),
                qualifier: format!("q {text}"),
            }],
        }],
        objects: vec![OCELObject {
            id: format!("o {text}"),
            object_type,
            attributes: vec![OCELObjectAttribute {
                name: format!("oa {text}"),
                value: OCELAttributeValue::String(format!("val {text}")),
                time: "2024-01-02T10:00:00+00:00".parse().unwrap(),
            }],
            relationships: vec![],
        }],
    }
}

/// A one-event log whose single event attribute carries `value` as text.
fn log_valued(value: &str) -> OCEL {
    let mut ocel = log_named("v");
    ocel.events[0].attributes[0].value = OCELAttributeValue::String(value.to_string());
    ocel
}

/// Everything the log says, as sorted lines, so two logs are compared exactly rather than by
/// counts. Object attributes are keyed by name only, because the CSV infers type declarations
/// rather than storing them.
fn facts(ocel: &OCEL) -> Vec<String> {
    let mut out = Vec::new();
    for t in &ocel.event_types {
        out.push(format!("event type {:?}", t.name));
    }
    for t in &ocel.object_types {
        out.push(format!("object type {:?}", t.name));
    }
    for e in &ocel.events {
        out.push(format!("event {:?} of {:?}", e.id, e.event_type));
        for a in &e.attributes {
            out.push(format!("event {:?} {:?} = {:?}", e.id, a.name, a.value));
        }
        for r in &e.relationships {
            out.push(format!(
                "e2o {:?} -> {:?} [{:?}]",
                e.id, r.object_id, r.qualifier
            ));
        }
    }
    for o in &ocel.objects {
        out.push(format!("object {:?} of {:?}", o.id, o.object_type));
        for a in &o.attributes {
            out.push(format!("object {:?} {:?} = {:?}", o.id, a.name, a.value));
        }
    }
    out.sort();
    out
}

fn through_csv(ocel: &OCEL) -> OCEL {
    let text = export_ocel_csv_to_string(ocel).expect("csv export");
    import_ocel_csv(text.as_bytes()).unwrap_or_else(|e| panic!("csv import: {e}\n---\n{text}"))
}

#[cfg(feature = "ocel-bundle")]
fn through_bundle(ocel: &OCEL, label: &str) -> OCEL {
    let dir = tempfile::tempdir().expect("tempdir");
    let target = dir.path().join(label);
    std::fs::create_dir_all(&target).expect("mkdir");
    export_ocel_bundle(
        ocel,
        &target,
        BundleExportOptions {
            layout: ContainerLayout::Directory,
            storage: if cfg!(feature = "ocel-bundle-parquet") {
                StorageFormat::Parquet
            } else {
                StorageFormat::Csv
            },
        },
    )
    .expect("bundle export");
    import_ocel_bundle(&target).expect("bundle import")
}

/// Text that has no special meaning to either format, so both must give it back untouched.
const ORDINARY: &[(&str, &str)] = &[
    ("comma", "a,b"),
    ("quote", "a\"b"),
    ("newline", "a\nb"),
    ("tab", "a\tb"),
    ("unicode", "Ünïcödé"),
    ("percent", "a%2Fb"),
    ("semicolon", "a;b"),
];

/// The characters that structure an `ot:<X>` cell. Written raw, `a/b` reads back as two
/// references, `a#b` as a truncated id, and `a{b` as broken JSON, so the exporter escapes them.
const RESERVED: &[(&str, &str)] = &[
    ("slash", "a/b"),
    ("hash", "a#b"),
    ("brace", "a{b"),
    ("backslash", "a\\b"),
    ("all four", "a/b#c{d\\e"),
    ("only separators", "/#{"),
];

#[test]
fn the_csv_gives_back_ordinary_text_unchanged() {
    for (label, text) in ORDINARY {
        let src = log_named(text);
        assert_eq!(facts(&src), facts(&through_csv(&src)), "{label}");
    }
}

#[test]
fn the_csv_gives_back_its_own_reserved_characters() {
    for (label, text) in RESERVED {
        let src = log_named(text);
        assert_eq!(facts(&src), facts(&through_csv(&src)), "{label}");
    }
}

/// Column names are arbitrary text too, so recognising the `ot:`/`ea:` prefixes must not assume
/// the header opens with single-byte characters.
#[test]
fn a_header_that_opens_with_multi_byte_text_is_read_not_split() {
    let csv = "id,activity,timestamp,Ünïcödé,ot:日本\n\
               e1,open,2024-01-01T10:00:00+0000,vÄ,o1";
    let ocel = import_ocel_csv(csv.as_bytes()).expect("import");
    assert_eq!(ocel.objects[0].object_type, "日本");
    assert_eq!(ocel.events[0].attributes[0].name, "Ünïcödé");
    assert_eq!(
        ocel.events[0].attributes[0].value,
        OCELAttributeValue::String("vÄ".into())
    );
}

/// A file written before the escape existed still reads the same way, because a backslash only
/// escapes the three structural characters and itself.
#[test]
fn an_unescaped_backslash_in_a_hand_written_file_is_still_a_backslash() {
    let csv = "id,activity,timestamp,ot:file\n\
               e1,open,2024-01-01T10:00:00+0000,C:\\Users\\me";
    let ocel = import_ocel_csv(csv.as_bytes()).expect("import");
    assert_eq!(ocel.objects.len(), 1);
    assert_eq!(ocel.objects[0].id, "C:\\Users\\me");
}

/// The format has no schema, so every value is retyped on the way in. A parse that cannot be
/// undone would lose the text silently, so these keep their string form.
#[test]
fn text_that_only_looks_numeric_stays_text() {
    for value in [
        "007",                            // leading zeros are not the number 7
        "+7",                             // the sign is not in the canonical spelling
        "1e3",                            // nor is an exponent
        "1_2",                            // nor a separator
        "0x1f",                           // nor a radix prefix
        "5.",                             // nor a bare point
        ".5",                             // nor a missing whole part
        "-0",                             // renders as `0`, so it would not come back
        "123456789012345678901234567890", // beyond i64: an f64 would round it
        "2024-01-01",                     // a date with no timezone is not an instant
        "2022-05-04 05:57:00",            // nor is a local wall clock
        // Short enough that a fixed-width look at the tail lands mid-character.
        "Ünïcödé",
        "aÄ",
        "日本",
        "aaaa̋",
    ] {
        let src = log_valued(value);
        let back = through_csv(&src);
        assert_eq!(
            back.events[0].attributes[0].value,
            OCELAttributeValue::String(value.to_string()),
            "{value:?} should have stayed a string"
        );
    }
}

/// The other half of the same rule: text that is the canonical spelling of a number or an
/// instant is meant to be read as one.
#[test]
fn text_that_is_a_number_is_read_as_one() {
    let cases: BTreeMap<&str, OCELAttributeValue> = [
        ("7", OCELAttributeValue::Integer(7)),
        ("-12", OCELAttributeValue::Integer(-12)),
        ("0", OCELAttributeValue::Integer(0)),
        ("0.5", OCELAttributeValue::Float(0.5)),
        ("-0.5", OCELAttributeValue::Float(-0.5)),
        ("-12.75", OCELAttributeValue::Float(-12.75)),
        // Trailing zeros are formatting, not value.
        ("5.00", OCELAttributeValue::Float(5.0)),
        ("true", OCELAttributeValue::Boolean(true)),
        // The format compares the boolean words without regard to case.
        ("TRUE", OCELAttributeValue::Boolean(true)),
    ]
    .into_iter()
    .collect();

    for (text, expected) in cases {
        let src = log_valued(text);
        let back = through_csv(&src);
        assert_eq!(
            back.events[0].attributes[0].value, expected,
            "{text:?} should have been read as {expected:?}"
        );
    }
}

/// An `ot:<X>` header names the exact object type, and an event attribute value is preserved
/// apart from RFC 4180 unquoting, so neither may be tidied.
#[test]
fn the_csv_keeps_the_whitespace_the_format_tells_it_to_keep() {
    let mut src = log_named("x");
    src.object_types[0].name = "ob trailing ".into();
    src.objects[0].object_type = "ob trailing ".into();
    src.events[0].attributes[0].value = OCELAttributeValue::String("  padded  ".into());

    let back = through_csv(&src);
    assert_eq!(back.objects[0].object_type, "ob trailing ");
    assert_eq!(
        back.events[0].attributes[0].value,
        OCELAttributeValue::String("  padded  ".into())
    );
}

/// The format says an id, an activity and a qualifier are read after their surrounding
/// whitespace is removed, so a name that ends in a space is not representable there.
#[test]
fn the_csv_trims_the_three_things_the_format_says_it_may() {
    let mut src = log_named("x");
    src.events[0].id = "e trailing ".into();
    src.events[0].event_type = "ev trailing ".into();
    src.event_types[0].name = "ev trailing ".into();
    src.events[0].relationships[0].qualifier = "q trailing ".into();

    let back = through_csv(&src);
    assert_eq!(back.events[0].id, "e trailing");
    assert_eq!(back.events[0].event_type, "ev trailing");
    assert_eq!(back.events[0].relationships[0].qualifier, "q trailing");
}

/// An o2o row naming a source the file never gave a type to is dropped. `strict` turns that
/// silent loss into an error on its own, without `verbose` also being set.
#[test]
fn strict_rejects_an_unknown_o2o_source_without_needing_verbose() {
    let csv = "id,activity,timestamp,ot:item\n\
               ghost,o2o,,i1#has";
    let strict = process_mining::core::event_data::object_centric::ocel_csv::OCELCSVImportOptions {
        strict: true,
        verbose: false,
        ..Default::default()
    };
    let err =
        process_mining::core::event_data::object_centric::ocel_csv::import_ocel_csv_with_options(
            csv.as_bytes(),
            &strict,
        );
    assert!(err.is_err(), "strict should reject an unknown o2o source");

    // Without `strict` it stays a skip, so the rest of the file still reads.
    let lenient = import_ocel_csv(csv.as_bytes()).expect("lenient import");
    assert!(lenient.objects.iter().all(|o| o.relationships.is_empty()));
}

/// An empty cell is a missing value, so an attribute whose value is the empty string cannot be
/// told apart from one that is absent.
#[test]
fn an_empty_string_attribute_does_not_survive_the_csv() {
    let src = log_valued("");
    let back = through_csv(&src);
    assert!(back.events[0].attributes.is_empty());
}

#[cfg(feature = "ocel-bundle")]
#[test]
fn the_bundle_gives_everything_back_exactly() {
    for (label, text) in ORDINARY.iter().chain(RESERVED) {
        let src = log_named(text);
        assert_eq!(
            facts(&src),
            facts(&through_bundle(&src, "names")),
            "{label}"
        );
    }

    for text in ["trailing ", " leading", "  ", "007", ""] {
        let mut src = log_named("x");
        src.events[0].attributes[0].value = OCELAttributeValue::String(text.to_string());
        src.objects[0].attributes[0].value = OCELAttributeValue::String(text.to_string());
        assert_eq!(
            facts(&src),
            facts(&through_bundle(&src, "values")),
            "value {text:?}"
        );
    }

    // The whitespace the CSV is required to trim, which the bundle has no reason to touch.
    let mut src = log_named("x");
    src.events[0].id = "e trailing ".into();
    src.events[0].event_type = "ev trailing ".into();
    src.event_types[0].name = "ev trailing ".into();
    src.events[0].relationships[0].qualifier = "q trailing ".into();
    assert_eq!(facts(&src), facts(&through_bundle(&src, "whitespace")));
}
