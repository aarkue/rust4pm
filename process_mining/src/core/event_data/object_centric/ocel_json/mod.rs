//! OCEL 2.0 JSON Format Import/Export
use serde::{
    de::{DeserializeSeed, IgnoredAny, MapAccess, SeqAccess, Visitor},
    Deserializer, Serialize, Serializer,
};

use crate::core::event_data::object_centric::{
    appendable::AppendableOCEL,
    io::OCELIOError,
    ocel_struct::{DeclaredAttributeTypes, JsonOCELEvent, JsonOCELObject, OCELType},
    readable::ReadableOCEL,
};

/// Stream an OCEL to `writer` as JSON. Field order matches `OCEL`'s `Serialize` derive
/// so `&OCEL` output is byte-identical.
pub(crate) fn export_ocel_json_to_writer<R, W>(ocel: &R, writer: W) -> Result<(), serde_json::Error>
where
    R: ReadableOCEL + ?Sized,
    W: std::io::Write,
{
    use serde::ser::SerializeMap;
    let mut ser = serde_json::Serializer::new(writer);
    let mut m = ser.serialize_map(Some(4))?;
    m.serialize_entry("eventTypes", ocel.event_types())?;
    m.serialize_entry("objectTypes", ocel.object_types())?;
    m.serialize_entry("events", &EventsStream { ocel })?;
    m.serialize_entry("objects", &ObjectsStream { ocel })?;
    m.end()
}

struct EventsStream<'a, R: ?Sized> {
    ocel: &'a R,
}

impl<'a, R: ReadableOCEL + ?Sized> Serialize for EventsStream<'a, R> {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        use serde::ser::SerializeSeq;
        let mut seq = s.serialize_seq(None)?;
        for e in self.ocel.iter_events() {
            seq.serialize_element(&*e)?;
        }
        seq.end()
    }
}

struct ObjectsStream<'a, R: ?Sized> {
    ocel: &'a R,
}

impl<'a, R: ReadableOCEL + ?Sized> Serialize for ObjectsStream<'a, R> {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        use serde::ser::SerializeSeq;
        let mut seq = s.serialize_seq(None)?;
        for o in self.ocel.iter_objects() {
            seq.serialize_element(&*o)?;
        }
        seq.end()
    }
}

/// Stream a JSON-serialized OCEL into an [`AppendableOCEL`].
///
/// The caller is responsible for invoking [`AppendableOCEL::finalize`] afterwards if
/// the implementation requires it. Type declarations and instances may be intermixed;
/// relationships referencing not-yet-seen object IDs are buffered by implementations
/// that support it.
pub(crate) fn import_ocel_json_into<R, A>(reader: R, ocel: &mut A) -> Result<(), OCELIOError>
where
    R: std::io::Read,
    A: AppendableOCEL,
    A::Error: Into<OCELIOError>,
{
    let mut append_err: Option<A::Error> = None;
    let mut de = serde_json::Deserializer::from_reader(reader);
    let result = de.deserialize_map(OcelTopVisitor {
        ocel,
        append_err: &mut append_err,
    });
    if let Some(e) = append_err {
        return Err(e.into());
    }
    result?;
    Ok(())
}

/// Visitor for the top-level OCEL JSON object. Type lists are fully deserialized;
/// `events` and `objects` are streamed element-by-element into the sink.
struct OcelTopVisitor<'a, A: AppendableOCEL> {
    ocel: &'a mut A,
    append_err: &'a mut Option<A::Error>,
}

impl<'a, 'de, A: AppendableOCEL> Visitor<'de> for OcelTopVisitor<'a, A> {
    type Value = ();

    fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("an OCEL JSON object")
    }

    fn visit_map<M: MapAccess<'de>>(self, mut map: M) -> Result<(), M::Error> {
        // Attribute values are read against the types declared before them; types declared
        // only after an event or object leave its string values to be inferred
        let mut event_attribute_types = DeclaredAttributeTypes::default();
        let mut object_attribute_types = DeclaredAttributeTypes::default();
        while let Some(key) = map.next_key::<String>()? {
            match key.as_str() {
                "eventTypes" => {
                    for t in map.next_value::<Vec<OCELType>>()? {
                        event_attribute_types.declare(&t);
                        if let Err(e) = self.ocel.declare_event_type(t) {
                            return tunnel_append_err::<_, _, M::Error>(self.append_err, e);
                        }
                    }
                }
                "objectTypes" => {
                    for t in map.next_value::<Vec<OCELType>>()? {
                        object_attribute_types.declare(&t);
                        if let Err(e) = self.ocel.declare_object_type(t) {
                            return tunnel_append_err::<_, _, M::Error>(self.append_err, e);
                        }
                    }
                }
                "events" => {
                    map.next_value_seed(OcelSeqSeed::<A, JsonOCELEvent, _> {
                        ocel: self.ocel,
                        append_err: self.append_err,
                        expecting: "an array of OCEL events",
                        append: |a: &mut A, e: JsonOCELEvent| {
                            let e = e.resolve(&event_attribute_types);
                            a.append_event(
                                e.id,
                                &e.event_type,
                                e.time,
                                e.attributes,
                                e.relationships,
                            )
                        },
                        _t: std::marker::PhantomData,
                    })?;
                }
                "objects" => {
                    map.next_value_seed(OcelSeqSeed::<A, JsonOCELObject, _> {
                        ocel: self.ocel,
                        append_err: self.append_err,
                        expecting: "an array of OCEL objects",
                        append: |a: &mut A, o: JsonOCELObject| {
                            let o = o.resolve(&object_attribute_types);
                            a.append_object(o.id, &o.object_type, o.attributes, o.relationships)
                        },
                        _t: std::marker::PhantomData,
                    })?;
                }
                _ => {
                    let _: IgnoredAny = map.next_value()?;
                }
            }
        }
        Ok(())
    }
}

/// Capture an [`AppendableOCEL`] error in `slot` and return a placeholder serde error.
/// The outer [`import_ocel_json_into`] checks `slot` first and returns the typed error,
/// preserving fidelity that `serde::de::Error::custom` would otherwise lose.
fn tunnel_append_err<E, R, DE: serde::de::Error>(slot: &mut Option<E>, err: E) -> Result<R, DE> {
    *slot = Some(err);
    Err(DE::custom("append error"))
}

/// Stream a JSON sequence (`events` or `objects`) element-by-element into the OCEL.
struct OcelSeqSeed<'a, A: AppendableOCEL, T, F> {
    ocel: &'a mut A,
    append_err: &'a mut Option<A::Error>,
    expecting: &'static str,
    append: F,
    _t: std::marker::PhantomData<fn(T)>,
}

impl<'a, 'de, A, T, F> DeserializeSeed<'de> for OcelSeqSeed<'a, A, T, F>
where
    A: AppendableOCEL,
    T: serde::Deserialize<'de>,
    F: FnMut(&mut A, T) -> Result<(), A::Error>,
{
    type Value = ();
    fn deserialize<D: Deserializer<'de>>(self, de: D) -> Result<(), D::Error> {
        de.deserialize_seq(self)
    }
}

impl<'a, 'de, A, T, F> Visitor<'de> for OcelSeqSeed<'a, A, T, F>
where
    A: AppendableOCEL,
    T: serde::Deserialize<'de>,
    F: FnMut(&mut A, T) -> Result<(), A::Error>,
{
    type Value = ();

    fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.expecting)
    }

    fn visit_seq<S: SeqAccess<'de>>(mut self, mut seq: S) -> Result<(), S::Error> {
        while let Some(item) = seq.next_element::<T>()? {
            if let Err(err) = (self.append)(self.ocel, item) {
                return tunnel_append_err::<_, _, S::Error>(self.append_err, err);
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::event_data::object_centric::linked_ocel::{
        slim_linked_ocel::SlimAppendError, LinkedOCELAccess, SlimLinkedOCEL,
    };
    use crate::core::event_data::object_centric::ocel_struct::{OCELAttributeValue, OCEL};
    use crate::test_utils::{get_test_data_path, sort_ocel_for_equality_compare};
    use crate::{Exportable, Importable};
    use std::collections::HashMap;
    use std::fs;

    fn fixture_bytes() -> Vec<u8> {
        fs::read(
            get_test_data_path()
                .join("ocel")
                .join("pm4py-ocel20_example.jsonocel"),
        )
        .unwrap()
    }

    /// `&OCEL` JSON output is byte-identical to the derived `serde_json::to_vec(&ocel)`.
    #[test]
    fn export_byte_identical_for_ocel() {
        let bytes = fixture_bytes();
        let ocel = OCEL::import_from_bytes(&bytes, "json").unwrap();
        let exported = ocel.export_to_bytes("json").unwrap();
        let reference = serde_json::to_vec(&ocel).unwrap();
        assert_eq!(exported, reference);
    }

    /// Streaming export from `SlimLinkedOCEL` reimports as the original `OCEL`.
    #[test]
    fn export_slim_roundtrip() {
        let bytes = fixture_bytes();
        let mut ocel = OCEL::import_from_bytes(&bytes, "json").unwrap();
        let slim = SlimLinkedOCEL::from_ocel(ocel.clone());
        let bytes_slim = slim.export_to_bytes("json").unwrap();
        let mut back = OCEL::import_from_bytes(&bytes_slim, "json").unwrap();
        sort_ocel_for_equality_compare(&mut ocel);
        sort_ocel_for_equality_compare(&mut back);
        assert_eq!(back.event_types, ocel.event_types);
        assert_eq!(back.object_types, ocel.object_types);
        let evs_ref: HashMap<&str, _> = ocel.events.iter().map(|e| (e.id.as_str(), e)).collect();
        let evs_back: HashMap<&str, _> = back.events.iter().map(|e| (e.id.as_str(), e)).collect();
        assert_eq!(evs_ref, evs_back);
        let obs_ref: HashMap<&str, _> = ocel.objects.iter().map(|o| (o.id.as_str(), o)).collect();
        let obs_back: HashMap<&str, _> = back.objects.iter().map(|o| (o.id.as_str(), o)).collect();
        assert_eq!(obs_ref, obs_back);
    }

    /// Streaming import tolerates a misordered JSON: events/objects appearing before their
    /// type lists are auto-declared by the `AppendableOCEL`, and the later type declarations
    /// merge missing attributes without reordering.
    #[test]
    fn import_into_slim_streaming_misordered() {
        let bytes = fixture_bytes();
        // Reorder top-level keys so events/objects precede their type lists.
        let v: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        let m = v.as_object().unwrap();
        let mut reordered = serde_json::Map::new();
        reordered.insert("events".into(), m["events"].clone());
        reordered.insert("objects".into(), m["objects"].clone());
        reordered.insert("eventTypes".into(), m["eventTypes"].clone());
        reordered.insert("objectTypes".into(), m["objectTypes"].clone());
        let bytes_misordered = serde_json::to_vec(&serde_json::Value::Object(reordered)).unwrap();

        let mut slim = SlimLinkedOCEL::new();
        import_ocel_json_into(bytes_misordered.as_slice(), &mut slim).unwrap();
        slim.finalize().unwrap();
        let via_ocel = SlimLinkedOCEL::from_ocel(OCEL::import_from_bytes(&bytes, "json").unwrap());

        let mut a = slim.construct_ocel();
        let mut b = via_ocel.construct_ocel();
        sort_ocel_for_equality_compare(&mut a);
        sort_ocel_for_equality_compare(&mut b);
        let a_evs: HashMap<&str, _> = a.events.iter().map(|e| (e.id.as_str(), e)).collect();
        let b_evs: HashMap<&str, _> = b.events.iter().map(|e| (e.id.as_str(), e)).collect();
        assert_eq!(a_evs, b_evs);
        let a_obs: HashMap<&str, _> = a.objects.iter().map(|o| (o.id.as_str(), o)).collect();
        let b_obs: HashMap<&str, _> = b.objects.iter().map(|o| (o.id.as_str(), o)).collect();
        assert_eq!(a_obs, b_obs);
    }

    /// Sink errors (e.g., a `DuplicateEventId` from `SlimLinkedOCEL`) surface from
    /// `import_ocel_json_into` as the typed sink error, not as a generic serde error.
    #[test]
    fn import_into_slim_surfaces_sink_error() {
        let json = br#"{
            "eventTypes": [{"name": "x", "attributes": []}],
            "objectTypes": [],
            "events": [
                {"id": "dup", "type": "x", "time": "2024-01-01T00:00:00Z", "attributes": [], "relationships": []},
                {"id": "dup", "type": "x", "time": "2024-01-02T00:00:00Z", "attributes": [], "relationships": []}
            ],
            "objects": []
        }"#;
        let mut slim = SlimLinkedOCEL::new();
        let err = import_ocel_json_into(json.as_slice(), &mut slim).unwrap_err();
        match err {
            OCELIOError::Append(SlimAppendError::DuplicateEventId(id)) => assert_eq!(id, "dup"),
            other => panic!("expected typed sink error, got {other:?}"),
        }
    }

    /// Build a minimal OCEL JSON with a single event attribute of the given raw JSON value,
    /// declared with `attr_type` in the event type.
    fn json_with_event_attr(attr_type: &str, raw_value: &str) -> Vec<u8> {
        format!(
            r#"{{
                "eventTypes": [{{"name": "x", "attributes": [{{"name": "a", "type": "{attr_type}"}}]}}],
                "objectTypes": [],
                "events": [{{"id": "e1", "type": "x", "time": "2024-01-01T00:00:00Z",
                             "attributes": [{{"name": "a", "value": {raw_value}}}], "relationships": []}}],
                "objects": []
            }}"#
        )
        .into_bytes()
    }

    fn attr_value_of(bytes: &[u8]) -> OCELAttributeValue {
        let ocel = OCEL::import_from_bytes(bytes, "json").unwrap();
        ocel.events[0].attributes[0].value.clone()
    }

    /// Scalar inference is unchanged: strings stay strings unless they are RFC3339.
    #[test]
    fn import_scalar_attribute_value_inference() {
        let cases: Vec<(&str, OCELAttributeValue)> = vec![
            ("42", OCELAttributeValue::Integer(42)),
            ("-7", OCELAttributeValue::Integer(-7)),
            ("1.5", OCELAttributeValue::Float(1.5)),
            ("true", OCELAttributeValue::Boolean(true)),
            ("null", OCELAttributeValue::Null),
            (
                r#""hello""#,
                OCELAttributeValue::String("hello".to_string()),
            ),
            // Not RFC3339 => stays a String at inference time.
            (
                r#""2023-10-06 09:30:21.890421""#,
                OCELAttributeValue::String("2023-10-06 09:30:21.890421".to_string()),
            ),
        ];
        for (raw, expected) in cases {
            assert_eq!(
                attr_value_of(&json_with_event_attr("string", raw)),
                expected,
                "raw value {raw:?}"
            );
        }
        // RFC3339 strings are recognized as timestamps unless declared with another type.
        let rfc3339 = r#""2024-01-01T00:00:00Z""#;
        for (declared, expect_time) in [("string", false), ("integer", false), ("time", true)] {
            let bytes = json_with_event_attr(declared, rfc3339);
            assert_eq!(
                matches!(attr_value_of(&bytes), OCELAttributeValue::Time(_)),
                expect_time,
                "declared {declared}"
            );
            let mut streamed = OCEL {
                event_types: vec![],
                object_types: vec![],
                events: vec![],
                objects: vec![],
            };
            import_ocel_json_into(bytes.as_slice(), &mut streamed).unwrap();
            assert_eq!(
                streamed.events[0].attributes[0].value,
                attr_value_of(&bytes),
                "declared {declared}"
            );
        }
        let undeclared = String::from_utf8(json_with_event_attr("string", rfc3339))
            .unwrap()
            .replace(r#""name": "a", "type""#, r#""name": "other", "type""#);
        assert!(matches!(
            attr_value_of(undeclared.as_bytes()),
            OCELAttributeValue::Time(_)
        ));
        assert_eq!(
            attr_value_of(&json_with_event_attr("string", r#"" A""#)),
            OCELAttributeValue::String(" A".to_string())
        );
    }

    /// Values are read as their declared type: the JSON schema types every value as a string,
    /// official logs write whole floats as integers, and pm4py declares timestamps as `date`.
    #[test]
    fn import_reads_values_as_declared_type() {
        let time = |s| OCELAttributeValue::Time(chrono::DateTime::parse_from_rfc3339(s).unwrap());
        for (declared, raw, expected) in [
            ("float", "5", OCELAttributeValue::Float(5.0)),
            ("float", r#""2.5""#, OCELAttributeValue::Float(2.5)),
            ("integer", r#""7""#, OCELAttributeValue::Integer(7)),
            ("boolean", r#""true""#, OCELAttributeValue::Boolean(true)),
            (
                "time",
                r#""2023-10-06 09:30:21""#,
                time("2023-10-06T09:30:21Z"),
            ),
            (
                "date",
                r#""2024-01-01T10:00:00""#,
                time("2024-01-01T10:00:00Z"),
            ),
            (
                "integer",
                r#""n/a""#,
                OCELAttributeValue::String("n/a".to_string()),
            ),
        ] {
            let bytes = json_with_event_attr(declared, raw);
            assert_eq!(attr_value_of(&bytes), expected, "{declared} {raw}");
        }
    }

    /// Integers exceeding `i64` degrade to `Float` rather than failing the import.
    #[test]
    fn import_out_of_range_integer_becomes_float() {
        assert!(matches!(
            attr_value_of(&json_with_event_attr("integer", "18446744073709551615")),
            OCELAttributeValue::Float(_)
        ));
    }

    /// Event timestamps accept the same non-RFC3339 formats as object attribute times.
    #[test]
    fn import_event_time_accepts_loose_formats() {
        for time in [
            "2023-10-06 09:30:21.890421",
            "2023-10-06T09:30:21",
            "2023-10-06T09:30:21+0000",
            "2023-10-06 09:30:21 UTC",
            "Mon Apr 03 2023 12:08:18 GMT+0200 (Mitteleuropäische Sommerzeit)",
        ] {
            let json = format!(
                r#"{{"eventTypes": [], "objectTypes": [],
                     "events": [{{"id": "e1", "type": "x", "time": "{time}",
                                  "attributes": [], "relationships": []}}],
                     "objects": []}}"#
            );
            assert!(
                OCEL::import_from_bytes(json.as_bytes(), "json").is_ok(),
                "event time {time:?} should parse"
            );
        }
    }

    /// A string attribute declared as `time` but not in RFC3339 is recovered by the
    /// type-directed coercion in `SlimLinkedOCEL`.
    #[test]
    fn declared_time_attribute_coerced_from_loose_format() {
        let bytes = json_with_event_attr("time", r#""2023-10-06 09:30:21.890421""#);
        let ocel = OCEL::import_from_bytes(&bytes, "json").unwrap();
        let slim = SlimLinkedOCEL::from_ocel(ocel);
        let back = slim.construct_ocel();
        assert!(matches!(
            back.events[0].attributes[0].value,
            OCELAttributeValue::Time(_)
        ));
    }

    /// Streaming import directly into `SlimLinkedOCEL` matches the via-`from_ocel` baseline.
    #[test]
    fn import_into_slim_streaming() {
        let bytes = fixture_bytes();
        let mut slim = SlimLinkedOCEL::new();
        import_ocel_json_into(bytes.as_slice(), &mut slim).unwrap();
        slim.finalize().unwrap();
        let via_ocel = SlimLinkedOCEL::from_ocel(OCEL::import_from_bytes(&bytes, "json").unwrap());

        let mut a = slim.construct_ocel();
        let mut b = via_ocel.construct_ocel();
        sort_ocel_for_equality_compare(&mut a);
        sort_ocel_for_equality_compare(&mut b);
        assert_eq!(a.event_types, b.event_types);
        assert_eq!(a.object_types, b.object_types);
        let a_evs: HashMap<&str, _> = a.events.iter().map(|e| (e.id.as_str(), e)).collect();
        let b_evs: HashMap<&str, _> = b.events.iter().map(|e| (e.id.as_str(), e)).collect();
        assert_eq!(a_evs, b_evs);
        let a_obs: HashMap<&str, _> = a.objects.iter().map(|o| (o.id.as_str(), o)).collect();
        let b_obs: HashMap<&str, _> = b.objects.iter().map(|o| (o.id.as_str(), o)).collect();
        assert_eq!(a_obs, b_obs);
    }
}
