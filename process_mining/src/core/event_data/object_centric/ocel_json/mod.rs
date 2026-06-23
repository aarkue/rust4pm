//! OCEL 2.0 JSON Format Import/Export
use std::{
    fs::File,
    io::{BufReader, BufWriter},
    path::Path,
};

use serde::{
    de::{DeserializeSeed, IgnoredAny, MapAccess, SeqAccess, Visitor},
    Deserializer, Serialize, Serializer,
};

use crate::core::event_data::object_centric::{
    appendable::AppendableOCEL,
    io::OCELIOError,
    ocel_struct::{OCELEvent, OCELObject, OCELType, OCEL},
    readable::ReadableOCEL,
};

///
/// Serialize an OCEL backend (e.g., [`OCEL`] or
/// [`super::linked_ocel::SlimLinkedOCEL`]) as a JSON [`String`].
///
pub fn ocel_to_json<R: ReadableOCEL + ?Sized>(ocel: &R) -> String {
    let bytes = export_ocel_json_to_vec(ocel).expect("writing JSON to a Vec cannot fail");
    String::from_utf8(bytes).expect("serde_json always emits valid UTF-8")
}

///
/// Import [`OCEL`] from a JSON [`String`], returning a [`Result`].
///
/// [`serde_json`] can also be used to import [`OCEL`] from other targets (e.g., `serde_json::from_reader`)
///
pub fn try_json_to_ocel(ocel_json: &str) -> Result<OCEL, serde_json::Error> {
    serde_json::from_str(ocel_json)
}

///
/// Import [`OCEL`] from a JSON [`String`].
///
/// Panics on malformed JSON; prefer [`try_json_to_ocel`] for fallible callers.
///
pub fn json_to_ocel(ocel_json: &str) -> OCEL {
    try_json_to_ocel(ocel_json).expect("malformed OCEL JSON")
}

///
/// Import [`OCEL`] from a JSON file given by a filepath
///
/// See also [`import_ocel_json_slice`].
///
pub fn import_ocel_json_path<P: AsRef<std::path::Path>>(path: P) -> Result<OCEL, std::io::Error> {
    let reader: BufReader<File> = BufReader::new(File::open(path)?);
    Ok(serde_json::from_reader(reader)?)
}

///
/// Import [`OCEL`] from a JSON byte slice
///
/// See also [`import_ocel_json_path`].
///
pub fn import_ocel_json_slice(slice: &[u8]) -> Result<OCEL, std::io::Error> {
    Ok(serde_json::from_slice(slice)?)
}

/// Export an OCEL backend to a JSON file at the specified path.
pub fn export_ocel_json_to_path<R, P>(ocel: &R, path: P) -> Result<(), std::io::Error>
where
    R: ReadableOCEL + ?Sized,
    P: AsRef<Path>,
{
    let writer: BufWriter<File> = BufWriter::new(File::create(path)?);
    Ok(write_ocel_json(ocel, writer)?)
}

/// Export an OCEL backend to JSON in a byte array ([`Vec<u8>`]).
pub fn export_ocel_json_to_vec<R: ReadableOCEL + ?Sized>(
    ocel: &R,
) -> Result<Vec<u8>, std::io::Error> {
    let mut buf = Vec::new();
    write_ocel_json(ocel, &mut buf)?;
    Ok(buf)
}

///
/// Stream an OCEL backend as JSON into the given writer.
///
pub fn export_ocel_json_to_writer<R, W>(ocel: &R, writer: W) -> Result<(), std::io::Error>
where
    R: ReadableOCEL + ?Sized,
    W: std::io::Write,
{
    Ok(write_ocel_json(ocel, writer)?)
}

/// Stream an OCEL to `writer` as JSON. Field order matches `OCEL`'s `Serialize` derive
/// so `&OCEL` output is byte-identical.
fn write_ocel_json<R, W>(ocel: &R, writer: W) -> Result<(), serde_json::Error>
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

///
/// Stream a JSON-serialized OCEL into an [`AppendableOCEL`].
///
/// The caller is responsible for invoking [`AppendableOCEL::finalize`] afterwards if
/// the implementation requires it. Type declarations and instances may be intermixed;
/// relationships referencing not-yet-seen object IDs are buffered by implementations
/// that support it.
///
pub fn import_ocel_json_into<R, A>(reader: R, ocel: &mut A) -> Result<(), OCELIOError>
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
        while let Some(key) = map.next_key::<String>()? {
            match key.as_str() {
                "eventTypes" => {
                    for t in map.next_value::<Vec<OCELType>>()? {
                        if let Err(e) = self.ocel.declare_event_type(t) {
                            return tunnel_append_err::<_, _, M::Error>(self.append_err, e);
                        }
                    }
                }
                "objectTypes" => {
                    for t in map.next_value::<Vec<OCELType>>()? {
                        if let Err(e) = self.ocel.declare_object_type(t) {
                            return tunnel_append_err::<_, _, M::Error>(self.append_err, e);
                        }
                    }
                }
                "events" => {
                    map.next_value_seed(OcelSeqSeed::<A, OCELEvent, _> {
                        ocel: self.ocel,
                        append_err: self.append_err,
                        expecting: "an array of OCEL events",
                        append: |a: &mut A, e: OCELEvent| {
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
                    map.next_value_seed(OcelSeqSeed::<A, OCELObject, _> {
                        ocel: self.ocel,
                        append_err: self.append_err,
                        expecting: "an array of OCEL objects",
                        append: |a: &mut A, o: OCELObject| {
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
    use crate::core::event_data::object_centric::linked_ocel::{LinkedOCELAccess, SlimLinkedOCEL};
    use crate::test_utils::{get_test_data_path, sort_ocel_for_equality_compare};
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

    /// Regression: `&OCEL` JSON output must remain byte-identical to a derived
    /// `serde_json::to_vec(&ocel)` reference.
    #[test]
    fn export_byte_identical_for_ocel() {
        let bytes = fixture_bytes();
        let ocel = import_ocel_json_slice(&bytes).unwrap();
        let exported = export_ocel_json_to_vec(&ocel).unwrap();
        let reference = serde_json::to_vec(&ocel).unwrap();
        assert_eq!(exported, reference);
    }

    /// Streaming export from `SlimLinkedOCEL` reimports as the original `OCEL`.
    #[test]
    fn export_slim_roundtrip() {
        let bytes = fixture_bytes();
        let mut ocel = import_ocel_json_slice(&bytes).unwrap();
        let slim = SlimLinkedOCEL::from_ocel(ocel.clone());
        let bytes_slim = export_ocel_json_to_vec(&slim).unwrap();
        let mut back = import_ocel_json_slice(&bytes_slim).unwrap();
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
        let via_ocel = SlimLinkedOCEL::from_ocel(import_ocel_json_slice(&bytes).unwrap());

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
            OCELIOError::Other(s) => assert!(
                s.contains("Duplicate event id: dup"),
                "expected DuplicateEventId, got {s:?}"
            ),
            other => panic!("expected typed sink error, got {other:?}"),
        }
    }

    /// Streaming import directly into `SlimLinkedOCEL` matches the via-`from_ocel` baseline.
    #[test]
    fn import_into_slim_streaming() {
        let bytes = fixture_bytes();
        let mut slim = SlimLinkedOCEL::new();
        import_ocel_json_into(bytes.as_slice(), &mut slim).unwrap();
        slim.finalize().unwrap();
        let via_ocel = SlimLinkedOCEL::from_ocel(import_ocel_json_slice(&bytes).unwrap());

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
