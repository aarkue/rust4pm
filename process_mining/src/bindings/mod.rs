#![cfg(feature = "bindings")]

//! # Bindings Module
//!
//! This module provides a framework for exposing Rust functions to dynamic environments
//! such as CLIs, Python bindings, or visual editors.
//!
//! ## Architecture
//!
//! - **Registry**: A global collection of `Binding` structs, collected via `inventory`.
//! - **AppState**: A thread-safe storage for "Big Types" (e.g., EventLogs)
//!   that are passed by reference (ID) rather than serialized.
//! - **Execution**: Functions are invoked via `call()`, which handles argument extraction
//!   and result storage.
//!
//! ## Usage
//!
//! 1. Define a function and annotate it with `#[register_binding]`.
//! 2. Use `list_functions()` to discover available commands.
//! 3. Use `call()` to execute them.
//!
//! ## Type Handling
//!
//! - **Simple Types**: Serialized/Deserialized via `serde_json`.
//! - **Big Types**: Stored in `AppState`. Arguments are string IDs pointing to the state.
//!   Return values are stored in state, and their new ID is returned.
//!
//! ## Helper Features
//!
//! - **Auto-Loading**: The `resolve_argument` function can automatically load "Big Types" from
//!   file paths, from base64 bytes or from inline JSON if the argument schema indicates a
//!   registry reference. `call_resolved` is `call` with that applied to every argument.

use crate::core::{
    event_data::{
        case_centric::utils::activity_projection::EventLogActivityProjection,
        object_centric::{
            linked_ocel::{IndexLinkedOCEL, LinkedOCELAccess, SlimLinkedOCEL},
            ocel_struct::OCEL,
        },
    },
    io::ExtensionWithMime,
    EventLog,
};
pub use macros_process_mining::{register_binding, CustomRegistryEntity, RegistryEntity};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{collections::HashMap, fmt::Display};
use std::{str::FromStr, sync::RwLock};

/// The formats the object-centric kinds accept on top of what an OCEL reader handles, by way of
/// the case-centric conversion. Empty without the feature that brings it.
fn case_centric_import_formats() -> Vec<ExtensionWithMime> {
    if cfg!(feature = "extraction-blueprint") {
        vec![
            ExtensionWithMime::new("xes", "application/xml"),
            ExtensionWithMime::new("xes.gz", "application/gzip"),
        ]
    } else {
        Vec::new()
    }
}

/// Whether `item_kind` should read `format` as a case-centric log rather than as an OCEL.
#[cfg(feature = "extraction-blueprint")]
fn reads_as_case_centric(item_kind: &RegistryItemKind, format: &str) -> bool {
    matches!(
        item_kind,
        RegistryItemKind::OCEL | RegistryItemKind::SlimLinkedOCEL
    ) && (format.ends_with("xes") || format.ends_with("xes.gz"))
}

/// Convert an already-parsed case-centric log into whichever object-centric kind was asked for.
#[cfg(feature = "extraction-blueprint")]
fn case_centric_as(item_kind: &RegistryItemKind, log: &EventLog) -> Result<RegistryItem, String> {
    use crate::core::event_data::object_centric::extraction::{
        event_log_to_ocel, event_log_to_slim_ocel,
    };
    match item_kind {
        RegistryItemKind::OCEL => event_log_to_ocel(log).map(RegistryItem::OCEL),
        _ => event_log_to_slim_ocel(log).map(RegistryItem::SlimLinkedOCEL),
    }
    .map_err(|e| e.to_string())
}

/// Manually maintained Registry enum of 'big' types
///
/// NOTE: When extending this with a new variant, make sure to also update `BIG_TYPES_NAMES` in the macro crate.
#[derive(Debug)]
#[allow(clippy::large_enum_variant, missing_docs)]
pub enum RegistryItem {
    TabularSource(TabularSource),
    EventLogActivityProjection(EventLogActivityProjection),
    IndexLinkedOCEL(IndexLinkedOCEL),
    SlimLinkedOCEL(SlimLinkedOCEL),
    EventLog(EventLog),
    OCEL(OCEL),
    /// A handle type contributed by a downstream crate, see [`CustomRegistryValue`].
    Custom(Box<dyn CustomRegistryValue>),
}

impl From<EventLog> for RegistryItem {
    fn from(value: EventLog) -> Self {
        Self::EventLog(value)
    }
}
impl From<EventLogActivityProjection> for RegistryItem {
    fn from(value: EventLogActivityProjection) -> Self {
        Self::EventLogActivityProjection(value)
    }
}
impl From<IndexLinkedOCEL> for RegistryItem {
    fn from(value: IndexLinkedOCEL) -> Self {
        Self::IndexLinkedOCEL(value)
    }
}
impl From<OCEL> for RegistryItem {
    fn from(value: OCEL) -> Self {
        Self::OCEL(value)
    }
}
impl From<SlimLinkedOCEL> for RegistryItem {
    fn from(value: SlimLinkedOCEL) -> Self {
        Self::SlimLinkedOCEL(value)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[allow(missing_docs)]
pub enum RegistryItemKind {
    TabularSource,
    EventLogActivityProjection,
    IndexLinkedOCEL,
    SlimLinkedOCEL,
    EventLog,
    OCEL,
    /// A kind contributed by a downstream crate, named by [`CustomRegistryValue::kind_name`].
    ///
    /// `&'static str` rather than `String` so the enum stays `Copy`.
    Custom(&'static str),
}

impl Display for RegistryItemKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.name())
    }
}

impl RegistryItemKind {
    /// The name this kind is known by, as used in ids, schemas and `x-registry-ref`.
    pub fn name(&self) -> &'static str {
        match self {
            RegistryItemKind::EventLogActivityProjection => "EventLogActivityProjection",
            RegistryItemKind::IndexLinkedOCEL => "IndexLinkedOCEL",
            RegistryItemKind::SlimLinkedOCEL => "SlimLinkedOCEL",
            RegistryItemKind::EventLog => "EventLog",
            RegistryItemKind::OCEL => "OCEL",
            RegistryItemKind::TabularSource => "TabularSource",
            RegistryItemKind::Custom(name) => name,
        }
    }

    /// Get all kinds of `RegistryItemKind`
    pub fn all_kinds() -> &'static [Self] {
        &[
            RegistryItemKind::OCEL,
            RegistryItemKind::EventLog,
            RegistryItemKind::EventLogActivityProjection,
            RegistryItemKind::SlimLinkedOCEL,
            RegistryItemKind::IndexLinkedOCEL,
            RegistryItemKind::TabularSource,
        ]
    }

    /// Get all kinds, including the ones downstream crates registered.
    ///
    /// [`RegistryItemKind::all_kinds`] stays limited to the built-ins, so a host that only knows
    /// those keeps seeing exactly those.
    pub fn all_registered_kinds() -> Vec<Self> {
        Self::all_kinds()
            .iter()
            .copied()
            .chain(custom_kinds().into_iter().map(|c| Self::Custom(c.name)))
            .collect()
    }

    /// Get known import formats
    pub fn known_import_formats(&self) -> Vec<ExtensionWithMime> {
        match self {
            RegistryItemKind::EventLogActivityProjection => {
                EventLogActivityProjection::known_import_formats()
            }
            RegistryItemKind::IndexLinkedOCEL => IndexLinkedOCEL::known_import_formats(),
            RegistryItemKind::EventLog => EventLog::known_import_formats(),
            RegistryItemKind::OCEL | RegistryItemKind::SlimLinkedOCEL => {
                let mut formats = OCEL::known_import_formats();
                formats.extend(case_centric_import_formats());
                formats
            }
            RegistryItemKind::TabularSource => TabularSource::known_import_formats(),
            RegistryItemKind::Custom(name) => custom_kind(name)
                .map(|c| (c.import_formats)())
                .unwrap_or_default(),
        }
    }
    /// Get known export formats
    pub fn known_export_formats(&self) -> Vec<ExtensionWithMime> {
        match self {
            RegistryItemKind::EventLogActivityProjection => {
                EventLogActivityProjection::known_export_formats()
            }
            RegistryItemKind::IndexLinkedOCEL => IndexLinkedOCEL::known_export_formats(),
            RegistryItemKind::EventLog => EventLog::known_export_formats(),
            RegistryItemKind::OCEL => OCEL::known_export_formats(),
            RegistryItemKind::SlimLinkedOCEL => OCEL::known_export_formats(),
            // A source is read, never written back out.
            RegistryItemKind::TabularSource => Vec::new(),
            RegistryItemKind::Custom(name) => custom_kind(name)
                .map(|c| (c.export_formats)())
                .unwrap_or_default(),
        }
    }
}

// Hand-written rather than derived because `Custom(&'static str)` has no derivable `Deserialize`,
// and because the derive would give it the externally tagged `{"Custom": "X"}` form while every
// other variant is a bare string. Going through `Display`/`FromStr` keeps the wire format a
// string for all kinds, byte-for-byte what the derive produced for the built-in six.
impl Serialize for RegistryItemKind {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(self.name())
    }
}

impl<'de> Deserialize<'de> for RegistryItemKind {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let s = String::deserialize(deserializer)?;
        s.parse().map_err(serde::de::Error::custom)
    }
}

impl std::str::FromStr for RegistryItemKind {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "EventLogActivityProjection" => Ok(RegistryItemKind::EventLogActivityProjection),
            "IndexLinkedOCEL" => Ok(RegistryItemKind::IndexLinkedOCEL),
            "EventLog" => Ok(RegistryItemKind::EventLog),
            "OCEL" => Ok(RegistryItemKind::OCEL),
            "SlimLinkedOCEL" => Ok(RegistryItemKind::SlimLinkedOCEL),
            "TabularSource" => Ok(RegistryItemKind::TabularSource),
            // Only registered custom names resolve, so a typo still reports as unknown.
            _ => custom_kind(s)
                .map(|c| RegistryItemKind::Custom(c.name))
                .ok_or_else(|| format!("Unknown RegistryItemKind: {}", s)),
        }
    }
}

use crate::core::io::{Exportable, Importable};
use crate::core::tabular_source::TabularSource;

impl RegistryItem {
    /// Wrap a downstream handle type as a registry item.
    ///
    /// There is no `From` impl for this: it would overlap the concrete `From` impls above.
    pub fn custom(value: impl CustomRegistryValue) -> Self {
        RegistryItem::Custom(Box::new(value))
    }

    /// Borrow the item as the custom type `T`, or `None` if it is not one.
    pub fn as_custom<T: CustomRegistryValue>(&self) -> Option<&T> {
        match self {
            RegistryItem::Custom(v) => (&**v as &dyn std::any::Any).downcast_ref::<T>(),
            _ => None,
        }
    }

    /// Mutably borrow the item as the custom type `T`, or `None` if it is not one.
    pub fn as_custom_mut<T: CustomRegistryValue>(&mut self) -> Option<&mut T> {
        match self {
            RegistryItem::Custom(v) => (&mut **v as &mut dyn std::any::Any).downcast_mut::<T>(),
            _ => None,
        }
    }

    /// Convert the registry item to a JSON value
    ///
    /// For "Big Types", this performs a full serialization.
    pub fn to_value(&self) -> Result<Value, String> {
        match self {
            RegistryItem::EventLog(log) => serde_json::to_value(log).map_err(|e| e.to_string()),
            RegistryItem::OCEL(ocel) => serde_json::to_value(ocel).map_err(|e| e.to_string()),
            RegistryItem::IndexLinkedOCEL(locel) => {
                serde_json::to_value(locel).map_err(|e| e.to_string())
            }
            RegistryItem::SlimLinkedOCEL(locel) => {
                let ocel = locel.construct_ocel();
                serde_json::to_value(ocel).map_err(|e| e.to_string())
            }
            RegistryItem::EventLogActivityProjection(proj) => {
                serde_json::to_value(proj).map_err(|e| e.to_string())
            }
            // Serialising a whole database into JSON would be a mistake, not a courtesy.
            RegistryItem::TabularSource(src) => Ok(serde_json::json!({
                "format": src.format(),
                "bytes": src.bytes().len(),
            })),
            RegistryItem::Custom(v) => v.to_value(),
        }
    }

    /// Try to load a registry item from a file path based on the expected type name
    ///
    /// Takes the same `xes`/`xes.gz` route as [`RegistryItem::load_from_bytes`], on the format
    /// inferred from the path.
    pub fn load_from_path(item_kind: &RegistryItemKind, path: &str) -> Result<Self, String> {
        let path = std::path::Path::new(path);

        #[cfg(feature = "extraction-blueprint")]
        if crate::core::io::infer_format_from_path(path)
            .is_some_and(|format| reads_as_case_centric(item_kind, &format))
        {
            let log = EventLog::import_from_path(path).map_err(|e| e.to_string())?;
            return case_centric_as(item_kind, &log);
        }

        match item_kind {
            RegistryItemKind::EventLog => Ok(RegistryItem::EventLog(
                EventLog::import_from_path(path).map_err(|e| e.to_string())?,
            )),
            RegistryItemKind::OCEL => Ok(RegistryItem::OCEL(
                OCEL::import_from_path(path).map_err(|e| e.to_string())?,
            )),
            RegistryItemKind::SlimLinkedOCEL => Ok(RegistryItem::SlimLinkedOCEL({
                SlimLinkedOCEL::import_from_path(path).map_err(|e| e.to_string())?
            })),
            RegistryItemKind::IndexLinkedOCEL => Ok(RegistryItem::IndexLinkedOCEL(
                IndexLinkedOCEL::import_from_path(path).map_err(|e| e.to_string())?,
            )),
            RegistryItemKind::EventLogActivityProjection => {
                Ok(RegistryItem::EventLogActivityProjection(
                    EventLogActivityProjection::import_from_path(path)
                        .map_err(|e| e.to_string())?,
                ))
            }
            RegistryItemKind::TabularSource => Ok(RegistryItem::TabularSource(
                TabularSource::import_from_path(path).map_err(|e| e.to_string())?,
            )),
            RegistryItemKind::Custom(name) => {
                let info = custom_kind(name).ok_or_else(|| unregistered_kind_msg(name))?;
                (info.from_path)(path)
            }
        }
    }

    /// Try to load a registry item from bytes based on the expected type name and format
    ///
    /// The object-centric kinds also accept `xes`/`xes.gz`, which no OCEL reader handles: the
    /// bytes are parsed as a case-centric [`EventLog`] and converted with
    /// [`write_event_log_to_sink`](crate::core::event_data::object_centric::extraction::write_event_log_to_sink),
    /// one object per case. Only with `extraction-blueprint`, which is what brings the sink the
    /// conversion is written against.
    pub fn load_from_bytes(
        item_kind: &RegistryItemKind,
        data: &[u8],
        format: &str,
    ) -> Result<Self, String> {
        #[cfg(feature = "extraction-blueprint")]
        if reads_as_case_centric(item_kind, format) {
            let log = EventLog::import_from_bytes(data, format).map_err(|e| e.to_string())?;
            return case_centric_as(item_kind, &log);
        }
        match item_kind {
            RegistryItemKind::EventLog => Ok(RegistryItem::EventLog(
                EventLog::import_from_bytes(data, format).map_err(|e| e.to_string())?,
            )),
            RegistryItemKind::OCEL => Ok(RegistryItem::OCEL(
                OCEL::import_from_bytes(data, format).map_err(|e| e.to_string())?,
            )),
            RegistryItemKind::IndexLinkedOCEL => Ok(RegistryItem::IndexLinkedOCEL(
                IndexLinkedOCEL::import_from_bytes(data, format).map_err(|e| e.to_string())?,
            )),
            RegistryItemKind::SlimLinkedOCEL => Ok(RegistryItem::SlimLinkedOCEL({
                OCEL::import_from_bytes(data, format)
                    .map(SlimLinkedOCEL::from_ocel)
                    .map_err(|e| e.to_string())?
            })),
            RegistryItemKind::EventLogActivityProjection => {
                Ok(RegistryItem::EventLogActivityProjection(
                    EventLogActivityProjection::import_from_bytes(data, format)
                        .map_err(|e| e.to_string())?,
                ))
            }
            RegistryItemKind::TabularSource => Ok(RegistryItem::TabularSource(
                TabularSource::import_from_bytes(data, format).map_err(|e| e.to_string())?,
            )),
            RegistryItemKind::Custom(name) => {
                let info = custom_kind(name).ok_or_else(|| unregistered_kind_msg(name))?;
                (info.from_bytes)(data, format)
            }
        }
    }

    /// Try to build a registry item from the JSON form of the given kind.
    ///
    /// The inverse of [`RegistryItem::to_value`] rather than a second JSON dialect: what that
    /// method writes for a kind is what this method reads back for it.
    ///
    /// # Errors
    /// Returns the deserializer's message, or an explanation for a kind that has no JSON form.
    pub fn from_json_value(item_kind: &RegistryItemKind, value: &Value) -> Result<Self, String> {
        match item_kind {
            RegistryItemKind::EventLog => serde_json::from_value(value.clone())
                .map(RegistryItem::EventLog)
                .map_err(|e| e.to_string()),
            RegistryItemKind::OCEL => serde_json::from_value(value.clone())
                .map(RegistryItem::OCEL)
                .map_err(|e| e.to_string()),
            RegistryItemKind::IndexLinkedOCEL => serde_json::from_value(value.clone())
                .map(RegistryItem::IndexLinkedOCEL)
                .map_err(|e| e.to_string()),
            RegistryItemKind::EventLogActivityProjection => serde_json::from_value(value.clone())
                .map(RegistryItem::EventLogActivityProjection)
                .map_err(|e| e.to_string()),
            // Mirrors `to_value`, which hands out the constructed OCEL: the linked form itself is
            // not `Deserialize`, so the OCEL is the JSON form of both directions.
            RegistryItemKind::SlimLinkedOCEL => serde_json::from_value::<OCEL>(value.clone())
                .map(|ocel| RegistryItem::SlimLinkedOCEL(SlimLinkedOCEL::from_ocel(ocel)))
                .map_err(|e| e.to_string()),
            // `to_value` only reports a source's format and size, which is nothing to rebuild from.
            RegistryItemKind::TabularSource => Err("a data source has no JSON form".to_string()),
            RegistryItemKind::Custom(name) => {
                let info = custom_kind(name).ok_or_else(|| unregistered_kind_msg(name))?;
                (info.from_value)(value)
            }
        }
    }

    /// Get the kind of the registry item
    pub fn kind(&self) -> RegistryItemKind {
        match self {
            RegistryItem::EventLogActivityProjection(_) => {
                RegistryItemKind::EventLogActivityProjection
            }
            RegistryItem::IndexLinkedOCEL(_) => RegistryItemKind::IndexLinkedOCEL,
            RegistryItem::EventLog(_) => RegistryItemKind::EventLog,
            RegistryItem::OCEL(_) => RegistryItemKind::OCEL,
            RegistryItem::SlimLinkedOCEL(_) => RegistryItemKind::SlimLinkedOCEL,
            RegistryItem::TabularSource(_) => RegistryItemKind::TabularSource,
            RegistryItem::Custom(v) => RegistryItemKind::Custom(v.kind()),
        }
    }

    /// Export the registry item to a file path, in the format the path names.
    ///
    /// Only the inference happens here, since each kind reads a path by its own rule (`.csv`
    /// means `ocel.csv` for an OCEL, a directory means the bundled format). The writing itself is
    /// [`RegistryItem::export_to_path_as`].
    pub fn export_to_path(&self, path: impl AsRef<std::path::Path>) -> Result<(), String> {
        let path = path.as_ref();
        let inferred = match self {
            RegistryItem::EventLog(_) => <EventLog as Exportable>::infer_format(path),
            RegistryItem::OCEL(_) => <OCEL as Exportable>::infer_format(path),
            RegistryItem::IndexLinkedOCEL(_) => <IndexLinkedOCEL as Exportable>::infer_format(path),
            RegistryItem::SlimLinkedOCEL(_) => <SlimLinkedOCEL as Exportable>::infer_format(path),
            RegistryItem::EventLogActivityProjection(_) => {
                <EventLogActivityProjection as Exportable>::infer_format(path)
            }
            // Neither kind has an inference rule of its own.
            RegistryItem::TabularSource(_) | RegistryItem::Custom(_) => {
                crate::core::io::infer_format_from_path(path)
            }
        };
        let format = inferred
            .ok_or_else(|| format!("Cannot infer format from path {}", path.to_string_lossy()))?;
        self.export_to_path_as(path, &format)
    }

    /// Export the registry item to a file path in an explicitly named format.
    ///
    /// Unlike [`RegistryItem::export_to_path`], the format is given rather than read off the
    /// path: a directory carries no extension, and the OCEL 2.0 bundled format's uncompressed
    /// form is a directory. This is also the route that avoids materialising the whole export in
    /// memory, which [`RegistryItem::export_to_bytes`] cannot.
    ///
    /// # Errors
    /// Returns the underlying exporter's message, or an explanation for a kind that has no
    /// file representation.
    pub fn export_to_path_as(
        &self,
        path: impl AsRef<std::path::Path>,
        format: &str,
    ) -> Result<(), String> {
        let path = path.as_ref();
        match self {
            RegistryItem::EventLog(x) => x
                .export_to_path_as(path, format, ())
                .map_err(|e| e.to_string()),
            RegistryItem::OCEL(x) => x
                .export_to_path_as(path, format, ())
                .map_err(|e| e.to_string()),
            RegistryItem::IndexLinkedOCEL(x) => x
                .export_to_path_as(path, format, ())
                .map_err(|e| e.to_string()),
            RegistryItem::SlimLinkedOCEL(x) => x
                .export_to_path_as(path, format, ())
                .map_err(|e| e.to_string()),
            RegistryItem::EventLogActivityProjection(x) => x
                .export_to_path_as(path, format, ())
                .map_err(|e| e.to_string()),
            RegistryItem::TabularSource(_) => Err("a data source cannot be exported".to_string()),
            RegistryItem::Custom(v) => {
                std::fs::write(path, v.export_to_bytes(format)?).map_err(|e| e.to_string())
            }
        }
    }

    /// Export the registry item to a byte vector
    pub fn export_to_bytes(&self, format: &str) -> Result<Vec<u8>, String> {
        let mut bytes = Vec::new();
        match self {
            RegistryItem::EventLog(x) => x
                .export_to_writer(&mut bytes, format)
                .map_err(|e| e.to_string())?,
            RegistryItem::OCEL(x) => x
                .export_to_writer(&mut bytes, format)
                .map_err(|e| e.to_string())?,
            RegistryItem::SlimLinkedOCEL(x) => x
                .construct_ocel()
                .export_to_writer(&mut bytes, format)
                .map_err(|e| e.to_string())?,
            RegistryItem::IndexLinkedOCEL(x) => x
                .export_to_writer(&mut bytes, format)
                .map_err(|e| e.to_string())?,
            RegistryItem::EventLogActivityProjection(x) => x
                .export_to_writer(&mut bytes, format)
                .map_err(|e| e.to_string())?,
            RegistryItem::TabularSource(_) => {
                return Err("a data source cannot be exported".to_string())
            }
            RegistryItem::Custom(x) => return x.export_to_bytes(format),
        };
        Ok(bytes)
    }

    /// Convert the registry item to another kind
    pub fn convert(&self, target_kind: RegistryItemKind) -> Result<Self, String> {
        match (self, target_kind) {
            (RegistryItem::EventLog(log), RegistryItemKind::EventLogActivityProjection) => {
                Ok(RegistryItem::EventLogActivityProjection(log.into()))
            }
            (RegistryItem::OCEL(ocel), RegistryItemKind::IndexLinkedOCEL) => Ok(
                RegistryItem::IndexLinkedOCEL(IndexLinkedOCEL::from_ocel(ocel.clone())),
            ),
            (RegistryItem::IndexLinkedOCEL(locel), RegistryItemKind::OCEL) => {
                Ok(RegistryItem::OCEL(locel.get_ocel_ref().clone()))
            }
            (RegistryItem::SlimLinkedOCEL(locel), RegistryItemKind::OCEL) => {
                Ok(RegistryItem::OCEL(locel.construct_ocel()))
            }
            (RegistryItem::OCEL(ocel), RegistryItemKind::SlimLinkedOCEL) => Ok(
                RegistryItem::SlimLinkedOCEL(SlimLinkedOCEL::from_ocel(ocel.clone())),
            ),
            _ => Err(format!("Cannot convert {} to {}", self.kind(), target_kind)),
        }
    }
}

/// The name of a custom value's kind, reachable through `dyn CustomRegistryValue`.
///
/// Blanket-implemented from [`CustomRegistryValue::kind_name`]. There is nothing to write by hand.
/// It exists because `kind_name` is `where Self: Sized`, as an associated function has to be, or
/// [`CustomRegistryValue`] would not be dyn-compatible and could not be boxed into
/// [`RegistryItem::Custom`].
pub trait CustomRegistryKind {
    /// Name this value's kind is known by.
    fn kind(&self) -> &'static str;
}

impl<T: CustomRegistryValue> CustomRegistryKind for T {
    fn kind(&self) -> &'static str {
        T::kind_name()
    }
}

/// A registry handle type owned by a downstream crate.
///
/// Implement this, then a `#[bind(handle)]` argument or a `#[register_binding(returns_handle)]`
/// return of that type crosses the binding boundary as a registry id instead of being serialized
/// as JSON, the same treatment the built-in big types get but without an entry in the macro
/// crate's list of them. Derive [`macro@CustomRegistryEntity`] to accept it as a `#[bind(handle)]`
/// argument.
///
/// Registering the type with [`crate::register_custom_registry_kind!`] is optional and adds what
/// needs a name rather than a Rust type: [`RegistryItemKind::from_str`](std::str::FromStr::from_str)
/// resolution, [`RegistryItem::load_from_path`] / [`RegistryItem::load_from_bytes`], and the
/// format lists. Unregistered, a value still stores, resolves by id and exports.
///
/// `Send + Sync` are load-bearing: the registry lives in a `static` in the wasm hosts, so
/// [`AppState`] has to stay `Sync`.
pub trait CustomRegistryValue:
    CustomRegistryKind + std::any::Any + Send + Sync + std::fmt::Debug
{
    /// Name this kind is known by in ids, in `x-registry-ref` and in [`RegistryItemKind::Custom`].
    fn kind_name() -> &'static str
    where
        Self: Sized;

    /// JSON projection of the value, as [`RegistryItem::to_value`] gives for a built-in kind.
    fn to_value(&self) -> Result<Value, String>;

    /// Rebuild the value from the JSON form [`CustomRegistryValue::to_value`] produces.
    fn from_value(_value: &Value) -> Result<Self, String>
    where
        Self: Sized,
    {
        Err(format!("{} cannot be read from JSON", Self::kind_name()))
    }

    /// Read the value from bytes in the named format.
    fn from_bytes(_bytes: &[u8], format: &str) -> Result<Self, String>
    where
        Self: Sized,
    {
        Err(format!(
            "{} cannot be read from '{}' bytes",
            Self::kind_name(),
            format
        ))
    }

    /// Read the value from a file, defaulting to [`CustomRegistryValue::from_bytes`] with the
    /// format inferred from the extension.
    fn from_path(path: &std::path::Path) -> Result<Self, String>
    where
        Self: Sized,
    {
        let format = crate::core::io::infer_format_from_path(path)
            .ok_or_else(|| format!("Cannot infer format from path {}", path.to_string_lossy()))?;
        let bytes = std::fs::read(path).map_err(|e| e.to_string())?;
        Self::from_bytes(&bytes, &format)
    }

    /// Write the value out in the named format.
    fn export_to_bytes(&self, format: &str) -> Result<Vec<u8>, String> {
        Err(format!(
            "{} cannot be exported as '{}'",
            self.kind(),
            format
        ))
    }

    /// Formats [`CustomRegistryValue::from_bytes`] accepts.
    fn known_import_formats() -> Vec<ExtensionWithMime>
    where
        Self: Sized,
    {
        Vec::new()
    }

    /// Formats [`CustomRegistryValue::export_to_bytes`] produces.
    fn known_export_formats() -> Vec<ExtensionWithMime>
    where
        Self: Sized,
    {
        Vec::new()
    }
}

/// A custom kind registered by a downstream crate, so its name resolves back to its loaders.
///
/// Submitted by [`crate::register_custom_registry_kind!`], not constructed by hand.
#[derive(Debug)]
pub struct CustomKindInfo {
    /// The kind name, matching [`CustomRegistryValue::kind_name`].
    pub name: &'static str,
    /// [`CustomRegistryValue::from_path`], wrapped into a [`RegistryItem`].
    pub from_path: fn(&std::path::Path) -> Result<RegistryItem, String>,
    /// [`CustomRegistryValue::from_bytes`], wrapped into a [`RegistryItem`].
    pub from_bytes: fn(&[u8], &str) -> Result<RegistryItem, String>,
    /// [`CustomRegistryValue::from_value`], wrapped into a [`RegistryItem`].
    pub from_value: fn(&Value) -> Result<RegistryItem, String>,
    /// [`CustomRegistryValue::known_import_formats`].
    pub import_formats: fn() -> Vec<ExtensionWithMime>,
    /// [`CustomRegistryValue::known_export_formats`].
    pub export_formats: fn() -> Vec<ExtensionWithMime>,
}
inventory::collect!(CustomKindInfo);

/// The registered custom kind called `name`, if there is one.
pub fn custom_kind(name: &str) -> Option<&'static CustomKindInfo> {
    inventory::iter::<CustomKindInfo>
        .into_iter()
        .find(|c| c.name == name)
}

/// All custom kinds registered by downstream crates.
pub fn custom_kinds() -> Vec<&'static CustomKindInfo> {
    inventory::iter::<CustomKindInfo>.into_iter().collect()
}

fn unregistered_kind_msg(name: &str) -> String {
    format!(
        "'{}' is not a registered custom kind (see register_custom_registry_kind!)",
        name
    )
}

#[doc(hidden)]
pub fn __custom_from_path<T: CustomRegistryValue>(
    path: &std::path::Path,
) -> Result<RegistryItem, String> {
    T::from_path(path).map(RegistryItem::custom)
}
#[doc(hidden)]
pub fn __custom_from_bytes<T: CustomRegistryValue>(
    bytes: &[u8],
    format: &str,
) -> Result<RegistryItem, String> {
    T::from_bytes(bytes, format).map(RegistryItem::custom)
}
#[doc(hidden)]
pub fn __custom_from_value<T: CustomRegistryValue>(value: &Value) -> Result<RegistryItem, String> {
    T::from_value(value).map(RegistryItem::custom)
}
#[doc(hidden)]
pub fn __custom_import_formats<T: CustomRegistryValue>() -> Vec<ExtensionWithMime> {
    T::known_import_formats()
}
#[doc(hidden)]
pub fn __custom_export_formats<T: CustomRegistryValue>() -> Vec<ExtensionWithMime> {
    T::known_export_formats()
}

/// Register a [`CustomRegistryValue`] implementor so its kind name resolves to its loaders.
///
/// The name defaults to the type's own tokens, so pass it explicitly for a path or a generic
/// instantiation:
///
/// ```ignore
/// register_custom_registry_kind!(MyHandle);
/// register_custom_registry_kind!(some::module::MyHandle, "MyHandle");
/// ```
///
/// The name given here has to match [`CustomRegistryValue::kind_name`], which is what
/// [`RegistryItem::kind`] reports.
#[macro_export]
macro_rules! register_custom_registry_kind {
    ($t:ty) => {
        $crate::register_custom_registry_kind!($t, ::core::stringify!($t));
    };
    ($t:ty, $name:expr) => {
        $crate::__private::inventory::submit! {
            $crate::bindings::CustomKindInfo {
                name: $name,
                from_path: $crate::bindings::__custom_from_path::<$t>,
                from_bytes: $crate::bindings::__custom_from_bytes::<$t>,
                from_value: $crate::bindings::__custom_from_value::<$t>,
                import_formats: $crate::bindings::__custom_import_formats::<$t>,
                export_formats: $crate::bindings::__custom_export_formats::<$t>,
            }
        }
    };
}

/// Inner App State
pub type InnerAppState = HashMap<String, RegistryItem>;

/// Read-only access to the registry from inside a binding, requested with `#[bind(state)]`.
///
/// For a binding that must look up items it is given the ids of, such as an extraction naming one
/// source per id. Every other argument arrives as JSON, and a big type arrives already resolved.
///
/// Not `&AppState`: that owns the `RwLock`, and the caller already holds a guard when the body
/// runs. `std::sync::RwLock` is not reentrant, so locking again would deadlock.
///
/// Read-only, and only on bindings with no `&mut` big-type argument. See the macro's own check.
#[derive(Debug, Clone, Copy)]
pub struct StateRef<'a> {
    items: &'a InnerAppState,
}

impl<'a> StateRef<'a> {
    /// Wrap the already-locked registry. Called by `#[register_binding]`, not by hand.
    #[must_use]
    pub fn new(items: &'a InnerAppState) -> Self {
        Self { items }
    }

    /// The item stored under `id`, if any.
    #[must_use]
    pub fn get(&self, id: &str) -> Option<&'a RegistryItem> {
        self.items.get(id)
    }

    /// Whether `id` names a stored item.
    #[must_use]
    pub fn contains(&self, id: &str) -> bool {
        self.items.contains_key(id)
    }
}

/// Writable access to the whole registry, requested with `#[bind(state_mut)]`.
///
/// For a binding that manages the registry itself rather than one resolved item, e.g. evicting
/// items to free memory. A `#[bind(handle)]` or `&mut` big-type
/// argument is still the right tool for "mutate the one item named by this parameter"; reach for
/// `state_mut` only when the set of items to touch is not known until the body runs.
///
/// Not `&mut AppState`, for the same reason [`StateRef`] is not `&AppState`: the caller already
/// holds the write guard, and `std::sync::RwLock` is not reentrant.
///
/// The macro's own check makes this the only argument that can ask for the write lock on a
/// binding that takes it. See [`StateRef`]'s docs for why a shared reference to the same registry
/// cannot coexist with it.
#[derive(Debug)]
pub struct StateRefMut<'a> {
    items: &'a mut InnerAppState,
}

impl<'a> StateRefMut<'a> {
    /// Wrap the already-locked registry. Called by `#[register_binding]`, not by hand.
    #[must_use]
    pub fn new(items: &'a mut InnerAppState) -> Self {
        Self { items }
    }

    /// The item stored under `id`, if any.
    #[must_use]
    pub fn get(&self, id: &str) -> Option<&RegistryItem> {
        self.items.get(id)
    }

    /// The item stored under `id`, mutably, if any.
    #[must_use]
    pub fn get_mut(&mut self, id: &str) -> Option<&mut RegistryItem> {
        self.items.get_mut(id)
    }

    /// Whether `id` names a stored item.
    #[must_use]
    pub fn contains(&self, id: &str) -> bool {
        self.items.contains_key(id)
    }

    /// Drop the item stored under `id`, returning it if there was one.
    pub fn remove(&mut self, id: &str) -> Option<RegistryItem> {
        self.items.remove(id)
    }

    /// Drop every item in the registry.
    pub fn clear(&mut self) {
        self.items.clear();
    }

    /// How many items are currently stored.
    #[must_use]
    pub fn len(&self) -> usize {
        self.items.len()
    }

    /// Whether the registry currently holds no items.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.items.is_empty()
    }

    /// The id of every stored item, in no particular order.
    pub fn ids(&self) -> impl Iterator<Item = &String> {
        self.items.keys()
    }
}

/// State that can store 'big' types
#[derive(Debug, Default)]
pub struct AppState {
    /// Stored items
    pub items: RwLock<InnerAppState>,
}
impl AppState {
    /// The registry for reading, recovering from a poisoned lock.
    ///
    /// A binding body that panics leaves the lock poisoned, but the registry is a plain map no
    /// half-finished insert can leave inconsistent, so refusing every later call would be worse.
    pub fn read(&self) -> std::sync::RwLockReadGuard<'_, InnerAppState> {
        self.items
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// The registry for writing. Recovers from a poisoned lock, see [`AppState::read`].
    pub fn write(&self) -> std::sync::RwLockWriteGuard<'_, InnerAppState> {
        self.items
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// Add the passed registry item
    pub fn add(&self, id: impl Into<String>, item: impl Into<RegistryItem>) {
        self.write().insert(id.into(), item.into());
    }
    /// Drop the item stored under `id`, returning it if there was one.
    pub fn remove(&self, id: &str) -> Option<RegistryItem> {
        self.write().remove(id)
    }
    /// Check if the state contains the passed key
    pub fn contains_key(&self, id: &str) -> bool {
        self.read().contains_key(id)
    }
}

/// Function Binding
#[derive(Debug)]
pub struct Binding {
    /// Unique ID of the function
    pub id: &'static str,
    /// Name of the function
    pub name: &'static str,
    /// Function handler (executing the function with (de-)serializing inputs/outputs).
    /// Returns the result pre-serialized as UTF-8 JSON bytes.
    pub handler: fn(&Value, &AppState) -> Result<Vec<u8>, String>,
    /// Documentation of function
    pub docs: fn() -> Vec<String>,
    /// Module path of declared function
    pub module: &'static str,
    /// File path of declared function
    pub source_path: &'static str,
    /// Line number of function in `source_path`
    pub source_line: u32,
    /// Get arguments of the function with the corresponding JSON schema
    pub args: fn() -> Vec<(String, Value)>,
    /// Get a list of all required arguments
    pub required_args: fn() -> Vec<String>,
    /// JSON Schema of return type
    pub return_type: fn() -> Value,
}
inventory::collect!(Binding);

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
/// Metadata of a function binding
pub struct BindingMeta {
    /// Unique ID of the function
    pub id: String,
    /// Name of the function
    pub name: String,
    /// Documentation of function
    pub docs: Vec<String>,
    /// Module path of declared function
    pub module: String,
    /// File path of declared function
    pub source_path: String,
    /// Line number of function in `source_path`
    pub source_line: u32,
    /// Get arguments of the function with the corresponding JSON schema
    pub args: Vec<(String, Value)>,
    /// Get a list of all required arguments
    pub required_args: Vec<String>,
    /// JSON Schema of return type
    pub return_type: Value,
}

impl From<&Binding> for BindingMeta {
    fn from(value: &Binding) -> Self {
        Self {
            id: value.id.to_string(),
            name: value.name.to_string(),
            docs: (value.docs)(),
            module: value.module.to_string(),
            source_path: value.source_path.to_string(),
            source_line: value.source_line,
            args: (value.args)(),
            required_args: (value.required_args)(),
            return_type: (value.return_type)(),
        }
    }
}

// Helper functions

/// Derive Value from Context
pub trait FromContext<'a>: Sized {
    /// Get value from context
    fn from_context(v: &Value, s: &'a InnerAppState) -> Result<Self, String>;
}

/// Try to extract function args (used in macro)
pub fn extract_param<'a, T: FromContext<'a>>(
    m: &serde_json::Map<String, Value>,
    k: &str,
    s: &'a InnerAppState,
    default: impl FnOnce() -> Option<T>,
) -> Result<T, String> {
    if let Some(x) = m.get(k) {
        // If argument is null in JSON, check if a default is given
        // when yes: Use that, otherwise, fallback to standard parsing
        if x.is_null() {
            let d = default();
            if let Some(d) = d {
                return Ok(d);
            }
        }
        T::from_context(x, s).map_err(|e| format!("Invalid Argument: {k}\n{e}"))
    } else {
        let r = default();
        r.ok_or_else(|| format!("Missing required argument {k}"))
    }
}

/// Extract a JSON-deserializable parameter without requiring state access.
///
/// Used by the `#[register_binding]` macro for functions with `&mut` big type parameters,
/// where non-big-type arguments are extracted before acquiring the write lock.
pub fn extract_param_json<T: serde::de::DeserializeOwned>(
    m: &serde_json::Map<String, Value>,
    k: &str,
    default: impl FnOnce() -> Option<T>,
) -> Result<T, String> {
    if let Some(x) = m.get(k) {
        if x.is_null() {
            if let Some(d) = default() {
                return Ok(d);
            }
        }
        serde_json::from_value(x.clone()).map_err(|e| format!("Invalid Argument: {k}\n{e}"))
    } else {
        default().ok_or_else(|| format!("Missing required argument {k}"))
    }
}

// Runtime Extraction
// If a type is Deserialize, we can extract it from JSON.
impl<'a, T> FromContext<'a> for T
where
    T: serde::de::DeserializeOwned,
{
    fn from_context(v: &Value, _: &'a InnerAppState) -> Result<Self, String> {
        serde_json::from_value(v.clone()).map_err(|e| e.to_string())
    }
}

/// The forms a registry-reference argument can take, see [`resolve_argument`].
enum HandleArg<'a> {
    /// A bare string: an id if the registry knows it, a file path otherwise.
    Bare(&'a str),
    Id(&'a str),
    Path(&'a str),
    Bytes {
        b64: &'a str,
        format: &'a str,
    },
    Inline(&'a Value),
}

/// Decide which form a registry-reference argument is in, or `None` for a value that is none of
/// them and is left alone.
///
/// An object is a wrapper only when its key set is exactly one of the recognised ones and the
/// recognised keys hold strings. Both halves matter: a real OCEL or `EventLog` object carries
/// dozens of keys and so can never be mistaken for a wrapper, and a domain object that happens to
/// be `{"bytes": 5}` is read as itself rather than reported as a malformed wrapper.
fn classify_handle_arg(value: &Value) -> Option<HandleArg<'_>> {
    match value {
        Value::String(s) => Some(HandleArg::Bare(s)),
        Value::Object(map) => {
            let string_field = |k: &str| map.get(k).and_then(Value::as_str);
            match map.len() {
                1 => {
                    if let Some(id) = string_field("id") {
                        Some(HandleArg::Id(id))
                    } else if let Some(path) = string_field("path") {
                        Some(HandleArg::Path(path))
                    } else if let Some(inner) = map.get("inline") {
                        Some(HandleArg::Inline(inner))
                    } else {
                        Some(HandleArg::Inline(value))
                    }
                }
                2 => match (string_field("bytes"), string_field("format")) {
                    (Some(b64), Some(format)) => Some(HandleArg::Bytes { b64, format }),
                    _ => Some(HandleArg::Inline(value)),
                },
                _ => Some(HandleArg::Inline(value)),
            }
        }
        Value::Array(_) => Some(HandleArg::Inline(value)),
        // Notably `null`, which has to reach `extract_param` untouched for `#[bind(default)]`
        // to still apply.
        _ => None,
    }
}

/// Accepts both the padded and the unpadded encoding: which of the two a host emits is not
/// something the caller should have to know.
fn decode_base64(s: &str) -> Result<Vec<u8>, String> {
    use base64::Engine;
    const ENGINE: base64::engine::GeneralPurpose = base64::engine::GeneralPurpose::new(
        &base64::alphabet::STANDARD,
        base64::engine::GeneralPurposeConfig::new()
            .with_decode_padding_mode(base64::engine::DecodePaddingMode::Indifferent),
    );
    ENGINE.decode(s).map_err(|e| e.to_string())
}

/// Resolve a string that names a stored item, returning `None` if the registry does not know it.
///
/// Converts on a kind mismatch, storing the result under `{id}_as_{arg_ref}`.
fn resolve_stored_id(id: &str, arg_ref: &str, state: &AppState) -> Result<Option<Value>, String> {
    // An id of the right kind inserts nothing, and taking the write lock for it would serialise
    // every argument of every call against all other writers.
    {
        let items = state.read();
        match items.get(id) {
            None => return Ok(None),
            Some(item) if item.kind().to_string() == arg_ref => {
                return Ok(Some(Value::String(id.to_string())))
            }
            Some(_) => {}
        }
    }

    let mut items = state.write();
    let Some(item) = items.get(id) else {
        return Ok(None);
    };
    if item.kind().to_string() == arg_ref {
        return Ok(Some(Value::String(id.to_string())));
    }

    // Try conversion
    let target_kind = RegistryItemKind::from_str(arg_ref)?;
    match item.convert(target_kind) {
        Ok(converted) => {
            let new_id = format!("{}_as_{}", id, arg_ref);
            items.insert(new_id.clone(), converted);
            Ok(Some(Value::String(new_id)))
        }
        Err(e) => Err(format!(
            "Type mismatch for ID '{}': expected {}, found {}. Conversion failed: {}",
            id,
            arg_ref,
            item.kind(),
            e
        )),
    }
}

/// Resolve an argument value based on its schema and the current state.
///
/// This function handles:
/// 1. Materialising "Big Types" if the schema indicates a registry reference, from any of the
///    forms below. Everything but the id itself is stored, and the new id is what comes back, so
///    a caller always ends up with a plain string id.
/// 2. Loading JSON objects from files if the value is a path ending in `.json`.
/// 3. Parsing JSON strings if the value is a string but the schema expects an object/array.
///
/// The registry-reference forms are:
///
/// | value | meaning |
/// |---|---|
/// | `"log1"` | a stored id, or else a file path |
/// | `{"id": "log1"}` | a stored id only, never the filesystem |
/// | `{"path": "/a/b.xes.gz"}` | a file path only, never a stored id |
/// | `{"bytes": "<base64>", "format": "xes.gz"}` | the bytes themselves, for a host with no filesystem |
/// | `{"inline": <json>}` | the JSON form of the item, as [`RegistryItem::to_value`] writes it |
/// | any other object or array | the JSON form, unwrapped |
///
/// The two id forms look up an item that already exists, so a mismatched kind is converted where
/// a conversion exists. The other forms have no kind to mismatch: the path, the bytes and the JSON
/// are read as the referenced kind directly.
pub fn resolve_argument(
    arg_name: &str,
    value: Value,
    schema: &Value,
    state: &AppState,
) -> Result<Value, String> {
    resolve_argument_tracked(arg_name, value, schema, state, &mut Vec::new())
}

/// [`resolve_argument`], pushing every id it stores an item under onto `minted`.
///
/// Those ids never reach the caller, so only the requester of the resolution can drop them again.
fn resolve_argument_tracked(
    arg_name: &str,
    value: Value,
    schema: &Value,
    state: &AppState,
    minted: &mut Vec<String>,
) -> Result<Value, String> {
    let schema_obj = schema.as_object().ok_or("Invalid schema")?;

    // Case 1: Registry Reference
    if let Some(arg_ref) = schema_obj.get("x-registry-ref").and_then(|r| r.as_str()) {
        if let Some(handle_arg) = classify_handle_arg(&value) {
            let invalid = |e: String| format!("Invalid Argument: {}\n{}", arg_name, e);
            // A kind that no downstream crate registered still resolves by id, so this is looked
            // up only where a loader is actually needed.
            let target_kind = || RegistryItemKind::from_str(arg_ref).map_err(invalid);
            let item = match handle_arg {
                HandleArg::Bare(id) => {
                    if let Some(resolved) = resolve_stored_id(id, arg_ref, state)? {
                        return Ok(resolved);
                    }
                    // Otherwise, try to load it from file
                    RegistryItem::load_from_path(&RegistryItemKind::from_str(arg_ref)?, id)?
                }
                HandleArg::Id(id) => {
                    return resolve_stored_id(id, arg_ref, state)?.ok_or_else(|| {
                        invalid(format!("No {} is stored under the ID '{}'", arg_ref, id))
                    })
                }
                HandleArg::Path(path) => {
                    RegistryItem::load_from_path(&target_kind()?, path).map_err(invalid)?
                }
                HandleArg::Bytes { b64, format } => {
                    let bytes = decode_base64(b64)
                        .map_err(|e| invalid(format!("'bytes' is not valid base64: {}", e)))?;
                    RegistryItem::load_from_bytes(&target_kind()?, &bytes, format)
                        .map_err(invalid)?
                }
                HandleArg::Inline(inner) => {
                    RegistryItem::from_json_value(&target_kind()?, inner).map_err(invalid)?
                }
            };
            let stored_name = format!("A{}_{}", arg_name, uuid::Uuid::new_v4());
            state.add(&stored_name, item);
            minted.push(stored_name.clone());
            return Ok(serde_json::Value::String(stored_name));
        }
    }

    // Case 2: Load JSON from file
    if let Some(val_str) = value.as_str() {
        if schema_obj.get("type") == Some(&serde_json::json!("object"))
            && val_str.ends_with(".json")
        {
            let file = std::fs::File::open(val_str)
                .map_err(|e| format!("Failed to open JSON file: {}", e))?;
            let reader = std::io::BufReader::new(file);
            let loaded_val: Value = serde_json::from_reader(reader)
                .map_err(|e| format!("Failed to parse JSON file: {}", e))?;
            return Ok(loaded_val);
        }
    }

    // Case 3: Parse JSON string (if needed)
    // If the schema expects an object/array but we got a string, try to parse it.
    if let Some(val_str) = value.as_str() {
        let type_field = schema_obj.get("type").and_then(|t| t.as_str());
        if matches!(type_field, Some("object") | Some("array")) {
            if let Ok(parsed) = serde_json::from_str::<Value>(val_str) {
                return Ok(parsed);
            }
        }
    }

    Ok(value)
}

/// Call the specified function with the passed arguments.
/// Returns the result pre-serialized as UTF-8 JSON bytes.
///
/// A panic in the binding body is caught and reported as an error, because hosts reach this
/// across an FFI or wasm boundary, where an unwind is undefined behaviour.
pub fn call(binding: &Binding, args: &Value, state: &AppState) -> Result<Vec<u8>, String> {
    let called = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        (binding.handler)(args, state)
    }));
    called.unwrap_or_else(|payload| {
        let what = payload
            .downcast_ref::<&str>()
            .map(|s| (*s).to_string())
            .or_else(|| payload.downcast_ref::<String>().cloned())
            .unwrap_or_else(|| "panicked".to_string());
        Err(format!("{} panicked: {}", binding.name, what))
    })
}

/// [`call`], with every argument first put through [`resolve_argument`].
///
/// This accepts a registry reference given as anything other than a stored id: a path, base64
/// bytes, or the item inline. Arguments the binding does not declare are passed through
/// untouched.
///
/// Separate from [`call`] rather than folded into it: resolution reads files named by a plain
/// string argument, so a host exposing bindings to something it does not trust keeps the
/// choice.
pub fn call_resolved(binding: &Binding, args: &Value, state: &AppState) -> Result<Vec<u8>, String> {
    let Some(passed) = args.as_object() else {
        return call(binding, args, state);
    };
    let schemas = (binding.args)();
    let mut resolved = serde_json::Map::with_capacity(passed.len());
    let mut minted: Vec<String> = Vec::new();
    for (name, value) in passed {
        let value = match schemas.iter().find(|(n, _)| n == name) {
            Some((_, schema)) => {
                match resolve_argument_tracked(name, value.clone(), schema, state, &mut minted) {
                    Ok(value) => value,
                    Err(e) => {
                        drop_minted(state, &minted);
                        return Err(e);
                    }
                }
            }
            None => value.clone(),
        };
        resolved.insert(name.clone(), value);
    }
    let result = call(binding, &Value::Object(resolved), state);
    drop_minted(state, &minted);
    result
}

/// Drop the registry entries [`call_resolved`] stored for the duration of one call.
fn drop_minted(state: &AppState, minted: &[String]) {
    if minted.is_empty() {
        return;
    }
    let mut items = state.write();
    for id in minted {
        items.remove(id);
    }
}

/// Get a list of all functions available through bindings
pub fn list_functions() -> Vec<&'static Binding> {
    inventory::iter::<Binding>.into_iter().collect()
}
/// Get a list of all function metadata available through bindings
pub fn list_functions_meta() -> Vec<BindingMeta> {
    inventory::iter::<Binding>
        .into_iter()
        .map(BindingMeta::from)
        .collect()
}

/// Get the binding information of an function by its name
pub fn get_fn_binding(id: &str) -> Option<&'static Binding> {
    inventory::iter::<Binding>.into_iter().find(|b| b.id == id)
}

#[cfg(feature = "extraction-blueprint")]
mod extraction_bindings;
#[cfg(feature = "extraction-dbcon")]
mod extraction_dbcon_bindings;
mod path_schema_bindings;
mod slim_ocel_bindings;

/// Get the number of objects in an [`OCEL`]
#[register_binding]
pub fn num_objects<'a>(ocel: &'a impl LinkedOCELAccess<'a>) -> usize {
    ocel.get_num_obs()
}
/// Get the number of events in an [`OCEL`]
#[register_binding]
pub fn num_events<'a>(ocel: &'a impl LinkedOCELAccess<'a>) -> usize {
    ocel.get_num_evs()
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
/// Statistics on the event and object types of an OCEL
///
pub struct OCELTypeStats {
    /// Number of events per event type/activity
    pub event_type_counts: HashMap<String, usize>,
    /// Number of objects per object type
    pub object_type_counts: HashMap<String, usize>,
}
#[register_binding]
/// Compute statistics on object/event types in the OCEL
pub fn ocel_type_stats<'a>(ocel: &'a impl LinkedOCELAccess<'a>) -> OCELTypeStats {
    OCELTypeStats {
        event_type_counts: ocel
            .get_ev_types()
            .map(|et| (et.to_string(), ocel.get_evs_of_type(et).count()))
            .collect(),
        object_type_counts: ocel
            .get_ob_types()
            .map(|ot| (ot.to_string(), ocel.get_obs_of_type(ot).count()))
            .collect(),
    }
}

/// Convert an [`OCEL`] to an [`IndexLinkedOCEL`]
#[register_binding]
pub fn index_link_ocel(ocel: &OCEL) -> IndexLinkedOCEL {
    IndexLinkedOCEL::from_ocel(ocel.clone())
}

/// Convert an [`OCEL`] to an [`SlimLinkedOCEL`]
#[register_binding]
pub fn slim_link_ocel(ocel: &OCEL) -> SlimLinkedOCEL {
    SlimLinkedOCEL::from_ocel(ocel.clone())
}

#[register_binding]
/// This is a test function.
///
/// **This should be bold**, *this is italic*, `and this code`.
///
pub fn test_some_inputs(s: String, n: usize, i: i32, f: f64, b: bool) -> String {
    format!("s={},n={},i={},f={},b={}", s, n, i, f, b)
}

#[cfg(test)]
mod tests {
    use crate::test_utils::get_test_data_path;

    use super::*;
    use std::collections::HashSet;

    #[test]
    fn export_bindings() {
        let bindings = list_functions_meta();
        let file = std::fs::File::create(
            get_test_data_path()
                .join("export")
                .join(format!("bindings-v{}.json", env!("CARGO_PKG_VERSION"))),
        )
        .unwrap();
        serde_json::to_writer_pretty(&file, &bindings).unwrap();
    }

    #[test]
    fn test_consistent_registry_item_variants() {
        // Ensure that we have the expected variants
        let variants = RegistryItemKind::all_kinds();
        let variant_names: HashSet<String> = variants.iter().map(|v| v.to_string()).collect();

        // Get the list of types from the macro crate
        let macro_types: &[&str] = macros_process_mining::big_types_list!();
        let macro_type_names: HashSet<String> = macro_types.iter().map(|s| s.to_string()).collect();

        // Check for consistency
        // 1. All types in macro must be in RegistryItem
        for macro_type in &macro_type_names {
            assert!(
                variant_names.contains(macro_type),
                "Macro expects type '{}' which is missing in RegistryItem enum",
                macro_type
            );
        }

        // 2. All types in RegistryItem must be in macro
        for variant in &variant_names {
            assert!(
                macro_type_names.contains(variant),
                "RegistryItem has variant '{}' which is missing in macros_process_mining::BIG_TYPES_NAMES",
                variant
            );
        }

        assert_eq!(
            variant_names.len(),
            macro_type_names.len(),
            "Mismatch in number of types between RegistryItem and macros_process_mining"
        );
    }

    /// A handle type of the shape a downstream crate would define: not in `BIG_TYPES_NAMES`, not
    /// `Deserialize`, reached only through the registry.
    #[derive(Debug, Clone, PartialEq, CustomRegistryEntity)]
    struct DummyHandle {
        label: String,
        hits: usize,
    }

    impl CustomRegistryValue for DummyHandle {
        fn kind_name() -> &'static str {
            "DummyHandle"
        }
        fn to_value(&self) -> Result<Value, String> {
            Ok(serde_json::json!({ "label": self.label, "hits": self.hits }))
        }
        fn from_value(value: &Value) -> Result<Self, String> {
            Ok(DummyHandle {
                label: value["label"]
                    .as_str()
                    .ok_or("DummyHandle needs a string 'label'")?
                    .to_string(),
                hits: value["hits"].as_u64().unwrap_or(0) as usize,
            })
        }
        fn from_bytes(bytes: &[u8], format: &str) -> Result<Self, String> {
            if format != "txt" {
                return Err(format!("DummyHandle cannot be read from '{}'", format));
            }
            Ok(DummyHandle {
                label: String::from_utf8_lossy(bytes).to_string(),
                hits: 0,
            })
        }
        fn export_to_bytes(&self, format: &str) -> Result<Vec<u8>, String> {
            if format != "txt" {
                return Err(format!("DummyHandle cannot be written as '{}'", format));
            }
            Ok(self.label.as_bytes().to_vec())
        }
        fn known_import_formats() -> Vec<ExtensionWithMime> {
            vec![ExtensionWithMime::new("txt", "text/plain")]
        }
        fn known_export_formats() -> Vec<ExtensionWithMime> {
            vec![ExtensionWithMime::new("txt", "text/plain")]
        }
    }

    crate::register_custom_registry_kind!(DummyHandle);

    /// The label of a stored handle, taken by shared reference.
    #[register_binding]
    fn dummy_label(#[bind(handle)] h: &DummyHandle) -> String {
        h.label.clone()
    }

    /// Bump a stored handle's counter in place.
    #[register_binding]
    fn dummy_bump(#[bind(handle)] h: &mut DummyHandle) -> usize {
        h.hits += 1;
        h.hits
    }

    /// Build a new handle and hand back its registry id.
    #[register_binding(returns_handle)]
    fn dummy_new(label: String) -> DummyHandle {
        DummyHandle { label, hits: 0 }
    }

    /// Bump a stored handle and hand back a copy of it: a handle result stored through the write
    /// guard, which is a different insert site than [`dummy_new`]'s.
    #[register_binding(returns_handle)]
    fn dummy_fork(#[bind(handle)] h: &mut DummyHandle) -> DummyHandle {
        h.hits += 1;
        h.clone()
    }

    /// A big-type result stored through the write guard, the last of the four insert sites.
    #[register_binding]
    fn dummy_log_of(#[bind(handle)] h: &mut DummyHandle) -> EventLog {
        h.hits += 1;
        EventLog::default()
    }

    /// Drop every item in the registry: the `#[bind(state_mut)]` escape hatch, for a binding
    /// that manages the registry itself (evicting items under memory pressure, say) rather than
    /// mutating one item named by an argument.
    #[register_binding]
    fn dummy_clear_all(#[bind(state_mut)] mut state: StateRefMut<'_>) -> usize {
        let n = state.len();
        state.clear();
        n
    }

    fn binding_named(name: &str) -> &'static Binding {
        list_functions()
            .into_iter()
            .find(|b| b.name == name)
            .unwrap_or_else(|| panic!("no binding named {}", name))
    }

    #[test]
    fn custom_registry_kind_is_a_string_everywhere() {
        let custom = RegistryItemKind::Custom("DummyHandle");
        assert_eq!(custom.to_string(), "DummyHandle");
        assert_eq!("DummyHandle".parse::<RegistryItemKind>().unwrap(), custom);
        assert_eq!(
            serde_json::to_value(custom).unwrap(),
            serde_json::json!("DummyHandle")
        );
        assert_eq!(
            serde_json::from_value::<RegistryItemKind>(serde_json::json!("DummyHandle")).unwrap(),
            custom
        );
        // The built-in six keep the exact wire form the derive gave them.
        assert_eq!(
            serde_json::to_value(RegistryItemKind::OCEL).unwrap(),
            serde_json::json!("OCEL")
        );
        // An unregistered name is still an error, not a `Custom`.
        assert!("NoSuchKind".parse::<RegistryItemKind>().is_err());
        assert!(RegistryItemKind::all_registered_kinds().contains(&custom));
        assert_eq!(RegistryItemKind::all_kinds().len(), 6);
        assert_eq!(custom.known_import_formats().len(), 1);
        assert_eq!(custom.known_export_formats().len(), 1);
    }

    #[test]
    fn custom_registry_item_import_and_export() {
        let kind: RegistryItemKind = "DummyHandle".parse().unwrap();
        let item = RegistryItem::load_from_bytes(&kind, b"from-bytes", "txt").unwrap();
        assert_eq!(item.kind(), kind);
        assert_eq!(item.as_custom::<DummyHandle>().unwrap().label, "from-bytes");
        assert_eq!(item.export_to_bytes("txt").unwrap(), b"from-bytes");
        assert!(item.export_to_bytes("xes").is_err());

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("handle.txt");
        item.export_to_path(&path).unwrap();
        let reloaded = RegistryItem::load_from_path(&kind, path.to_str().unwrap()).unwrap();
        assert_eq!(
            reloaded.as_custom::<DummyHandle>().unwrap(),
            item.as_custom::<DummyHandle>().unwrap()
        );
    }

    #[test]
    fn custom_handle_crosses_the_binding_boundary() {
        let state = AppState::default();
        state.add(
            "d1",
            RegistryItem::custom(DummyHandle {
                label: "one".to_string(),
                hits: 0,
            }),
        );

        {
            let items = state.items.read().unwrap();
            let item = items.get("d1").unwrap();
            assert_eq!(item.kind(), RegistryItemKind::Custom("DummyHandle"));
            assert_eq!(item.as_custom::<DummyHandle>().unwrap().label, "one");
            assert!(RegistryItem::EventLog(EventLog::default())
                .as_custom::<DummyHandle>()
                .is_none());
            assert_eq!(
                item.to_value().unwrap(),
                serde_json::json!({ "label": "one", "hits": 0 })
            );
        }

        // A shared handle argument arrives as an id and is borrowed out of the registry.
        let shared = binding_named("dummy_label");
        assert_eq!(
            (shared.args)()[0].1["x-registry-ref"],
            serde_json::json!("DummyHandle")
        );
        let out = call(shared, &serde_json::json!({ "h": "d1" }), &state).unwrap();
        assert_eq!(out, b"\"one\"");

        // A `&mut` handle argument goes through the write lock and the change is visible after.
        let bump = binding_named("dummy_bump");
        let out = call(bump, &serde_json::json!({ "h": "d1" }), &state).unwrap();
        assert_eq!(out, b"1");
        let out = call(bump, &serde_json::json!({ "h": "d1" }), &state).unwrap();
        assert_eq!(out, b"2");
        assert_eq!(
            state
                .items
                .read()
                .unwrap()
                .get("d1")
                .unwrap()
                .as_custom::<DummyHandle>()
                .unwrap()
                .hits,
            2
        );

        // A `returns_handle` binding stores its result and reports the new id.
        let make = binding_named("dummy_new");
        assert_eq!(
            (make.return_type)()["x-registry-ref"],
            serde_json::json!("DummyHandle")
        );
        let out = call(make, &serde_json::json!({ "label": "two" }), &state).unwrap();
        let new_id: String = serde_json::from_slice(&out).unwrap();
        let items = state.items.read().unwrap();
        let created = items.get(&new_id).unwrap();
        assert_eq!(created.kind(), RegistryItemKind::Custom("DummyHandle"));
        assert_eq!(
            created.as_custom::<DummyHandle>().unwrap(),
            &DummyHandle {
                label: "two".to_string(),
                hits: 0
            }
        );

        // A wrong id is reported, not silently mistaken for a handle.
        assert!(call(shared, &serde_json::json!({ "h": "nope" }), &state).is_err());
    }

    #[test]
    fn state_mut_reaches_every_item_by_id_not_just_one_named_argument() {
        let state = AppState::default();
        state.add(
            "d1",
            RegistryItem::custom(DummyHandle {
                label: "one".to_string(),
                hits: 0,
            }),
        );
        state.add("d2", RegistryItem::EventLog(EventLog::default()));

        // `#[bind(state_mut)]` is not a JSON argument: no schema, not in the required list.
        let clear_all = binding_named("dummy_clear_all");
        assert!((clear_all.args)().is_empty());
        assert!((clear_all.required_args)().is_empty());

        let out = call(clear_all, &serde_json::json!({}), &state).unwrap();
        let n: usize = serde_json::from_slice(&out).unwrap();
        assert_eq!(n, 2, "both items were counted before being cleared");
        assert!(
            state.items.read().unwrap().is_empty(),
            "state_mut actually reached and cleared items no argument named"
        );
    }

    fn arg_schema(binding: &str, arg: &str) -> Value {
        (binding_named(binding).args)()
            .into_iter()
            .find(|(n, _)| n == arg)
            .unwrap_or_else(|| panic!("{} has no argument {}", binding, arg))
            .1
    }

    fn base64_of(bytes: &[u8]) -> String {
        use base64::Engine;
        base64::engine::general_purpose::STANDARD.encode(bytes)
    }

    /// A stored handle behind the id the resolved value names.
    fn stored_handle(state: &AppState, resolved: &Value) -> DummyHandle {
        state
            .items
            .read()
            .unwrap()
            .get(resolved.as_str().unwrap())
            .unwrap()
            .as_custom::<DummyHandle>()
            .unwrap()
            .clone()
    }

    #[test]
    fn handle_argument_accepts_every_documented_form() {
        let schema = arg_schema("dummy_label", "h");
        let state = AppState::default();
        state.add(
            "d1",
            RegistryItem::custom(DummyHandle {
                label: "stored".to_string(),
                hits: 0,
            }),
        );

        // A bare id that is stored comes back untouched, exactly as before.
        assert_eq!(
            resolve_argument("h", serde_json::json!("d1"), &schema, &state).unwrap(),
            serde_json::json!("d1")
        );
        // The explicit id wrapper does the same, without ever looking at the filesystem.
        assert_eq!(
            resolve_argument("h", serde_json::json!({ "id": "d1" }), &schema, &state).unwrap(),
            serde_json::json!("d1")
        );

        // A path wrapper.
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("handle.txt");
        std::fs::write(&path, b"from-path").unwrap();
        let resolved = resolve_argument(
            "h",
            serde_json::json!({ "path": path.to_str().unwrap() }),
            &schema,
            &state,
        )
        .unwrap();
        assert_eq!(stored_handle(&state, &resolved).label, "from-path");

        // Bytes plus a format, the form that needs no filesystem.
        let resolved = resolve_argument(
            "h",
            serde_json::json!({ "bytes": base64_of(b"from-bytes"), "format": "txt" }),
            &schema,
            &state,
        )
        .unwrap();
        assert_eq!(stored_handle(&state, &resolved).label, "from-bytes");
        // Unpadded base64 is accepted too.
        let resolved = resolve_argument(
            "h",
            serde_json::json!({ "bytes": base64_of(b"abcde").trim_end_matches('='), "format": "txt" }),
            &schema,
            &state,
        )
        .unwrap();
        assert_eq!(stored_handle(&state, &resolved).label, "abcde");

        // Wrapped inline JSON.
        let resolved = resolve_argument(
            "h",
            serde_json::json!({ "inline": { "label": "wrapped", "hits": 3 } }),
            &schema,
            &state,
        )
        .unwrap();
        assert_eq!(
            stored_handle(&state, &resolved),
            DummyHandle {
                label: "wrapped".to_string(),
                hits: 3
            }
        );

        // Bare inline JSON: two keys, so it is exactly the shape a wrapper could have been.
        let resolved = resolve_argument(
            "h",
            serde_json::json!({ "label": "bare", "hits": 7 }),
            &schema,
            &state,
        )
        .unwrap();
        assert_eq!(
            stored_handle(&state, &resolved),
            DummyHandle {
                label: "bare".to_string(),
                hits: 7
            }
        );

        // `null` still reaches the binding untouched, so `#[bind(default)]` keeps working.
        assert_eq!(
            resolve_argument("h", Value::Null, &schema, &state).unwrap(),
            Value::Null
        );

        // Bad inputs name the argument.
        let err = resolve_argument(
            "h",
            serde_json::json!({ "bytes": "not base64!!", "format": "txt" }),
            &schema,
            &state,
        )
        .unwrap_err();
        assert!(err.starts_with("Invalid Argument: h\n"), "{}", err);
        assert!(err.contains("base64"), "{}", err);

        let err = resolve_argument(
            "h",
            serde_json::json!({ "bytes": base64_of(b"x"), "format": "xes" }),
            &schema,
            &state,
        )
        .unwrap_err();
        assert!(err.starts_with("Invalid Argument: h\n"), "{}", err);

        let err = resolve_argument("h", serde_json::json!({ "id": "nope" }), &schema, &state)
            .unwrap_err();
        assert!(err.contains("nope"), "{}", err);

        let err =
            resolve_argument("h", serde_json::json!({ "hits": 1 }), &schema, &state).unwrap_err();
        assert!(err.starts_with("Invalid Argument: h\n"), "{}", err);

        // A missing path is still an error, and the bare form is still a path fallback.
        assert!(
            resolve_argument("h", serde_json::json!("no/such/file.txt"), &schema, &state).is_err()
        );
    }

    fn tiny_ocel_json() -> Value {
        serde_json::json!({
            "eventTypes": [],
            "objectTypes": [{ "name": "item", "attributes": [] }],
            "events": [],
            "objects": [
                { "id": "i1", "type": "item" },
                { "id": "i2", "type": "item" }
            ]
        })
    }

    #[test]
    fn built_in_handle_argument_forms_convert() {
        // `num_objects` takes a `SlimLinkedOCEL`, so an `OCEL` in any form needs converting.
        let schema = arg_schema("num_objects", "ocel");
        assert_eq!(
            schema["x-registry-ref"],
            serde_json::json!("SlimLinkedOCEL")
        );
        let state = AppState::default();
        let ocel: OCEL = serde_json::from_value(tiny_ocel_json()).unwrap();
        state.add("o1", ocel);

        // A stored id of the wrong kind is converted and stored under the derived name.
        let resolved = resolve_argument("ocel", serde_json::json!("o1"), &schema, &state).unwrap();
        assert_eq!(resolved, serde_json::json!("o1_as_SlimLinkedOCEL"));
        assert_eq!(
            state.items.read().unwrap()["o1_as_SlimLinkedOCEL"].kind(),
            RegistryItemKind::SlimLinkedOCEL
        );
        // The id wrapper converts the same way.
        assert_eq!(
            resolve_argument("ocel", serde_json::json!({ "id": "o1" }), &schema, &state).unwrap(),
            serde_json::json!("o1_as_SlimLinkedOCEL")
        );

        // Inline JSON, and the bytes of the same JSON, both land as a `SlimLinkedOCEL`.
        for value in [
            serde_json::json!({ "inline": tiny_ocel_json() }),
            tiny_ocel_json(),
            serde_json::json!({
                "bytes": base64_of(serde_json::to_string(&tiny_ocel_json()).unwrap().as_bytes()),
                "format": "json"
            }),
        ] {
            let resolved = resolve_argument("ocel", value, &schema, &state).unwrap();
            let items = state.items.read().unwrap();
            let item = &items[resolved.as_str().unwrap()];
            assert_eq!(item.kind(), RegistryItemKind::SlimLinkedOCEL);
            let RegistryItem::SlimLinkedOCEL(locel) = item else {
                unreachable!()
            };
            assert_eq!(locel.get_num_obs(), 2);
        }

        let err = resolve_argument(
            "ocel",
            serde_json::json!({ "inline": { "not": "an ocel", "at": "all" } }),
            &schema,
            &state,
        )
        .unwrap_err();
        assert!(err.starts_with("Invalid Argument: ocel\n"), "{}", err);
    }

    #[test]
    fn call_resolved_accepts_the_new_forms() {
        let state = AppState::default();
        let out = call_resolved(
            binding_named("dummy_label"),
            &serde_json::json!({ "h": { "inline": { "label": "inline-label", "hits": 0 } } }),
            &state,
        )
        .unwrap();
        assert_eq!(out, b"\"inline-label\"");

        let out = call_resolved(
            binding_named("num_objects"),
            &serde_json::json!({ "ocel": tiny_ocel_json() }),
            &state,
        )
        .unwrap();
        assert_eq!(out, b"2");

        // A plain id keeps working through the same entry point.
        state.add(
            "d1",
            RegistryItem::custom(DummyHandle {
                label: "stored".to_string(),
                hits: 0,
            }),
        );
        let out = call_resolved(
            binding_named("dummy_label"),
            &serde_json::json!({ "h": "d1" }),
            &state,
        )
        .unwrap();
        assert_eq!(out, b"\"stored\"");
    }

    /// The id a call reports back.
    fn called_id(name: &str, args: Value, state: &AppState) -> String {
        let out = call(binding_named(name), &args, state).unwrap();
        serde_json::from_slice(&out).unwrap()
    }

    fn stored_handle_at(state: &AppState, id: &str) -> DummyHandle {
        state
            .items
            .read()
            .unwrap()
            .get(id)
            .unwrap_or_else(|| panic!("nothing stored under {}", id))
            .as_custom::<DummyHandle>()
            .unwrap()
            .clone()
    }

    #[test]
    fn output_id_stores_the_result_under_exactly_that_id() {
        let state = AppState::default();
        state.add(
            "d1",
            RegistryItem::custom(DummyHandle {
                label: "src".to_string(),
                hits: 0,
            }),
        );
        state.add(
            "o1",
            serde_json::from_value::<OCEL>(tiny_ocel_json()).unwrap(),
        );

        // Custom handle, read-lock path.
        let id = called_id(
            "dummy_new",
            serde_json::json!({ "label": "two", "output_id": "chosen" }),
            &state,
        );
        assert_eq!(id, "chosen");
        assert_eq!(
            stored_handle_at(&state, "chosen"),
            DummyHandle {
                label: "two".to_string(),
                hits: 0
            }
        );

        // Custom handle, write-guard path.
        let id = called_id(
            "dummy_fork",
            serde_json::json!({ "h": "d1", "output_id": "forked" }),
            &state,
        );
        assert_eq!(id, "forked");
        assert_eq!(stored_handle_at(&state, "forked").hits, 1);

        // Big type, write-guard path.
        let id = called_id(
            "dummy_log_of",
            serde_json::json!({ "h": "d1", "output_id": "a_log" }),
            &state,
        );
        assert_eq!(id, "a_log");
        assert_eq!(
            state.items.read().unwrap()["a_log"].kind(),
            RegistryItemKind::EventLog
        );

        // Big type, read-lock path.
        let id = called_id(
            "index_link_ocel",
            serde_json::json!({ "ocel": "o1", "output_id": "linked" }),
            &state,
        );
        assert_eq!(id, "linked");
        assert_eq!(
            state.items.read().unwrap()["linked"].kind(),
            RegistryItemKind::IndexLinkedOCEL
        );

        // The same id twice replaces, rather than minting a second item.
        let before = state.items.read().unwrap().len();
        let id = called_id(
            "dummy_new",
            serde_json::json!({ "label": "again", "output_id": "chosen" }),
            &state,
        );
        assert_eq!(id, "chosen");
        assert_eq!(stored_handle_at(&state, "chosen").label, "again");
        assert_eq!(state.items.read().unwrap().len(), before);

        // A non-string is rejected by name rather than silently ignored.
        let err = call(
            binding_named("dummy_new"),
            &serde_json::json!({ "label": "x", "output_id": 7 }),
            &state,
        )
        .unwrap_err();
        assert!(err.contains("output_id"), "{}", err);
    }

    #[test]
    fn omitting_output_id_keeps_the_generated_id() {
        let state = AppState::default();
        state.add(
            "d1",
            RegistryItem::custom(DummyHandle {
                label: "src".to_string(),
                hits: 0,
            }),
        );

        for args in [
            serde_json::json!({ "label": "two" }),
            serde_json::json!({ "label": "two", "output_id": null }),
        ] {
            let id = called_id("dummy_new", args, &state);
            assert!(id.starts_with("res_"), "got {}", id);
            assert_eq!(stored_handle_at(&state, &id).label, "two");
        }

        // Every insert site keeps the old behaviour, not just the read-lock one.
        let id = called_id("dummy_fork", serde_json::json!({ "h": "d1" }), &state);
        assert!(id.starts_with("res_"), "got {}", id);
        let id = called_id("dummy_log_of", serde_json::json!({ "h": "d1" }), &state);
        assert!(id.starts_with("res_"), "got {}", id);
    }

    #[test]
    fn output_id_is_declared_only_where_a_handle_is_returned() {
        for name in [
            "dummy_new",
            "dummy_fork",
            "dummy_log_of",
            "index_link_ocel",
            "slim_link_ocel",
        ] {
            let binding = binding_named(name);
            let schema = arg_schema(name, "output_id");
            assert_eq!(schema["type"], serde_json::json!(["string", "null"]));
            assert_eq!(schema["title"], serde_json::json!("output_id"));
            assert!(schema["description"].is_string(), "{}", name);
            assert!(
                !(binding.required_args)().iter().any(|a| a == "output_id"),
                "{} requires output_id",
                name
            );
            // Appended after the function's own arguments, so their order is untouched.
            let names: Vec<String> = (binding.args)().into_iter().map(|(n, _)| n).collect();
            assert_eq!(names.last().unwrap(), "output_id");
        }
        assert_eq!(
            (binding_named("dummy_new").args)()
                .into_iter()
                .map(|(n, _)| n)
                .collect::<Vec<_>>(),
            vec!["label", "output_id"]
        );
        assert_eq!(
            (binding_named("dummy_new").required_args)(),
            vec!["label".to_string()]
        );

        // A value-returning binding is untouched, including the ones that take a handle.
        for name in [
            "dummy_label",
            "dummy_bump",
            "num_objects",
            "test_some_inputs",
        ] {
            let binding = binding_named(name);
            assert!(
                !(binding.args)().iter().any(|(n, _)| n == "output_id"),
                "{} grew an output_id",
                name
            );
        }
        assert_eq!((binding_named("num_objects").args)().len(), 1);
        assert_eq!((binding_named("test_some_inputs").args)().len(), 5);
    }

    #[test]
    fn a_concurrent_insert_disturbs_neither_side() {
        use std::sync::Arc;
        let state = Arc::new(AppState::default());
        let other = Arc::clone(&state);
        let writer = std::thread::spawn(move || {
            for i in 0..64usize {
                other.add(
                    format!("unrelated_{}", i),
                    RegistryItem::custom(DummyHandle {
                        label: format!("u{}", i),
                        hits: i,
                    }),
                );
            }
        });

        let id = called_id(
            "dummy_new",
            serde_json::json!({ "label": "named", "output_id": "chosen" }),
            &state,
        );
        writer.join().unwrap();

        assert_eq!(id, "chosen");
        assert_eq!(stored_handle_at(&state, "chosen").label, "named");
        let items = state.items.read().unwrap();
        for i in 0..64usize {
            assert_eq!(
                items[&format!("unrelated_{}", i)]
                    .as_custom::<DummyHandle>()
                    .unwrap()
                    .hits,
                i
            );
        }
        assert_eq!(items.len(), 65);
    }
}
