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
//! 1. Define a function and annotate it with `#[register_binding]`. An argument
//!    `#[bind(ctx)] ctl: &Control<'_>` receives the call's [`Control`], for progress and
//!    cancellation.
//! 2. Use `list_functions()` to discover available commands.
//! 3. Use `call()` to execute them.
//!
//! ## Type Handling
//!
//! - **Simple Types**: Serialized/Deserialized via `serde_json`.
//! - **Big Types**: Stored in `AppState`. Arguments are string IDs pointing to the state.
//!   Return values are stored in state, and their new ID is returned. A function returning a
//!   `(BigType, T)` pair stores the first element under the then required `output_id` argument
//!   and returns `T`. Its `output_id` schema names the stored kind as `x-registry-out`.
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

use crate::utils::control::{Cancelled, Control};

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
    ) && crate::core::event_data::case_centric::io::EventLogFormat::parse_xes_gz(format).is_ok()
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
/// A new variant also needs an entry in `BIG_TYPES_NAMES` in the macro crate.
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

/// Every `(from, to)` pair [`RegistryItem::convert`] and [`RegistryItem::convert_into`] handle.
///
/// The conversions themselves stay a `match` there, since each arm destructures a different
/// variant. The test `conversion_table_matches_convert` checks every pair of kinds against this
/// table.
const CONVERSIONS: &[(RegistryItemKind, RegistryItemKind)] = &[
    (
        RegistryItemKind::EventLog,
        RegistryItemKind::EventLogActivityProjection,
    ),
    (RegistryItemKind::OCEL, RegistryItemKind::IndexLinkedOCEL),
    (RegistryItemKind::OCEL, RegistryItemKind::SlimLinkedOCEL),
    (RegistryItemKind::IndexLinkedOCEL, RegistryItemKind::OCEL),
    (
        RegistryItemKind::IndexLinkedOCEL,
        RegistryItemKind::SlimLinkedOCEL,
    ),
    (RegistryItemKind::SlimLinkedOCEL, RegistryItemKind::OCEL),
    (
        RegistryItemKind::SlimLinkedOCEL,
        RegistryItemKind::IndexLinkedOCEL,
    ),
];

impl RegistryItemKind {
    /// The kinds an item of this kind can be converted to with [`RegistryItem::convert`].
    ///
    /// Empty for a custom kind: the registry has no conversions for those.
    pub fn convertible_to(&self) -> Vec<RegistryItemKind> {
        CONVERSIONS
            .iter()
            .filter(|(from, _)| from == self)
            .map(|(_, to)| *to)
            .collect()
    }

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

// Hand-written because `Custom(&'static str)` has no derivable `Deserialize`, and a derive would
// give it the externally tagged `{"Custom": "X"}` form. Going through `Display`/`FromStr` keeps
// every kind a bare string on the wire.
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

use crate::core::io::{controlled, Exportable, Importable, IMPORT_STAGE};
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
            // A summary only: the bytes can be a whole database.
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
    pub fn load_from_path(
        item_kind: &RegistryItemKind,
        path: impl AsRef<std::path::Path>,
    ) -> Result<Self, String> {
        Self::load_from_path_with(item_kind, path, &Control::new()).map_err(String::from)
    }

    /// [`RegistryItem::load_from_path`], importing through
    /// [`Importable::import_from_path_with`], so it reports progress to `ctl` and returns
    /// [`CallError::Cancelled`] once `ctl` is cancelled. A custom kind loads without progress.
    pub fn load_from_path_with(
        item_kind: &RegistryItemKind,
        path: impl AsRef<std::path::Path>,
        ctl: &Control<'_>,
    ) -> Result<Self, CallError> {
        let path = path.as_ref();
        controlled(ctl, || {
            #[cfg(feature = "extraction-blueprint")]
            if crate::core::io::infer_format_from_path(path)
                .is_some_and(|format| reads_as_case_centric(item_kind, &format))
            {
                let log = EventLog::import_from_path_with(path, Default::default(), ctl)
                    .map_err(|e| e.to_string())?;
                return Ok(case_centric_as(item_kind, &log)?);
            }

            Ok(match item_kind {
                RegistryItemKind::EventLog => RegistryItem::EventLog(
                    EventLog::import_from_path_with(path, Default::default(), ctl)
                        .map_err(|e| e.to_string())?,
                ),
                RegistryItemKind::OCEL => RegistryItem::OCEL(
                    OCEL::import_from_path_with(path, Default::default(), ctl)
                        .map_err(|e| e.to_string())?,
                ),
                RegistryItemKind::SlimLinkedOCEL => RegistryItem::SlimLinkedOCEL(
                    SlimLinkedOCEL::import_from_path_with(path, Default::default(), ctl)
                        .map_err(|e| e.to_string())?,
                ),
                RegistryItemKind::IndexLinkedOCEL => RegistryItem::IndexLinkedOCEL(
                    IndexLinkedOCEL::import_from_path_with(path, Default::default(), ctl)
                        .map_err(|e| e.to_string())?,
                ),
                RegistryItemKind::EventLogActivityProjection => {
                    RegistryItem::EventLogActivityProjection(
                        EventLogActivityProjection::import_from_path_with(path, (), ctl)
                            .map_err(|e| e.to_string())?,
                    )
                }
                RegistryItemKind::TabularSource => RegistryItem::TabularSource(
                    TabularSource::import_from_path_with(path, (), ctl)
                        .map_err(|e| e.to_string())?,
                ),
                RegistryItemKind::Custom(name) => {
                    let info = custom_kind(name).ok_or_else(|| unregistered_kind_msg(name))?;
                    (info.from_path)(path)?
                }
            })
        })
    }

    /// Try to load a registry item from bytes based on the expected type name and format
    ///
    /// The object-centric kinds also accept `xes`/`xes.gz`, which no OCEL reader handles: the
    /// bytes are parsed as a case-centric [`EventLog`] and converted with
    /// `extraction::write_event_log_to_sink`, one object per case. That conversion needs the
    /// `extraction-blueprint` feature.
    pub fn load_from_bytes(
        item_kind: &RegistryItemKind,
        data: &[u8],
        format: &str,
    ) -> Result<Self, String> {
        Self::load_from_bytes_with(item_kind, data, format, &Control::new()).map_err(String::from)
    }

    /// [`RegistryItem::load_from_bytes`], importing through
    /// [`Importable::import_from_bytes_with`], so it reports progress to `ctl` and returns
    /// [`CallError::Cancelled`] once `ctl` is cancelled. A custom kind loads without progress.
    pub fn load_from_bytes_with(
        item_kind: &RegistryItemKind,
        data: &[u8],
        format: &str,
        ctl: &Control<'_>,
    ) -> Result<Self, CallError> {
        controlled(ctl, || {
            #[cfg(feature = "extraction-blueprint")]
            if reads_as_case_centric(item_kind, format) {
                let log = EventLog::import_from_bytes_with(data, format, Default::default(), ctl)
                    .map_err(|e| e.to_string())?;
                return Ok(case_centric_as(item_kind, &log)?);
            }
            Ok(match item_kind {
                RegistryItemKind::EventLog => RegistryItem::EventLog(
                    EventLog::import_from_bytes_with(data, format, Default::default(), ctl)
                        .map_err(|e| e.to_string())?,
                ),
                RegistryItemKind::OCEL => RegistryItem::OCEL(
                    OCEL::import_from_bytes_with(data, format, Default::default(), ctl)
                        .map_err(|e| e.to_string())?,
                ),
                RegistryItemKind::IndexLinkedOCEL => RegistryItem::IndexLinkedOCEL(
                    IndexLinkedOCEL::import_from_bytes_with(data, format, Default::default(), ctl)
                        .map_err(|e| e.to_string())?,
                ),
                RegistryItemKind::SlimLinkedOCEL => RegistryItem::SlimLinkedOCEL(
                    SlimLinkedOCEL::import_from_bytes_with(data, format, Default::default(), ctl)
                        .map_err(|e| e.to_string())?,
                ),
                RegistryItemKind::EventLogActivityProjection => {
                    RegistryItem::EventLogActivityProjection(
                        EventLogActivityProjection::import_from_bytes_with(data, format, (), ctl)
                            .map_err(|e| e.to_string())?,
                    )
                }
                RegistryItemKind::TabularSource => RegistryItem::TabularSource(
                    TabularSource::import_from_bytes_with(data, format, (), ctl)
                        .map_err(|e| e.to_string())?,
                ),
                RegistryItemKind::Custom(name) => {
                    let info = custom_kind(name).ok_or_else(|| unregistered_kind_msg(name))?;
                    (info.from_bytes)(data, format)?
                }
            })
        })
    }

    /// Try to build a registry item from the JSON form of the given kind.
    ///
    /// The inverse of [`RegistryItem::to_value`]: what that method writes for a kind, this one
    /// reads back.
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
    /// form is a directory. This is also the route that avoids materializing the whole export in
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
                .export_to_path_as(path, format, Default::default())
                .map_err(|e| e.to_string()),
            RegistryItem::IndexLinkedOCEL(x) => x
                .export_to_path_as(path, format, Default::default())
                .map_err(|e| e.to_string()),
            RegistryItem::SlimLinkedOCEL(x) => x
                .export_to_path_as(path, format, Default::default())
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

    /// Convert the registry item to another kind, leaving `self` as it is
    ///
    /// [`RegistryItemKind::convertible_to`] lists the kinds this succeeds for. Use
    /// [`RegistryItem::convert_into`] when the item is not needed afterwards: it moves rather
    /// than copies where it can.
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
            (RegistryItem::IndexLinkedOCEL(locel), RegistryItemKind::SlimLinkedOCEL) => {
                Ok(RegistryItem::SlimLinkedOCEL(locel.into()))
            }
            (RegistryItem::SlimLinkedOCEL(locel), RegistryItemKind::OCEL) => {
                Ok(RegistryItem::OCEL(locel.construct_ocel()))
            }
            (RegistryItem::SlimLinkedOCEL(locel), RegistryItemKind::IndexLinkedOCEL) => {
                Ok(RegistryItem::IndexLinkedOCEL(locel.into()))
            }
            (RegistryItem::OCEL(ocel), RegistryItemKind::SlimLinkedOCEL) => Ok(
                RegistryItem::SlimLinkedOCEL(SlimLinkedOCEL::from_ocel(ocel.clone())),
            ),
            _ => Err(conversion_error(self.kind(), target_kind)),
        }
    }

    /// Convert the registry item to another kind, consuming it
    ///
    /// Same conversions as [`RegistryItem::convert`], but an [`OCEL`] or [`IndexLinkedOCEL`] is
    /// moved into the result instead of copied. On error, the item is returned alongside the
    /// message, so it is not lost.
    pub fn convert_into(self, target_kind: RegistryItemKind) -> Result<Self, (Box<Self>, String)> {
        match (self, target_kind) {
            (RegistryItem::EventLog(log), RegistryItemKind::EventLogActivityProjection) => {
                Ok(RegistryItem::EventLogActivityProjection((&log).into()))
            }
            (RegistryItem::OCEL(ocel), RegistryItemKind::IndexLinkedOCEL) => Ok(
                RegistryItem::IndexLinkedOCEL(IndexLinkedOCEL::from_ocel(ocel)),
            ),
            (RegistryItem::IndexLinkedOCEL(locel), RegistryItemKind::OCEL) => {
                Ok(RegistryItem::OCEL(locel.into_inner()))
            }
            (RegistryItem::IndexLinkedOCEL(locel), RegistryItemKind::SlimLinkedOCEL) => {
                Ok(RegistryItem::SlimLinkedOCEL(locel.into()))
            }
            (RegistryItem::SlimLinkedOCEL(locel), RegistryItemKind::OCEL) => {
                Ok(RegistryItem::OCEL(locel.construct_ocel()))
            }
            (RegistryItem::SlimLinkedOCEL(locel), RegistryItemKind::IndexLinkedOCEL) => {
                Ok(RegistryItem::IndexLinkedOCEL((&locel).into()))
            }
            (RegistryItem::OCEL(ocel), RegistryItemKind::SlimLinkedOCEL) => Ok(
                RegistryItem::SlimLinkedOCEL(SlimLinkedOCEL::from_ocel(ocel)),
            ),
            (item, _) => {
                let msg = conversion_error(item.kind(), target_kind);
                Err((Box::new(item), msg))
            }
        }
    }
}

fn conversion_error(from: RegistryItemKind, to: RegistryItemKind) -> String {
    format!("Cannot convert {} to {}", from, to)
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
/// For a binding that manages the registry itself instead of one resolved item, e.g., evicting
/// items to free memory. To change the one item a parameter names, take it by `&mut` (or
/// `#[bind(handle)] &mut`). Use `state_mut` only when the items to touch are not known until the
/// body runs.
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
    /// A binding body that panics while holding the write lock poisons it. The map itself is
    /// still consistent, and [`call`] drops every item the panicking body held by `&mut`, so
    /// refusing every later call would be worse.
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

/// Why a binding call failed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CallError {
    /// The call's [`Control`] was cancelled. The call stored nothing.
    Cancelled,
    /// Any other failure, with its message.
    Failed(String),
}

impl std::fmt::Display for CallError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CallError::Cancelled => f.write_str("cancelled"),
            CallError::Failed(msg) => f.write_str(msg),
        }
    }
}

impl std::error::Error for CallError {}

impl From<String> for CallError {
    fn from(msg: String) -> Self {
        CallError::Failed(msg)
    }
}

impl From<&str> for CallError {
    fn from(msg: &str) -> Self {
        CallError::Failed(msg.to_string())
    }
}

impl From<Cancelled> for CallError {
    fn from(_: Cancelled) -> Self {
        CallError::Cancelled
    }
}

impl From<CallError> for String {
    fn from(e: CallError) -> Self {
        e.to_string()
    }
}

/// Function Binding
#[derive(Debug)]
pub struct Binding {
    /// `<crate>::<name>`, so it stays when the function moves
    pub id: &'static str,
    /// Name of the function
    pub name: &'static str,
    /// Function handler (executing the function with (de-)serializing inputs/outputs).
    /// Returns the result pre-serialized as UTF-8 JSON bytes.
    pub handler: fn(&Value, &AppState, &Control<'_>) -> Result<Vec<u8>, CallError>,
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
/// An object is a wrapper only when its key set is exactly one of the recognized ones and the
/// recognized keys hold strings. Both halves matter: a real OCEL or `EventLog` object carries
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
/// Converts on a kind mismatch, storing the result under a fresh id pushed onto `minted`. A
/// `mutable` argument is never converted: the binding would mutate the copy, not `id`.
fn resolve_stored_id(
    id: &str,
    arg_ref: &str,
    mutable: bool,
    state: &AppState,
    minted: &mut Vec<String>,
) -> Result<Option<Value>, String> {
    // A conversion can be slow, and under the write lock it would block every other call.
    let converted = {
        let items = state.read();
        let Some(item) = items.get(id) else {
            return Ok(None);
        };
        if item.kind().to_string() == arg_ref {
            return Ok(Some(Value::String(id.to_string())));
        }
        if mutable {
            return Err(format!(
                "Type mismatch for ID '{}': expected {}, found {}. This argument is modified in \
                 place, so it is not converted implicitly. Convert the item explicitly and pass \
                 the converted ID.",
                id,
                arg_ref,
                item.kind()
            ));
        }
        let target_kind = RegistryItemKind::from_str(arg_ref)?;
        item.convert(target_kind).map_err(|e| {
            format!(
                "Type mismatch for ID '{}': expected {}, found {}. Conversion failed: {}",
                id,
                arg_ref,
                item.kind(),
                e
            )
        })?
    };
    let new_id = format!("{}_as_{}_{}", id, arg_ref, uuid::Uuid::new_v4());
    state.add(new_id.clone(), converted);
    minted.push(new_id.clone());
    Ok(Some(Value::String(new_id)))
}

/// Resolve an argument value based on its schema and the current state.
///
/// This function handles:
/// 1. Materializing "Big Types" if the schema indicates a registry reference, from any of the
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
/// a conversion exists, except for an argument the binding takes by `&mut`. The other forms have
/// no kind to mismatch: the path, the bytes and the JSON are read as the referenced kind directly.
///
/// An argument the binding takes by `&mut` accepts only the two id forms naming a stored item:
/// the binding would otherwise change a copy that is dropped after the call.
pub fn resolve_argument(
    arg_name: &str,
    value: Value,
    schema: &Value,
    state: &AppState,
) -> Result<Value, String> {
    resolve_argument_tracked(
        arg_name,
        value,
        schema,
        state,
        &mut Vec::new(),
        &Control::new(),
    )
}

/// [`resolve_argument`], pushing every id it stores an item under onto `minted`.
///
/// Those ids never reach the caller, so only the requester of the resolution can drop them again.
///
/// An item read from a path or bytes reports its import to `ctl` as stage `"Importing <arg_name>"`
/// and stops once `ctl` is cancelled.
fn resolve_argument_tracked(
    arg_name: &str,
    value: Value,
    schema: &Value,
    state: &AppState,
    minted: &mut Vec<String>,
    ctl: &Control<'_>,
) -> Result<Value, String> {
    let schema_obj = schema.as_object().ok_or("Invalid schema")?;

    // Case 1: Registry Reference
    if let Some(arg_ref) = schema_obj.get("x-registry-ref").and_then(|r| r.as_str()) {
        if let Some(handle_arg) = classify_handle_arg(&value) {
            let import_ctl = import_control(ctl, arg_name);
            let mutable = is_mut_registry_arg(schema);
            let invalid = |e: String| format!("Invalid Argument: {}\n{}", arg_name, e);
            // A kind that no downstream crate registered still resolves by id, so this is looked
            // up only where a loader is actually needed.
            let target_kind = || RegistryItemKind::from_str(arg_ref).map_err(invalid);
            let needs_stored_id = || {
                invalid(format!(
                    "A binding that changes its argument needs the ID of a stored {}.",
                    arg_ref
                ))
            };
            let item = match handle_arg {
                HandleArg::Bare(id) => {
                    if let Some(resolved) = resolve_stored_id(id, arg_ref, mutable, state, minted)?
                    {
                        return Ok(resolved);
                    }
                    if mutable {
                        return Err(invalid(format!(
                            "No {} is stored under the ID '{}'. A binding that changes its \
                             argument needs the ID of a stored {}.",
                            arg_ref, id, arg_ref
                        )));
                    }
                    RegistryItem::load_from_path_with(&target_kind()?, id, &import_ctl)
                        .map_err(|e| invalid(e.into()))?
                }
                HandleArg::Path(_) | HandleArg::Bytes { .. } | HandleArg::Inline(_) if mutable => {
                    return Err(needs_stored_id());
                }
                HandleArg::Id(id) => {
                    return resolve_stored_id(id, arg_ref, mutable, state, minted)?.ok_or_else(
                        || invalid(format!("No {} is stored under the ID '{}'", arg_ref, id)),
                    )
                }
                HandleArg::Path(path) => {
                    RegistryItem::load_from_path_with(&target_kind()?, path, &import_ctl)
                        .map_err(|e| invalid(e.into()))?
                }
                HandleArg::Bytes { b64, format } => {
                    let bytes = decode_base64(b64)
                        .map_err(|e| invalid(format!("'bytes' is not valid base64: {}", e)))?;
                    RegistryItem::load_from_bytes_with(&target_kind()?, &bytes, format, &import_ctl)
                        .map_err(|e| invalid(e.into()))?
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

/// A control for importing argument `arg_name` that reports to `ctl` with the stage `"Importing"`
/// renamed to `"Importing <arg_name>"` (`"Importing events"` to `"Importing <arg_name> (events)"`), and
/// passes on warnings and the cancel flag.
fn import_control<'c>(ctl: &'c Control<'_>, arg_name: &str) -> Control<'c> {
    let mut import_ctl = Control::new().on_warning(|w| ctl.warn(w));
    if ctl.wants_progress() {
        let label = format!("{IMPORT_STAGE} {arg_name}");
        import_ctl = import_ctl.on_progress(move |p| {
            let stage = match p.stage.strip_prefix(IMPORT_STAGE) {
                Some("") => label.clone(),
                Some(rest) => format!("{label} ({})", rest.trim_start()),
                None => format!("{label}: {}", p.stage),
            };
            ctl.progress(stage, p.done, p.total);
        });
    }
    if let Some(flag) = ctl.cancel_flag_ref() {
        import_ctl = import_ctl.cancel_flag(flag);
    }
    import_ctl
}

/// Call the specified function with the passed arguments.
/// Returns the result pre-serialized as UTF-8 JSON bytes.
///
/// A panic in the binding body is caught and reported as an error, because hosts reach this
/// across an FFI or wasm boundary, where an unwind is undefined behavior.
///
/// A panic can leave an item the body held by `&mut` half-updated, so every such item is removed
/// from `state` before the error is returned.
///
/// Runs without progress reports or cancellation; see [`call_with_outcome`].
pub fn call(binding: &Binding, args: &Value, state: &AppState) -> Result<Vec<u8>, String> {
    call_controlled(binding, args, state, &Control::new()).map_err(String::from)
}

fn call_controlled(
    binding: &Binding,
    args: &Value,
    state: &AppState,
    ctl: &Control<'_>,
) -> Result<Vec<u8>, CallError> {
    ctl.check()?;
    let called = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        (binding.handler)(args, state, ctl)
    }));
    match called {
        Ok(Err(CallError::Failed(_))) if ctl.is_cancelled() => Err(CallError::Cancelled),
        Ok(result) => result,
        Err(payload) => {
            let what = payload
                .downcast_ref::<&str>()
                .map(|s| (*s).to_string())
                .or_else(|| payload.downcast_ref::<String>().cloned())
                .unwrap_or_else(|| "panicked".to_string());
            let dropped = drop_mut_args(&(binding.args)(), args, state);
            let dropped_note = if dropped.is_empty() {
                String::new()
            } else {
                format!(
                    " (removed possibly inconsistent item(s): {})",
                    dropped.join(", ")
                )
            };
            Err(CallError::Failed(format!(
                "{} panicked: {}{}",
                binding.name, what, dropped_note
            )))
        }
    }
}

/// Whether `schema` describes a registry argument the binding takes by `&mut`.
fn is_mut_registry_arg(schema: &Value) -> bool {
    schema.get("x-registry-mut").and_then(Value::as_bool) == Some(true)
}

/// The ids passed in `args` for the arguments that `schemas` mark as taken by `&mut`.
fn mut_arg_ids(schemas: &[(String, Value)], args: &Value) -> Vec<String> {
    let Some(passed) = args.as_object() else {
        return Vec::new();
    };
    schemas
        .iter()
        .filter(|(_, schema)| is_mut_registry_arg(schema))
        .filter_map(|(name, _)| passed.get(name)?.as_str().map(str::to_string))
        .collect()
}

/// Remove the items taken by `&mut` from `state`, returning their ids.
fn drop_mut_args(schemas: &[(String, Value)], args: &Value, state: &AppState) -> Vec<String> {
    let ids = mut_arg_ids(schemas, args);
    let mut items = state.write();
    ids.into_iter()
        .filter(|id| items.remove(id).is_some())
        .collect()
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
    resolve_and_call(binding, &(binding.args)(), args, state, &Control::new())
        .map(|(result, _, _)| result)
        .map_err(String::from)
}

/// [`call_resolved`], returning the result together with the arguments as the binding received
/// them and the ids stored for the duration of the call (already dropped again).
///
/// `schemas` are the binding's argument schemas, as `(binding.args)()` builds them.
fn resolve_and_call(
    binding: &Binding,
    schemas: &[(String, Value)],
    args: &Value,
    state: &AppState,
    ctl: &Control<'_>,
) -> Result<(Vec<u8>, Value, Vec<String>), CallError> {
    let Some(passed) = args.as_object() else {
        return call_controlled(binding, args, state, ctl)
            .map(|result| (result, args.clone(), Vec::new()));
    };
    let mut resolved = serde_json::Map::with_capacity(passed.len());
    let mut minted: Vec<String> = Vec::new();
    for (name, value) in passed {
        let value = match schemas.iter().find(|(n, _)| n == name) {
            Some((_, schema)) => {
                match resolve_argument_tracked(name, value.clone(), schema, state, &mut minted, ctl)
                {
                    Ok(value) => value,
                    Err(e) => {
                        drop_minted(state, &minted);
                        return Err(if ctl.is_cancelled() {
                            CallError::Cancelled
                        } else {
                            e.into()
                        });
                    }
                }
            }
            None => value.clone(),
        };
        resolved.insert(name.clone(), value);
    }
    let resolved = Value::Object(resolved);
    let result = call_controlled(binding, &resolved, state, ctl);
    drop_minted(state, &minted);
    result.map(|result| (result, resolved, minted))
}

/// What a successful call did to the registry, next to its result. See [`call_with_outcome`].
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct CallOutcome {
    /// The result, pre-serialized as UTF-8 JSON bytes, exactly as [`call`] returns it.
    pub result: Vec<u8>,
    /// The id the binding stored its result under, for a binding that returns a registry handle
    /// or stores an item under a required `output_id` next to its result.
    ///
    /// An `output_id` naming an existing item replaces that item, and is reported here too.
    /// Items [`call_resolved_with_outcome`] stored only for the duration of the call are gone
    /// again and not listed.
    pub minted: Vec<String>,
    /// The ids of the items the binding took by `&mut`, so possibly changed in place.
    ///
    /// A binding taking `#[bind(state_mut)]` can change any item, which is not tracked here.
    pub mutated: Vec<String>,
}

/// [`call`], reporting progress and partial results through `ctl`, stopping once it is
/// cancelled, and reporting which registry ids the call stored or changed.
///
/// Bindings that take `#[bind(ctx)]` report and check `ctl` themselves. Once cancelled, a call
/// that fails or would store its result returns [`CallError::Cancelled`] and stores nothing,
/// whether or not the binding noticed the cancel. A result that is not stored is returned as
/// usual, and a panic is reported as the panic, cancelled or not.
///
/// The ids are read off the binding's schema and result rather than by comparing the registry
/// before and after, so a concurrent call on the same [`AppState`] does not show up in it.
pub fn call_with_outcome(
    binding: &Binding,
    args: &Value,
    state: &AppState,
    ctl: &Control<'_>,
) -> Result<CallOutcome, CallError> {
    let result = call_controlled(binding, args, state, ctl)?;
    Ok(call_outcome(binding, &(binding.args)(), args, result, &[]))
}

/// [`call_resolved`] with a [`Control`] and the registry ids the call stored or changed, as in
/// [`call_with_outcome`]. Also reports progress while it imports event data arguments.
pub fn call_resolved_with_outcome(
    binding: &Binding,
    args: &Value,
    state: &AppState,
    ctl: &Control<'_>,
) -> Result<CallOutcome, CallError> {
    let schemas = (binding.args)();
    let (result, resolved, dropped) = resolve_and_call(binding, &schemas, args, state, ctl)?;
    Ok(call_outcome(binding, &schemas, &resolved, result, &dropped))
}

/// `args` are the arguments as the handler got them, so registry ids, not paths or inline JSON.
fn call_outcome(
    binding: &Binding,
    schemas: &[(String, Value)],
    args: &Value,
    result: Vec<u8>,
    dropped: &[String],
) -> CallOutcome {
    let stores_beside_result = schemas
        .iter()
        .any(|(name, schema)| name == "output_id" && schema.get("x-registry-out").is_some());
    let minted = if stores_beside_result {
        args.get("output_id")
            .and_then(Value::as_str)
            .map(str::to_string)
            .into_iter()
            .collect()
    } else if result.first() == Some(&b'"')
        && (binding.return_type)().get("x-registry-ref").is_some()
    {
        serde_json::from_slice::<String>(&result)
            .ok()
            .into_iter()
            .collect()
    } else {
        Vec::new()
    };
    let mut mutated = mut_arg_ids(schemas, args);
    mutated.retain(|id| !dropped.contains(id));
    CallOutcome {
        result,
        minted,
        mutated,
    }
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

/// Get the binding with this id, e.g., `process_mining::discover_dfg`
pub fn get_fn_binding(id: &str) -> Option<&'static Binding> {
    inventory::iter::<Binding>.into_iter().find(|b| b.id == id)
}

#[cfg(feature = "extraction-blueprint")]
mod extraction_bindings;
#[cfg(feature = "extraction-dbcon")]
mod extraction_dbcon_bindings;
mod model_kinds;
pub use model_kinds::OCDeclareModel;
mod path_schema_bindings;
mod reflow_bindings;
pub use reflow_bindings::{ReflowCellRef, ReflowSummary};
mod shared_meta;
pub use shared_meta::{list_functions_meta_shared, BindingsMetaShared};
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

#[cfg(test)]
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

    #[register_binding]
    fn dummy_index_link(ocel: &OCEL) -> IndexLinkedOCEL {
        IndexLinkedOCEL::from_ocel(ocel.clone())
    }

    #[register_binding]
    fn dummy_slim_link(ocel: &OCEL) -> SlimLinkedOCEL {
        SlimLinkedOCEL::from_ocel(ocel.clone())
    }

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

    /// Count to `n`, reporting each step and streaming each number, and stop once cancelled.
    #[register_binding(stringify_error)]
    fn dummy_count(n: u64, #[bind(ctx)] ctl: &Control<'_>) -> Result<u64, Cancelled> {
        let counter = ctl.counter("count", Some(n));
        for i in 0..n {
            ctl.check()?;
            ctl.partial(&i);
            counter.tick();
        }
        Ok(n)
    }

    /// A handle built under a context, to check that a cancelled call stores nothing.
    #[register_binding(returns_handle)]
    fn dummy_new_ctx(label: String, #[bind(ctx)] ctl: &Control<'_>) -> DummyHandle {
        ctl.progress("new", 1, Some(1));
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

    /// A stored handle next to a returned value, under a context so a cancel can land during
    /// the body.
    #[register_binding(returns_handle)]
    fn dummy_new_with_len(label: String, #[bind(ctx)] ctl: &Control<'_>) -> (DummyHandle, usize) {
        ctl.progress("new", 1, Some(1));
        let len = label.len();
        (DummyHandle { label, hits: 0 }, len)
    }

    /// A big type next to a returned value, stored through the write guard.
    #[register_binding(stringify_error)]
    fn dummy_log_and_hits(
        #[bind(handle)] h: &mut DummyHandle,
    ) -> Result<(EventLog, usize), String> {
        h.hits += 1;
        Ok((EventLog::default(), h.hits))
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
        // A built-in kind is its bare name.
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

        // A bare id that is stored comes back untouched.
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

        // A stored id of the wrong kind is converted and stored under a derived name.
        let resolved = resolve_argument("ocel", serde_json::json!("o1"), &schema, &state).unwrap();
        let resolved = resolved.as_str().unwrap();
        assert!(
            resolved.starts_with("o1_as_SlimLinkedOCEL_"),
            "{}",
            resolved
        );
        assert_eq!(
            state.items.read().unwrap()[resolved].kind(),
            RegistryItemKind::SlimLinkedOCEL
        );
        // The id wrapper converts the same way.
        let resolved =
            resolve_argument("ocel", serde_json::json!({ "id": "o1" }), &schema, &state).unwrap();
        assert!(resolved
            .as_str()
            .unwrap()
            .starts_with("o1_as_SlimLinkedOCEL_"));

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
    fn call_resolved_drops_the_converted_copy() {
        let state = AppState::default();
        let ocel: OCEL = serde_json::from_value(tiny_ocel_json()).unwrap();
        state.add("o1", ocel);
        let out = call_resolved(
            binding_named("num_objects"),
            &serde_json::json!({ "ocel": "o1" }),
            &state,
        )
        .unwrap();
        assert_eq!(out, b"2");
        let items = state.read();
        assert_eq!(items.keys().collect::<Vec<_>>(), vec!["o1"]);
    }

    #[test]
    fn mut_argument_is_not_converted_implicitly() {
        let schema = arg_schema("locel_add_event_type", "ocel");
        assert_eq!(schema["x-registry-mut"], serde_json::json!(true));
        assert!(arg_schema("num_objects", "ocel")
            .get("x-registry-mut")
            .is_none());

        let state = AppState::default();
        let ocel: OCEL = serde_json::from_value(tiny_ocel_json()).unwrap();
        state.add("o1", ocel);
        let err = call_resolved(
            binding_named("locel_add_event_type"),
            &serde_json::json!({ "ocel": "o1", "event_type": "new" }),
            &state,
        )
        .unwrap_err();
        assert!(err.contains("not converted implicitly"), "{}", err);
        let items = state.read();
        assert_eq!(items.keys().collect::<Vec<_>>(), vec!["o1"]);
        assert_eq!(items["o1"].kind(), RegistryItemKind::OCEL);
    }

    /// Mutate a stored handle, then panic halfway through.
    #[register_binding]
    fn dummy_bump_then_panic(#[bind(handle)] h: &mut DummyHandle) -> usize {
        h.hits += 1;
        panic!("half done");
    }

    #[test]
    fn panic_in_mut_binding_drops_the_item() {
        let state = AppState::default();
        state.add(
            "h1",
            RegistryItem::custom(DummyHandle {
                label: "x".to_string(),
                hits: 0,
            }),
        );
        let err = call(
            binding_named("dummy_bump_then_panic"),
            &serde_json::json!({ "h": "h1" }),
            &state,
        )
        .unwrap_err();
        assert!(err.contains("half done"), "{}", err);
        assert!(!state.contains_key("h1"));
        // The registry stays usable after the poisoned write lock.
        state.add(
            "h2",
            RegistryItem::custom(DummyHandle {
                label: "y".to_string(),
                hits: 0,
            }),
        );
        assert_eq!(
            call(
                binding_named("dummy_bump"),
                &serde_json::json!({ "h": "h2" }),
                &state
            )
            .unwrap(),
            b"1"
        );
    }

    /// Mutate a stored handle, report progress, then panic.
    #[register_binding]
    fn dummy_bump_report_then_panic(
        #[bind(handle)] h: &mut DummyHandle,
        #[bind(ctx)] ctl: &Control<'_>,
    ) -> usize {
        h.hits += 1;
        ctl.progress("bump", 1, Some(1));
        panic!("half done");
    }

    #[test]
    fn panic_after_cancel_is_reported_as_the_panic() {
        use std::sync::atomic::{AtomicBool, Ordering};
        let state = AppState::default();
        state.add(
            "h1",
            RegistryItem::custom(DummyHandle {
                label: "x".to_string(),
                hits: 0,
            }),
        );
        let flag = AtomicBool::new(false);
        let ctl = Control::new()
            .on_progress(|_| flag.store(true, Ordering::Relaxed))
            .cancel_flag(&flag);
        let out = call_with_outcome(
            binding_named("dummy_bump_report_then_panic"),
            &serde_json::json!({ "h": "h1" }),
            &state,
            &ctl,
        );
        match out {
            Err(CallError::Failed(msg)) => {
                assert!(msg.contains("half done"), "{}", msg);
                assert!(
                    msg.contains("removed possibly inconsistent item(s): h1"),
                    "{}",
                    msg
                );
            }
            other => panic!("expected the panic, got {:?}", other),
        }
        assert!(!state.contains_key("h1"));
    }

    #[test]
    fn add_event_with_unknown_object_errors_and_leaves_ocel_usable() {
        let state = AppState::default();
        state.add("l", SlimLinkedOCEL::new());
        let run = |name: &str, args: Value| call(binding_named(name), &args, &state);
        run(
            "locel_add_event_type",
            serde_json::json!({ "ocel": "l", "event_type": "a" }),
        )
        .unwrap();
        let t = "2024-01-01T00:00:00Z";
        let err = run(
            "locel_add_event",
            serde_json::json!({
                "ocel": "l", "event_type": "a", "time": t, "id": "e1",
                "relationships": [["q", 3]]
            }),
        )
        .unwrap_err();
        assert!(err.contains("object index 3"), "{}", err);
        let out = run(
            "locel_add_event",
            serde_json::json!({ "ocel": "l", "event_type": "a", "time": t, "id": "e1" }),
        )
        .unwrap();
        assert_eq!(out, b"0");
        assert_eq!(
            run("num_events", serde_json::json!({ "ocel": "l" })).unwrap(),
            b"1"
        );
    }

    #[test]
    fn binding_names_are_unique() {
        let mut seen = HashSet::new();
        let dupes: Vec<&str> = list_functions()
            .into_iter()
            .map(|b| b.name)
            .filter(|n| !seen.insert(*n))
            .collect();
        assert!(dupes.is_empty(), "registered more than once: {:?}", dupes);
    }

    #[test]
    fn call_resolved_accepts_every_reference_form() {
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
            "dummy_index_link",
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

        // The insert sites under the write lock mint `res_` ids too.
        let id = called_id("dummy_fork", serde_json::json!({ "h": "d1" }), &state);
        assert!(id.starts_with("res_"), "got {}", id);
        let id = called_id("dummy_log_of", serde_json::json!({ "h": "d1" }), &state);
        assert!(id.starts_with("res_"), "got {}", id);
    }

    #[test]
    fn a_binding_without_registry_arguments_holds_no_lock_during_its_body() {
        use std::sync::atomic::{AtomicBool, Ordering};
        let state = AppState::default();
        let writable = AtomicBool::new(false);
        let ctl = Control::new().on_progress(|_| {
            writable.store(state.items.try_write().is_ok(), Ordering::Relaxed);
        });
        call_with_outcome(
            binding_named("dummy_new_with_len"),
            &serde_json::json!({ "label": "x", "output_id": "p" }),
            &state,
            &ctl,
        )
        .unwrap();
        assert!(writable.load(Ordering::Relaxed));
    }

    #[test]
    fn a_pair_stores_its_first_element_under_output_id_and_returns_the_second() {
        let state = AppState::default();
        state.add(
            "d1",
            RegistryItem::custom(DummyHandle {
                label: "src".to_string(),
                hits: 0,
            }),
        );

        // Custom handle, read-lock path.
        let out = call(
            binding_named("dummy_new_with_len"),
            &serde_json::json!({ "label": "three", "output_id": "p" }),
            &state,
        )
        .unwrap();
        assert_eq!(out, b"5");
        assert_eq!(stored_handle_at(&state, "p").label, "three");

        // An existing id is replaced.
        let before = state.read().len();
        call(
            binding_named("dummy_new_with_len"),
            &serde_json::json!({ "label": "again", "output_id": "p" }),
            &state,
        )
        .unwrap();
        assert_eq!(stored_handle_at(&state, "p").label, "again");
        assert_eq!(state.read().len(), before);

        // Big type, write-guard path.
        let out = call(
            binding_named("dummy_log_and_hits"),
            &serde_json::json!({ "h": "d1", "output_id": "log" }),
            &state,
        )
        .unwrap();
        assert_eq!(out, b"1");
        assert_eq!(state.read()["log"].kind(), RegistryItemKind::EventLog);

        // Without an id the body does not run and nothing is stored.
        let before = state.read().len();
        for args in [
            serde_json::json!({ "h": "d1" }),
            serde_json::json!({ "h": "d1", "output_id": null }),
        ] {
            let err = call(binding_named("dummy_log_and_hits"), &args, &state).unwrap_err();
            assert!(err.contains("output_id"), "{}", err);
        }
        let err = call(
            binding_named("dummy_new_with_len"),
            &serde_json::json!({ "label": "x" }),
            &state,
        )
        .unwrap_err();
        assert_eq!(err, "Missing required argument output_id");
        assert_eq!(state.read().len(), before);
        assert_eq!(stored_handle_at(&state, "d1").hits, 1);
    }

    #[test]
    fn output_id_is_declared_only_where_a_handle_is_returned() {
        for name in [
            "dummy_new",
            "dummy_fork",
            "dummy_log_of",
            "dummy_index_link",
            "dummy_slim_link",
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
        assert!(arg_schema("dummy_new", "output_id")
            .get("x-registry-out")
            .is_none());

        // A pair's result is its value, so the caller must name the id itself.
        for (name, kind) in [
            ("dummy_new_with_len", "DummyHandle"),
            ("dummy_log_and_hits", "EventLog"),
        ] {
            let binding = binding_named(name);
            let schema = arg_schema(name, "output_id");
            assert_eq!(schema["type"], serde_json::json!("string"));
            assert_eq!(schema["title"], serde_json::json!("output_id"));
            assert!(schema["description"].is_string(), "{}", name);
            assert_eq!(schema["x-registry-out"], serde_json::json!(kind));
            assert_eq!(
                (binding.required_args)().last().map(String::as_str),
                Some("output_id")
            );
            let names: Vec<String> = (binding.args)().into_iter().map(|(n, _)| n).collect();
            assert_eq!(names.last().unwrap(), "output_id");
            let ret = (binding.return_type)();
            assert!(ret.get("x-registry-ref").is_none(), "{}", name);
            assert_eq!(ret["type"], serde_json::json!("integer"), "{}", name);
        }

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

    fn sample_item(kind: RegistryItemKind) -> RegistryItem {
        let ocel: OCEL = serde_json::from_value(tiny_ocel_json()).unwrap();
        match kind {
            RegistryItemKind::EventLog => EventLog::default().into(),
            RegistryItemKind::EventLogActivityProjection => {
                EventLogActivityProjection::from(&EventLog::default()).into()
            }
            RegistryItemKind::OCEL => ocel.into(),
            RegistryItemKind::IndexLinkedOCEL => IndexLinkedOCEL::from_ocel(ocel).into(),
            RegistryItemKind::SlimLinkedOCEL => SlimLinkedOCEL::from_ocel(ocel).into(),
            RegistryItemKind::TabularSource => {
                RegistryItem::TabularSource(TabularSource::new(Vec::new(), "csv"))
            }
            RegistryItemKind::Custom(_) => RegistryItem::custom(DummyHandle {
                label: "x".to_string(),
                hits: 0,
            }),
        }
    }

    #[test]
    fn conversion_table_matches_convert() {
        let mut kinds = RegistryItemKind::all_kinds().to_vec();
        kinds.push(RegistryItemKind::Custom("DummyHandle"));
        for from in &kinds {
            for to in &kinds {
                let listed = from.convertible_to().contains(to);
                match sample_item(*from).convert(*to) {
                    Ok(item) => assert!(listed && item.kind() == *to, "{} -> {}", from, to),
                    Err(_) => assert!(!listed, "{} -> {} is listed but fails", from, to),
                }
                match sample_item(*from).convert_into(*to) {
                    Ok(item) => assert!(listed && item.kind() == *to, "{} -> {}", from, to),
                    Err((item, _)) => {
                        assert!(!listed, "{} -> {} is listed but fails", from, to);
                        assert_eq!(item.kind(), *from, "the item comes back on error");
                    }
                }
            }
        }
        assert!(RegistryItemKind::SlimLinkedOCEL
            .convertible_to()
            .contains(&RegistryItemKind::IndexLinkedOCEL));
    }

    #[test]
    fn slim_and_index_linked_convert_into_each_other() {
        let RegistryItem::IndexLinkedOCEL(index) = sample_item(RegistryItemKind::SlimLinkedOCEL)
            .convert(RegistryItemKind::IndexLinkedOCEL)
            .unwrap()
        else {
            unreachable!()
        };
        assert_eq!(index.get_num_obs(), 2);
        let RegistryItem::SlimLinkedOCEL(slim) = RegistryItem::IndexLinkedOCEL(index)
            .convert_into(RegistryItemKind::SlimLinkedOCEL)
            .unwrap()
        else {
            unreachable!()
        };
        assert_eq!(slim.get_num_obs(), 2);
    }

    #[test]
    fn call_with_outcome_reports_minted_and_mutated_ids() {
        let state = AppState::default();
        state.add(
            "d1",
            RegistryItem::custom(DummyHandle {
                label: "one".to_string(),
                hits: 0,
            }),
        );

        let out = call_with_outcome(
            binding_named("dummy_fork"),
            &serde_json::json!({ "h": "d1", "output_id": "forked" }),
            &state,
            &Control::new(),
        )
        .unwrap();
        assert_eq!(out.result, b"\"forked\"");
        assert_eq!(out.minted, vec!["forked"]);
        assert_eq!(out.mutated, vec!["d1"]);

        let out = call_with_outcome(
            binding_named("dummy_new"),
            &serde_json::json!({ "label": "two" }),
            &state,
            &Control::new(),
        )
        .unwrap();
        assert_eq!(out.minted.len(), 1);
        assert!(state.contains_key(&out.minted[0]));
        assert!(out.mutated.is_empty());

        // A pair reports the id it stored under, not its value.
        let out = call_with_outcome(
            binding_named("dummy_log_and_hits"),
            &serde_json::json!({ "h": "d1", "output_id": "pair_log" }),
            &state,
            &Control::new(),
        )
        .unwrap();
        assert_eq!(out.result, b"2");
        assert_eq!(out.minted, vec!["pair_log"]);
        assert_eq!(out.mutated, vec!["d1"]);
        let out = call_resolved_with_outcome(
            binding_named("dummy_new_with_len"),
            &serde_json::json!({ "label": "ab", "output_id": "pair_handle" }),
            &state,
            &Control::new(),
        )
        .unwrap();
        assert_eq!(out.result, b"2");
        assert_eq!(out.minted, vec!["pair_handle"]);
        assert!(state.contains_key("pair_handle"));

        // A string result is not mistaken for an id when the binding returns no handle.
        let out = call_with_outcome(
            binding_named("dummy_label"),
            &serde_json::json!({ "h": "d1" }),
            &state,
            &Control::new(),
        )
        .unwrap();
        assert_eq!(out.result, b"\"one\"");
        assert!(out.minted.is_empty() && out.mutated.is_empty());

        // Resolution reports what the binding got: the stored id it mutated, and the result
        // stored from an inline argument, but not the temporary copy of that argument.
        state.add("l", SlimLinkedOCEL::new());
        let out = call_resolved_with_outcome(
            binding_named("locel_add_event_type"),
            &serde_json::json!({ "ocel": { "id": "l" }, "event_type": "a" }),
            &state,
            &Control::new(),
        )
        .unwrap();
        assert_eq!(out.mutated, vec!["l"]);
        for ocel in [
            tiny_ocel_json(),
            serde_json::json!({ "path": "missing.jsonocel" }),
            serde_json::json!("not-stored"),
        ] {
            let err = call_resolved_with_outcome(
                binding_named("locel_add_event_type"),
                &serde_json::json!({ "ocel": ocel, "event_type": "a" }),
                &state,
                &Control::new(),
            )
            .unwrap_err()
            .to_string();
            assert!(err.starts_with("Invalid Argument: ocel\n"), "{err}");
            assert!(err.contains("needs the ID of a stored"), "{err}");
        }
        assert!(state.read().keys().all(|k| !k.starts_with("Aocel_")));
        let out = call_resolved_with_outcome(
            binding_named("dummy_index_link"),
            &serde_json::json!({ "ocel": tiny_ocel_json() }),
            &state,
            &Control::new(),
        )
        .unwrap();
        assert_eq!(out.minted.len(), 1);
        assert_eq!(
            state.read()[&out.minted[0]].kind(),
            RegistryItemKind::IndexLinkedOCEL
        );
        assert!(state.read().keys().all(|k| !k.starts_with("Aocel_")));
    }

    #[test]
    fn context_reports_progress_and_partials() {
        use std::sync::Mutex;
        let state = AppState::default();
        let steps = Mutex::new(Vec::new());
        let partials = Mutex::new(Vec::new());
        let ctl = Control::new()
            .on_progress(|p| steps.lock().unwrap().push(p.done))
            .on_partial(|v| partials.lock().unwrap().push(v));
        let binding = binding_named("dummy_count");
        let out = call_with_outcome(binding, &serde_json::json!({ "n": 3 }), &state, &ctl);
        assert_eq!(out.unwrap().result, b"3");
        drop(ctl);
        assert_eq!(steps.into_inner().unwrap(), vec![0, 1, 2, 3]);
        assert_eq!(
            partials.into_inner().unwrap(),
            vec![
                serde_json::json!(0),
                serde_json::json!(1),
                serde_json::json!(2)
            ]
        );
        assert!(!(binding.args)().iter().any(|(name, _)| name == "ctl"));
        assert!(!(binding.required_args)().contains(&"ctl".to_string()));
    }

    #[test]
    fn resolving_a_path_reports_the_import_and_honors_cancel() {
        use std::sync::atomic::{AtomicBool, Ordering};
        use std::sync::Mutex;
        let path = get_test_data_path()
            .join("ocel")
            .join("order-management.json");
        let len = std::fs::metadata(&path).unwrap().len();
        let args = serde_json::json!({ "ocel": { "path": path.to_str().unwrap() } });
        let state = AppState::default();
        let seen = Mutex::new(Vec::new());
        let ctl = Control::new().on_progress(|p| seen.lock().unwrap().push(p));
        call_resolved_with_outcome(binding_named("num_events"), &args, &state, &ctl).unwrap();
        drop(ctl);
        let import: Vec<_> = seen
            .into_inner()
            .unwrap()
            .into_iter()
            .filter(|p| p.stage == "Importing ocel")
            .collect();
        assert!(import.len() > 2);
        assert!(import.iter().all(|p| p.total == Some(len)));
        assert_eq!(import.last().unwrap().done, len);

        let flag = AtomicBool::new(false);
        let ctl = Control::new()
            .on_progress(|_| flag.store(true, Ordering::Relaxed))
            .cancel_flag(&flag);
        let out = call_resolved_with_outcome(binding_named("num_events"), &args, &state, &ctl);
        assert_eq!(out, Err(CallError::Cancelled));
        assert!(state.read().is_empty());
    }

    #[test]
    fn control_loads_registry_items_with_progress_and_cancel() {
        use std::sync::atomic::{AtomicBool, Ordering};
        use std::sync::Mutex;
        let path = get_test_data_path()
            .join("ocel")
            .join("order-management.json");
        let bytes = std::fs::read(&path).unwrap();
        let seen = Mutex::new(Vec::new());
        let ctl = Control::new().on_progress(|p| seen.lock().unwrap().push(p));
        let item = RegistryItem::load_from_path_with(
            &RegistryItemKind::SlimLinkedOCEL,
            path.to_str().unwrap(),
            &ctl,
        )
        .unwrap();
        assert_eq!(item.kind(), RegistryItemKind::SlimLinkedOCEL);
        let item =
            RegistryItem::load_from_bytes_with(&RegistryItemKind::OCEL, &bytes, "json", &ctl)
                .unwrap();
        assert_eq!(item.kind(), RegistryItemKind::OCEL);
        drop(ctl);
        let seen = seen.into_inner().unwrap();
        let len = bytes.len() as u64;
        assert_eq!(
            seen.iter()
                .filter(|p| p.stage == "Importing" && p.done == len && p.total == Some(len))
                .count(),
            2,
            "{seen:?}"
        );

        let flag = AtomicBool::new(false);
        let ctl = Control::new()
            .on_progress(|_| flag.store(true, Ordering::Relaxed))
            .cancel_flag(&flag);
        assert_eq!(
            RegistryItem::load_from_bytes_with(&RegistryItemKind::OCEL, &bytes, "json", &ctl)
                .map(|_| ()),
            Err(CallError::Cancelled)
        );
        assert!(matches!(
            SlimLinkedOCEL::import_from_path_with(&path, Default::default(), &ctl),
            Err(crate::core::event_data::object_centric::io::OCELIOError::Cancelled)
        ));
    }

    #[test]
    fn control_loads_xes_registry_items_with_progress_and_cancel() {
        use std::sync::atomic::{AtomicBool, Ordering};
        use std::sync::Mutex;
        let path = get_test_data_path()
            .join("xes")
            .join("Sepsis Cases - Event Log.xes.gz");
        let path_str = path.to_str().unwrap();
        let bytes = std::fs::read(&path).unwrap();
        let mut kinds = vec![
            RegistryItemKind::EventLog,
            RegistryItemKind::EventLogActivityProjection,
        ];
        if cfg!(feature = "extraction-blueprint") {
            kinds.extend([RegistryItemKind::OCEL, RegistryItemKind::SlimLinkedOCEL]);
        }
        let seen = Mutex::new(Vec::new());
        let ctl = Control::new().on_progress(|p| seen.lock().unwrap().push(p));
        for kind in &kinds {
            let item = RegistryItem::load_from_path_with(kind, path_str, &ctl).unwrap();
            assert_eq!(&item.kind(), kind);
            let item = RegistryItem::load_from_bytes_with(kind, &bytes, "xes.gz", &ctl).unwrap();
            assert_eq!(&item.kind(), kind);
        }
        drop(ctl);
        let seen = seen.into_inner().unwrap();
        let len = bytes.len() as u64;
        assert_eq!(
            seen.iter()
                .filter(|p| p.stage == "Importing" && p.done == len && p.total == Some(len))
                .count(),
            2 * kinds.len(),
            "{seen:?}"
        );

        let flag = AtomicBool::new(false);
        let ctl = Control::new()
            .on_progress(|_| flag.store(true, Ordering::Relaxed))
            .cancel_flag(&flag);
        for kind in &kinds {
            flag.store(false, Ordering::Relaxed);
            assert_eq!(
                RegistryItem::load_from_path_with(kind, path_str, &ctl).map(|_| ()),
                Err(CallError::Cancelled)
            );
            flag.store(false, Ordering::Relaxed);
            assert_eq!(
                RegistryItem::load_from_bytes_with(kind, &bytes, "xes.gz", &ctl).map(|_| ()),
                Err(CallError::Cancelled)
            );
        }
    }

    #[test]
    fn cancelled_call_returns_cancelled_and_stores_nothing() {
        use std::sync::atomic::{AtomicBool, Ordering};
        let state = AppState::default();
        let flag = AtomicBool::new(false);
        let ctl = Control::new()
            .on_progress(|p| {
                if p.done == 2 {
                    flag.store(true, Ordering::Relaxed);
                }
            })
            .cancel_flag(&flag);
        let out = call_with_outcome(
            binding_named("dummy_count"),
            &serde_json::json!({ "n": 10 }),
            &state,
            &ctl,
        );
        assert_eq!(out, Err(CallError::Cancelled));

        // Set during the body, after which the binding finishes anyway: still nothing is stored.
        let flag = AtomicBool::new(false);
        let ctl = Control::new()
            .on_progress(|_| flag.store(true, Ordering::Relaxed))
            .cancel_flag(&flag);
        let out = call_with_outcome(
            binding_named("dummy_new_ctx"),
            &serde_json::json!({ "label": "x", "output_id": "kept" }),
            &state,
            &ctl,
        );
        assert_eq!(out, Err(CallError::Cancelled));
        assert!(state.read().is_empty());

        let flag = AtomicBool::new(false);
        let ctl = Control::new()
            .on_progress(|_| flag.store(true, Ordering::Relaxed))
            .cancel_flag(&flag);
        let out = call_with_outcome(
            binding_named("dummy_new_with_len"),
            &serde_json::json!({ "label": "x", "output_id": "kept" }),
            &state,
            &ctl,
        );
        assert_eq!(out, Err(CallError::Cancelled));
        assert!(state.read().is_empty());

        // Already cancelled: the binding does not run.
        let out = call_with_outcome(
            binding_named("dummy_new"),
            &serde_json::json!({ "label": "x" }),
            &state,
            &ctl,
        );
        assert_eq!(out, Err(CallError::Cancelled));
        assert!(state.read().is_empty());
        assert_eq!(CallError::Cancelled.to_string(), "cancelled");
    }

    #[test]
    fn locel_getters_reject_out_of_range_indices() {
        let state = AppState::default();
        let ocel: OCEL = serde_json::from_value(tiny_ocel_json()).unwrap();
        state.add("l", SlimLinkedOCEL::from_ocel(ocel));
        let run = |name: &str, args: Value| call(binding_named(name), &args, &state);

        for name in [
            "locel_get_ev_id",
            "locel_get_ev_type_of",
            "locel_get_ev_time",
            "locel_get_e2o",
            "locel_get_full_ev",
        ] {
            let err = run(name, serde_json::json!({ "ocel": "l", "ev": 0 })).unwrap_err();
            assert!(
                err.contains("event index 0 is out of range"),
                "{}: {}",
                name,
                err
            );
        }
        let err = run(
            "locel_get_ev_attr_val",
            serde_json::json!({ "ocel": "l", "ev": 3, "attr_name": "x" }),
        )
        .unwrap_err();
        assert!(err.contains("out of range"), "{}", err);

        for name in [
            "locel_get_ob_id",
            "locel_get_ob_type_of",
            "locel_get_e2o_rev",
            "locel_get_o2o",
            "locel_get_o2o_rev",
            "locel_get_full_ob",
            "locel_get_ob_activity_trace",
        ] {
            let err = run(name, serde_json::json!({ "ocel": "l", "ob": 2 })).unwrap_err();
            assert!(
                err.contains("object index 2 is out of range: the OCEL has 2 object(s)"),
                "{}: {}",
                name,
                err
            );
        }
        let err = run(
            "locel_get_ob_attr_vals",
            serde_json::json!({ "ocel": "l", "ob": 9, "attr_name": "x" }),
        )
        .unwrap_err();
        assert!(err.contains("out of range"), "{}", err);

        assert_eq!(
            run(
                "locel_get_ob_id",
                serde_json::json!({ "ocel": "l", "ob": 1 })
            )
            .unwrap(),
            b"\"i2\""
        );
        // The schema's return type is the plain value, without the error.
        assert_eq!(
            (binding_named("locel_get_ob_id").return_type)()["type"],
            serde_json::json!("string")
        );
    }
}
