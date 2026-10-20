//! In-memory [`ExtractionSink`], backed by a [`SlimLinkedOCEL`].

use std::collections::HashMap;

use chrono::{DateTime, FixedOffset};

use crate::core::event_data::object_centric::linked_ocel::slim_linked_ocel::{
    EventIndex, ObjectIndex,
};
use crate::core::event_data::object_centric::linked_ocel::{LinkedOCELAccess, SlimLinkedOCEL};
use crate::core::event_data::object_centric::{OCELAttributeValue, OCELTypeAttribute};

use super::sink::{EventRef, ExtractionSink, FinalizeReport, ObjectRef, Resolution, SinkError};

/// In-memory [`ExtractionSink`], backed by a [`SlimLinkedOCEL`].
///
/// The handles this sink hands out are always [`EventRef::Index`] / [`ObjectRef::Index`],
/// wrapping the same [`EventIndex`]/[`ObjectIndex`] `SlimLinkedOCEL` itself uses, so relation
/// wiring is a direct index operation with no extra lookup.
///
/// `SlimLinkedOCEL::add_event`/`add_object` require attribute values positioned to match the
/// declared type's attribute order exactly (by position, not by name), so this sink keeps its
/// own name -> position mirror per type, built as [`declare_event_type`](ExtractionSink::declare_event_type)
/// / [`declare_object_type`](ExtractionSink::declare_object_type) are called, and uses it to
/// reorder the name/value pairs `add_event`/`add_object` are given.
#[derive(Debug, Default)]
pub struct SlimOcelSink {
    locel: SlimLinkedOCEL,
    event_attr_order: HashMap<String, AttributeOrder>,
    object_attr_order: HashMap<String, AttributeOrder>,
}

/// A value offered under a name the entity's type never declared. Refused rather than dropped, so
/// a driver that adds an entity before declaring every attribute does not lose values silently.
fn undeclared(name: &str) -> SinkError {
    SinkError::Backend(format!(
        "attribute '{name}' was never declared for this entity's type"
    ))
}

impl SlimOcelSink {
    /// An empty sink.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// The OCEL built so far.
    #[must_use]
    pub fn ocel(&self) -> &SlimLinkedOCEL {
        &self.locel
    }

    /// Consume the sink, returning the OCEL it built.
    #[must_use]
    pub fn into_ocel(self) -> SlimLinkedOCEL {
        self.locel
    }

    fn event_index(r: &EventRef) -> Result<EventIndex, SinkError> {
        match r {
            EventRef::Index(i) => Ok(EventIndex::from(*i)),
            EventRef::Id(_) => Err(SinkError::InvalidRef),
        }
    }

    fn object_index(r: &ObjectRef) -> Result<ObjectIndex, SinkError> {
        match r {
            ObjectRef::Index(i) => Ok(ObjectIndex::from(*i)),
            ObjectRef::Id(_) => Err(SinkError::InvalidRef),
        }
    }
}

/// Whether an endpoint's declared type (`None` = "any") accepts an entity of type `actual`.
fn type_matches(declared: Option<&str>, actual: &str) -> bool {
    declared.is_none_or(|t| t == actual)
}

/// Extend `order` with any name in `attrs` not already present, preserving first-seen order --
/// mirrors the merge [`SlimLinkedOCEL::add_event_type`]/`add_object_type` themselves apply, so
/// positions agree.
fn merge_order(order: &mut AttributeOrder, attrs: &[OCELTypeAttribute]) {
    for a in attrs {
        order.push(&a.name);
    }
}

/// One type's attribute names, in declared order, with the name -> position map a `(name, value)`
/// pair resolves through.
///
/// The map is what keeps a wide type linear: scanning `names` for each position instead is
/// quadratic in the type's attribute count, paid once per entity.
#[derive(Debug, Default)]
struct AttributeOrder {
    names: Vec<String>,
    positions: HashMap<String, usize>,
}

impl AttributeOrder {
    fn push(&mut self, name: &str) {
        if !self.positions.contains_key(name) {
            self.positions.insert(name.to_string(), self.names.len());
            self.names.push(name.to_string());
        }
    }

    fn len(&self) -> usize {
        self.names.len()
    }

    fn position(&self, name: &str) -> Option<usize> {
        self.positions.get(name).copied()
    }
}

/// Insert `(time, value)` into an attribute's history unless that instant is already taken,
/// keeping the history sorted by time so the test is a binary search rather than a scan of every
/// change recorded so far.
fn record_at(
    history: &mut Vec<(DateTime<FixedOffset>, OCELAttributeValue)>,
    time: DateTime<FixedOffset>,
    value: OCELAttributeValue,
) {
    if let Err(pos) = history.binary_search_by(|(t, _)| t.cmp(&time)) {
        history.insert(pos, (time, value));
    }
}

impl ExtractionSink for SlimOcelSink {
    fn declare_event_type(
        &mut self,
        name: &str,
        attrs: &[OCELTypeAttribute],
    ) -> Result<(), SinkError> {
        merge_order(
            self.event_attr_order.entry(name.to_string()).or_default(),
            attrs,
        );
        self.locel.add_event_type(name, attrs.to_vec());
        Ok(())
    }

    fn declare_object_type(
        &mut self,
        name: &str,
        attrs: &[OCELTypeAttribute],
    ) -> Result<(), SinkError> {
        merge_order(
            self.object_attr_order.entry(name.to_string()).or_default(),
            attrs,
        );
        self.locel.add_object_type(name, attrs.to_vec());
        Ok(())
    }

    fn add_event(
        &mut self,
        event_type: &str,
        time: DateTime<FixedOffset>,
        id: &str,
        attributes: &[(String, OCELAttributeValue)],
    ) -> Result<EventRef, SinkError> {
        let order =
            self.event_attr_order
                .get(event_type)
                .ok_or_else(|| SinkError::UnknownType {
                    kind: "event",
                    name: event_type.to_string(),
                })?;
        let mut values = vec![OCELAttributeValue::Null; order.len()];
        for (name, value) in attributes {
            let pos = order.position(name).ok_or_else(|| undeclared(name))?;
            values[pos] = value.clone();
        }
        self.locel
            .add_event(event_type, time, Some(id.to_string()), values, Vec::new())
            .map(|idx| EventRef::Index(idx.into_inner()))
            .ok_or_else(|| SinkError::DuplicateEvent { id: id.to_string() })
    }

    fn add_object(
        &mut self,
        object_type: &str,
        id: &str,
        attributes: &[(String, DateTime<FixedOffset>, OCELAttributeValue)],
    ) -> Result<ObjectRef, SinkError> {
        let order =
            self.object_attr_order
                .get(object_type)
                .ok_or_else(|| SinkError::UnknownType {
                    kind: "object",
                    name: object_type.to_string(),
                })?;
        let mut values: Vec<Vec<(DateTime<FixedOffset>, OCELAttributeValue)>> =
            vec![Vec::new(); order.len()];
        for (name, time, value) in attributes {
            let pos = order.position(name).ok_or_else(|| undeclared(name))?;
            // The same first-wins rule `add_object_attribute` applies, for the case where one
            // call carries two values for one `(name, time)`. Both paths must agree, or an
            // object's attribute history would depend on whether the row that wrote it also
            // created the object.
            record_at(&mut values[pos], *time, value.clone());
        }
        self.locel
            .add_object(object_type, Some(id.to_string()), values, Vec::new())
            .map(|idx| ObjectRef::Index(idx.into_inner()))
            .ok_or_else(|| SinkError::DuplicateObject { id: id.to_string() })
    }

    /// First-wins on `(name, time)`, this sink's answer to the contract on
    /// [`ExtractionSink::add_object_attribute`].
    fn add_object_attribute(
        &mut self,
        object: &ObjectRef,
        name: &str,
        time: DateTime<FixedOffset>,
        value: OCELAttributeValue,
    ) -> Result<(), SinkError> {
        let idx = Self::object_index(object)?;
        match idx.get_attribute_value_mut(name, &mut self.locel) {
            Some(history) => {
                record_at(history, time, value);
                Ok(())
            }
            None => Err(undeclared(name)),
        }
    }

    /// Always [`Resolution::Exists`] or [`Resolution::Missing`], never
    /// [`Resolution::Deferred`]: the whole log is in memory, so there is nothing to defer.
    ///
    /// An event of this id under a different type answers `Missing`, not `Exists`: no event of
    /// the requested type carries that id, and answering `Exists` would silently merge two types
    /// that happen to share an id under `IdRendering::Raw`.
    fn resolve_event(&mut self, id: &str, event_type: Option<&str>) -> Resolution<EventRef> {
        match self.locel.get_ev_by_id(id) {
            Some(i) => {
                if type_matches(event_type, i.get_ev_type(&self.locel)) {
                    Resolution::Exists(EventRef::Index(i.into_inner()))
                } else {
                    Resolution::Missing
                }
            }
            None => Resolution::Missing,
        }
    }

    /// See [`resolve_event`](ExtractionSink::resolve_event).
    fn resolve_object(&mut self, id: &str, object_type: Option<&str>) -> Resolution<ObjectRef> {
        match self.locel.get_ob_by_id(id) {
            Some(i) => {
                if type_matches(object_type, i.get_ob_type(&self.locel)) {
                    Resolution::Exists(ObjectRef::Index(i.into_inner()))
                } else {
                    Resolution::Missing
                }
            }
            None => Resolution::Missing,
        }
    }

    /// Nothing to do: this sink defers nothing, so every counter is zero and the caller's
    /// per-mapping stats already hold the whole story.
    fn finalize(&mut self) -> Result<FinalizeReport, SinkError> {
        Ok(FinalizeReport::default())
    }

    fn add_e2o(
        &mut self,
        event: &EventRef,
        object: &ObjectRef,
        qualifier: &str,
    ) -> Result<(), SinkError> {
        let e = Self::event_index(event)?;
        let o = Self::object_index(object)?;
        if self.locel.add_e2o(e, o, qualifier.to_string()) {
            Ok(())
        } else {
            Err(SinkError::InvalidRef)
        }
    }

    fn add_o2o(
        &mut self,
        source: &ObjectRef,
        target: &ObjectRef,
        qualifier: &str,
    ) -> Result<(), SinkError> {
        let s = Self::object_index(source)?;
        let t = Self::object_index(target)?;
        if self.locel.add_o2o(s, t, qualifier.to_string()) {
            Ok(())
        } else {
            Err(SinkError::InvalidRef)
        }
    }
}
