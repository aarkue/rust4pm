//! OCEL Struct for Efficient Usage of Relations
use std::borrow::Cow;

use crate::core::event_data::object_centric::OCELAttributeValue;

use super::ocel_struct::{OCELEvent, OCELObject, OCELType};

#[allow(unused_imports)]
use super::ocel_struct::OCEL;

/// An [`OCEL`] linked based on event and object indices
///
/// The resulting [`IndexLinkedOCEL`] conveniently represents objects or events by their index in the [`OCEL`].
pub mod index_linked_ocel;
use chrono::{DateTime, FixedOffset};
pub use index_linked_ocel::IndexLinkedOCEL;
/// An [`OCEL`] linked through object/event identifiers (i.e., [`String`]s)
pub mod id_linked_ocel;
pub mod slim_linked_ocel;
pub use id_linked_ocel::IDLinkedOCEL;
pub use slim_linked_ocel::SlimLinkedOCEL;

/// Linked access to an [`OCEL`], making it easy to follow event-to-object and object-to-object relationships, as well as their reverse
///
/// See also [`IndexLinkedOCEL`] and [`IDLinkedOCEL`].
pub trait LinkedOCELAccess<'a> {
    /// Return and argument type/representation for events (i.e., what type is returned when events are accessed, e.g., through [`LinkedOCELAccess::get_e2o_rev`])
    type EventRepr: 'a;
    /// Return and argument type/representation for objects (i.e., what type is returned when objects are accessed, e.g., through [`LinkedOCELAccess::get_e2o`])
    type ObjectRepr: 'a;

    /// Get all events of the given event type (activity)
    fn get_evs_of_type(&'a self, ev_type: &'_ str) -> impl Iterator<Item = &'a Self::EventRepr>;
    /// Get all object of the given object type
    fn get_obs_of_type(&'a self, ob_type: &'_ str) -> impl Iterator<Item = &'a Self::ObjectRepr>;

    /// Get all event types (activities)
    fn get_ev_types(&'a self) -> impl Iterator<Item = &'a str>;
    /// Get all object types
    fn get_ob_types(&'a self) -> impl Iterator<Item = &'a str>;

    /// Get all events
    ///
    /// Also see [`LinkedOCELAccess::get_all_evs_ref`].
    fn get_all_evs(&'a self) -> impl Iterator<Item = Cow<'a, OCELEvent>>;
    /// Get all objects
    ///
    /// Also see [`LinkedOCELAccess::get_all_obs_ref`].
    fn get_all_obs(&'a self) -> impl Iterator<Item = Cow<'a, OCELObject>>;

    /// Get all event references
    ///
    /// In contrast to [`LinkedOCELAccess::get_all_evs`], this does not necessarily return direct event references (i.e., &[`OCELEvent`]), but the linked-access specific representation of events
    fn get_all_evs_ref(&'a self) -> impl Iterator<Item = &'a Self::EventRepr>;

    /// Get all object references
    ///
    /// In contrast to [`LinkedOCELAccess::get_all_obs`], this does not necessarily return direct object references (i.e., &[`OCELObject`]), but the linked-access specific representation of objects
    fn get_all_obs_ref(&'a self) -> impl Iterator<Item = &'a Self::ObjectRepr>;

    /// Get an event reference based on the linked-access specific representation of an event
    fn get_ev(&'a self, index: &Self::EventRepr) -> Cow<'a, OCELEvent>;

    /// Get an object reference based on the linked-access specific representation of an object
    fn get_ob(&'a self, index: &Self::ObjectRepr) -> Cow<'a, OCELObject>;
    /// Get the object type of an object reference
    fn get_ob_type_of(&'a self, object: &Self::ObjectRepr) -> &'a str;

    /// Get the event type (i.e., activity) of an event reference
    fn get_ev_type_of(&'a self, event: &Self::EventRepr) -> &'a str;

    /// Get all objects related to the given event (through E2O (event-to-object) relations)
    fn get_e2o(
        &'a self,
        index: &Self::EventRepr,
    ) -> impl Iterator<Item = (&'a str, &'a Self::ObjectRepr)>;

    /// Get all events to which the given object is related (through the reverse E2O (event-to-object) relations)
    fn get_e2o_rev(
        &'a self,
        index: &Self::ObjectRepr,
    ) -> impl Iterator<Item = (&'a str, &'a Self::EventRepr)>;

    /// Get all objects related to the given object (through O2O (object-to-object) relations)
    fn get_o2o(
        &'a self,
        index: &Self::ObjectRepr,
    ) -> impl Iterator<Item = (&'a str, &'a Self::ObjectRepr)>;

    /// Get all objects inversely related to the given object (through **reverse** O2O (object-to-object) relations)
    fn get_o2o_rev(
        &'a self,
        index: &Self::ObjectRepr,
    ) -> impl Iterator<Item = (&'a str, &'a Self::ObjectRepr)>;

    /// Get event type ([`OCELType`]) from type name (i.e., activity)
    fn get_ev_type(&'a self, ev_type: impl AsRef<str>) -> Option<&'a OCELType>;

    /// Get object type ([`OCELType`]) from type name
    fn get_ob_type(&'a self, ob_type: impl AsRef<str>) -> Option<&'a OCELType>;

    /// Get the names of all attributes that an event has
    fn get_ev_attrs(&'a self, ev: &Self::EventRepr) -> impl Iterator<Item = &'a str>;
    /// Get the value assigned to an event attribute (by name) for an event
    fn get_ev_attr_val(
        &'a self,
        ev: &Self::EventRepr,
        attr_name: impl AsRef<str>,
    ) -> Option<&'a OCELAttributeValue>;

    /// Get the names of all attributes that an object has
    fn get_ob_attrs(&'a self, ob: &Self::ObjectRepr) -> impl Iterator<Item = &'a str>;

    /// Get the value assigned to an object attribute (by name) for an object
    fn get_ob_attr_vals(
        &'a self,
        ob: &Self::ObjectRepr,
        attr_name: impl AsRef<str>,
    ) -> impl Iterator<Item = (&'a DateTime<FixedOffset>, &'a OCELAttributeValue)>;

    /// Get the ID of an object
    fn get_ob_id(&'a self, ob: &Self::ObjectRepr) -> &'a str;

    /// Get the ID of an event
    fn get_ev_id(&'a self, ev: &Self::EventRepr) -> &'a str;

    /// Get an event based on its ID
    fn get_ev_by_id(&'a self, ev_id: impl AsRef<str>) -> Option<Self::EventRepr>;

    /// Get an object based on its ID
    fn get_ob_by_id(&'a self, ob_id: impl AsRef<str>) -> Option<Self::ObjectRepr>;

    /// Get timestamp of an event
    fn get_ev_time(&'a self, ev: &Self::EventRepr) -> &'a DateTime<FixedOffset>;
}
