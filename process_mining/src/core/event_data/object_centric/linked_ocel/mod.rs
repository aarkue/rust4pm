//! OCEL Struct for Efficient Usage of Relations
use std::borrow::{Borrow, Cow};

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

    /// Get all events in the dataset
    fn get_all_evs(&'a self) -> impl Iterator<Item = Self::EventRepr>;

    /// Get all objects in the dataset
    fn get_all_obs(&'a self) -> impl Iterator<Item = Self::ObjectRepr>;
    /// Get all objects related to the given event (through E2O (event-to-object) relations)
    fn get_e2o(
        &'a self,
        index: impl Borrow<Self::EventRepr>,
    ) -> impl Iterator<Item = (&'a str, &'a Self::ObjectRepr)>;

    /// Get all events to which the given object is related (through the reverse E2O (event-to-object) relations)
    fn get_e2o_rev(
        &'a self,
        index: impl Borrow<Self::ObjectRepr>,
    ) -> impl Iterator<Item = (&'a str, &'a Self::EventRepr)>;

    /// Get all objects related to the given object (through O2O (object-to-object) relations)
    fn get_o2o(
        &'a self,
        index: impl Borrow<Self::ObjectRepr>,
    ) -> impl Iterator<Item = (&'a str, &'a Self::ObjectRepr)>;

    /// Get all objects (in reverse) related to the given object (through **reverse** O2O (object-to-object) relations)
    fn get_o2o_rev(
        &'a self,
        index: impl Borrow<Self::ObjectRepr>,
    ) -> impl Iterator<Item = (&'a str, &'a Self::ObjectRepr)>;

    /// Get all objects of a specified type related with the given event
    fn get_e2o_of_type(
        &'a self,
        index: impl Borrow<Self::EventRepr>,
        ob_type: impl AsRef<str>,
    ) -> impl Iterator<Item = (&'a str, &'a Self::ObjectRepr)> {
        self.get_e2o(index)
            .filter(move |(_q, o)| self.get_ob_type_of(o.borrow()) == ob_type.as_ref())
    }
    /// Get all events of a specified type associated with the given object (through reverse E2O relations)
    fn get_e2o_rev_of_type(
        &'a self,
        index: impl Borrow<Self::ObjectRepr>,
        ev_type: impl AsRef<str>,
    ) -> impl Iterator<Item = (&'a str, &'a Self::EventRepr)> {
        self.get_e2o_rev(index)
            .filter(move |(_q, o)| self.get_ev_type_of(o.borrow()) == ev_type.as_ref())
    }

    /// Get all objects of a specified type related with the given object through an O2O relationship (from the given object, i.e., through O2O relations)
    fn get_o2o_of_type(
        &'a self,
        from_obj: impl Borrow<Self::ObjectRepr>,
        to_ob_type: impl AsRef<str>,
    ) -> impl Iterator<Item = (&'a str, &'a Self::ObjectRepr)> {
        self.get_o2o(from_obj)
            .filter(move |(_q, o)| self.get_ob_type_of(o.borrow()) == to_ob_type.as_ref())
    }
    /// Get all objects of a specified type that have an O2O relationship _to_ the given object (through _reverse_ O2O relations)
    fn get_o2o_rev_of_type(
        &'a self,
        to_obj: impl Borrow<Self::ObjectRepr>,
        from_ob_type: impl AsRef<str>,
    ) -> impl Iterator<Item = (&'a str, &'a Self::ObjectRepr)> {
        self.get_o2o_rev(to_obj)
            .filter(move |(_q, o)| self.get_ob_type_of(o.borrow()) == from_ob_type.as_ref())
    }

    /// Get the object type of an object reference
    fn get_ob_type_of(&'a self, object: impl Borrow<Self::ObjectRepr>) -> &'a str;

    /// Get the event type (i.e., activity) of an event reference
    fn get_ev_type_of(&'a self, event: impl Borrow<Self::EventRepr>) -> &'a str;

    /// Get the ID of an object
    fn get_ob_id(&'a self, ob: impl Borrow<Self::ObjectRepr>) -> &'a str;

    /// Get the ID of an event
    fn get_ev_id(&'a self, ev: impl Borrow<Self::EventRepr>) -> &'a str;

    /// Get the timestamp of an event
    fn get_ev_time(&'a self, ev: impl Borrow<Self::EventRepr>) -> &'a DateTime<FixedOffset>;

    /// Get the names of all attributes that an event has
    fn get_ev_attrs(&'a self, ev: impl Borrow<Self::EventRepr>) -> impl Iterator<Item = &'a str>;
    /// Get the value assigned to an event attribute (by name) for an event
    fn get_ev_attr_val(
        &'a self,
        ev: impl Borrow<Self::EventRepr>,
        attr_name: impl AsRef<str>,
    ) -> Option<&'a OCELAttributeValue>;

    /// Get the names of all attributes that an object has
    fn get_ob_attrs(&'a self, ob: impl Borrow<Self::ObjectRepr>) -> impl Iterator<Item = &'a str>;

    /// Get the value assigned to an object attribute (by name) for an object
    fn get_ob_attr_vals(
        &'a self,
        ob: impl Borrow<Self::ObjectRepr>,
        attr_name: impl AsRef<str>,
    ) -> impl Iterator<Item = (&'a DateTime<FixedOffset>, &'a OCELAttributeValue)>;

    /// Get an event based on its ID
    fn get_ev_by_id(&'a self, ev_id: impl AsRef<str>) -> Option<Self::EventRepr>;

    /// Get an object based on its ID
    fn get_ob_by_id(&'a self, ob_id: impl AsRef<str>) -> Option<Self::ObjectRepr>;

    /// Get the full (materialized) event, depending on the backing implementation, either as a reference or owned value.
    /// __Avoid using this function. Depending on the implementation, it might have significant performance overhead.__
    /// Instead, use specialized functions to access specific fields or properties (e.g., [`Self::get_ev_time`]).
    fn get_full_ev(&'a self, index: impl Borrow<Self::EventRepr>) -> Cow<'a, OCELEvent>;

    /// Get the full (materialized) object, depending on the backing implementation, either as a reference or owned value.
    /// __Avoid using this function. Depending on the implementation, it might have significant performance overhead.__
    /// Instead, use specialized functions to access specific fields or properties (e.g., [`Self::get_ob_type_of`]).
    fn get_full_ob(&'a self, index: impl Borrow<Self::ObjectRepr>) -> Cow<'a, OCELObject>;

    /// Get event type specification ([`OCELType`]) from type name (i.e., activity)
    ///
    /// Note: If you want to get the type of an specific event, use [`Self::get_ev_type_of`] instead.
    fn get_ev_type(&'a self, ev_type: impl AsRef<str>) -> Option<&'a OCELType>;

    /// Get object type ([`OCELType`]) from type name
    ///
    /// Note: If you want to get the type of an specific object, use [`Self::get_ob_type_of`] instead.
    fn get_ob_type(&'a self, ob_type: impl AsRef<str>) -> Option<&'a OCELType>;

    /// Get all events of the given event type (activity)
    fn get_evs_of_type(&'a self, ev_type: &'_ str) -> impl Iterator<Item = &'a Self::EventRepr>;
    /// Get all object of the given object type
    fn get_obs_of_type(&'a self, ob_type: &'_ str) -> impl Iterator<Item = &'a Self::ObjectRepr>;

    /// Get all event types (activities)
    fn get_ev_types(&'a self) -> impl Iterator<Item = &'a str>;
    /// Get all object types
    fn get_ob_types(&'a self) -> impl Iterator<Item = &'a str>;

    /// Get the number of objects
    ///
    ///
    /// ## Implementation Note
    // Implementers might choose to override this function for efficient O(1) runtime.
    // However, for standard iterators constructed from [`Vec`], the count method already runs in constant time.
    fn get_num_obs(&'a self) -> usize {
        self.get_all_obs().count()
    }
    // Get the number of events
    //
    /// ## Implementation Note
    // Implementers might choose to override this function for efficient O(1) runtime.
    // However, for standard iterators constructed from [`Vec`], the count method already runs in constant time.
    fn get_num_evs(&'a self) -> usize {
        self.get_all_evs().count()
    }

    /// Construct [`OCEL`] from this linked version
    ///
    /// Note: This conversion might be expensive!
    fn construct_ocel(&'a self) -> OCEL {
        OCEL {
            event_types: self
                .get_ev_types()
                .flat_map(|et| self.get_ev_type(et))
                .cloned()
                .collect(),
            object_types: self
                .get_ob_types()
                .flat_map(|et| self.get_ob_type(et))
                .cloned()
                .collect(),
            events: self
                .get_all_evs()
                .map(|ev| self.get_full_ev(&ev).into_owned())
                .collect(),
            objects: self
                .get_all_obs()
                .map(|ev| self.get_full_ob(&ev).into_owned())
                .collect(),
        }
    }
}
