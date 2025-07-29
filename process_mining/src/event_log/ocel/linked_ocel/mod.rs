use super::ocel_struct::{OCELEvent, OCELObject, OCELType};

#[allow(unused_imports)]
use super::ocel_struct::OCEL;

/// An [`OCEL`] linked based on event and object indices
///
/// The resulting [`IndexLinkedOCEL`] conveniently represents objects or events by their index in the [`OCEL`].
pub mod index_linked_ocel;
pub use index_linked_ocel::IndexLinkedOCEL;
/// An [`OCEL`] linked through object/event identifiers (i.e., [`String`]s)
pub mod id_linked_ocel;
pub use id_linked_ocel::{IDLinkedOCEL, OwnedIDLinkedOCEL};

/// Linked access to an [`OCEL`], making it easy to follow event-to-object and object-to-object relationships, as well as their reverse
///
/// See also [`IndexLinkedOCEL`] and [`IDLinkedOCEL`].
pub trait LinkedOCELAccess<'a> {
    type EvRetType: 'a;
    type ObRetType: 'a;
    type EvRefType: 'a + From<&'a Self::EvRetType>;
    type ObRefType: 'a + From<&'a Self::ObRetType>;

    // <'a, EvRefType: 'a, ObRefType: 'a, EvRetType: 'a, ObRetType: 'a>}

    /// Get all events of the given event type (activity)
    fn get_evs_of_type(&'a self, ev_type: &'_ str) -> impl Iterator<Item = &'a Self::EvRetType>;
    /// Get all object of the given object type
    fn get_obs_of_type(&'a self, ob_type: &'_ str) -> impl Iterator<Item = &'a Self::ObRetType>;

    /// Get all event types (activities)
    fn get_ev_types(&'a self) -> impl Iterator<Item = &'a str>;
    /// Get all object types
    fn get_ob_types(&'a self) -> impl Iterator<Item = &'a str>;

    /// Get all events
    ///
    /// Also see [`LinkedOCELAccess::get_all_evs_ref`].
    fn get_all_evs(&'a self) -> impl Iterator<Item = &'a OCELEvent>;
    /// Get all objects
    ///
    /// Also see [`LinkedOCELAccess::get_all_obs_ref`].
    fn get_all_obs(&'a self) -> impl Iterator<Item = &'a OCELObject>;

    /// Get all event references
    ///
    /// In contrast to [`LinkedOCELAccess::get_all_evs`], this does not necessarily return direct event references (i.e., &[`OCELEvent`]), but the linked-access specific representation of events
    fn get_all_evs_ref(&'a self) -> impl Iterator<Item = &'a Self::EvRefType>;

    /// Get all object references
    ///
    /// In contrast to [`LinkedOCELAccess::get_all_obs`], this does not necessarily return direct object references (i.e., &[`OCELObject`]), but the linked-access specific representation of objects
    fn get_all_obs_ref(&'a self) -> impl Iterator<Item = &'a Self::ObRefType>;

    /// Get an event reference based on the linked-access specific representation of an event
    fn get_ev(&'a self, index: &Self::EvRefType) -> &'a OCELEvent;

    /// Get an object reference based on the linked-access specific representation of an object
    fn get_ob(&'a self, index: &Self::ObRefType) -> &'a OCELObject;

    /// Get all objects related to the given event (through E2O (event-to-object) relations)
    fn get_e2o(
        &'a self,
        index: &Self::EvRefType,
    ) -> impl Iterator<Item = (&'a str, &'a Self::ObRetType)>;

    /// Get all events to which the given object is related (through the reverse E2O (event-to-object) relations)
    fn get_e2o_rev(
        &'a self,
        index: &Self::ObRefType,
    ) -> impl Iterator<Item = (&'a str, &'a Self::EvRetType)>;

    /// Get all objects related to the given object (through O2O (object-to-object) relations)
    fn get_o2o(
        &'a self,
        index: &Self::ObRefType,
    ) -> impl Iterator<Item = (&'a str, &'a Self::ObRetType)>;

    /// Get all objects inversely related to the given object (through **reverse** O2O (object-to-object) relations)
    fn get_o2o_rev(
        &'a self,
        index: &Self::ObRefType,
    ) -> impl Iterator<Item = (&'a str, &'a Self::ObRetType)>;

    /// Get event type ([`OCELType`]) from type name (i.e., activity)
    fn get_ev_type(&'a self, ev_type: impl AsRef<str>) -> Option<&'a OCELType>;

    /// Get object type ([`OCELType`]) from type name
    fn get_ob_type(&'a self, ob_type: impl AsRef<str>) -> Option<&'a OCELType>;
}
