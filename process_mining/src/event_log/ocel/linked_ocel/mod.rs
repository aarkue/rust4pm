use super::ocel_struct::{OCELEvent, OCELObject};
mod index_linked_ocel;
mod reference_linked_ocel;

pub trait LinkedOCELAccess<'a, EvRefType: 'a, ObRefType: 'a, EvRetType: 'a, ObRetType: 'a>
where
    EvRefType: From<&'a EvRetType>,
    ObRefType: From<&'a ObRetType>,
{
    fn get_evs_of_type(&'a self, ev_type: &'_ str) -> impl Iterator<Item = &'a EvRetType>;
    fn get_obs_of_type(&'a self, ob_type: &'_ str) -> impl Iterator<Item = &'a ObRetType>;

    fn get_ev(&'a self, index: &EvRefType) -> &'a OCELEvent;
    fn get_ob(&'a self, index: &ObRefType) -> &'a OCELObject;

    fn get_e2o(&'a self, index: &EvRefType) -> impl Iterator<Item = (&'a str, &'a ObRetType)>;

    fn get_e2o_rev(&'a self, index: &ObRefType) -> impl Iterator<Item = (&'a str, &'a EvRetType)>;

    fn get_o2o(&'a self, index: &ObRefType) -> impl Iterator<Item = (&'a str, &'a ObRetType)>;

    fn get_o2o_rev(&'a self, index: &ObRefType) -> impl Iterator<Item = (&'a str, &'a ObRetType)>;
}
