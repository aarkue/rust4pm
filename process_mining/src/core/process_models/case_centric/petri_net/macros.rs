//! Convenient Macros for creating Petri nets
///
/// Creates a [`PetriNet`].
///
/// # Examples
///
/// ```rust
/// use process_mining::{petri_net, PetriNet};
/// let petri_net: PetriNet = petri_net!(("a", "b", "c"; "c", "d"), ("f"; "e", "g"));
/// ```

#[macro_export]
macro_rules! petri_net {
    ( $( ($($x:expr),* ; $($y:expr),* ) ),* ) => {{
       #[allow(unused_imports)]
        use std::collections::HashMap;
       #[allow(unused_imports)]
        use $crate::core::process_models::case_centric::petri_net::{
            ArcType,
            PetriNet,
            PlaceID,
            TransitionID,
        };

        let mut result = PetriNet::new();
        let mut transition_id_dict: HashMap<String, TransitionID> = HashMap::new();
        let mut place_id_dict: HashMap<String, &PlaceID> = HashMap::new();

        let mut counter: u64 = 0;

        $(
            counter = counter + 1;
            let place_name: String = format!("p_{}", counter);

            let place_id = result.add_place(None);
            place_id_dict.insert(place_name, &place_id);

            $(
                let t_label = $x.to_string();
                if !transition_id_dict.contains_key(&t_label) {
                    transition_id_dict.insert(t_label.clone(), result.add_transition(Some(t_label.clone()), None));
                }

                let t_in = transition_id_dict.get(&t_label).unwrap();
                result.add_arc(ArcType::TransitionPlace(t_in.get_uuid(), place_id.get_uuid()), None);
            )*
            $(
                let t_label = $y.to_string();
                if !transition_id_dict.contains_key(&t_label) {
                    transition_id_dict.insert(t_label.clone(), result.add_transition(Some(t_label.clone()), None));
                }

                let t_out = transition_id_dict.get(&t_label).unwrap();
                result.add_arc(ArcType::PlaceTransition(place_id.get_uuid(), t_out.get_uuid()), None);
            )*
        )*

        result
    }}
}
