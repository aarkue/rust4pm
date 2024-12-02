use std::collections::HashMap;
use std::collections::HashSet;
use std::hash::{Hash, Hasher};
use std::sync::Arc;
use uuid::Uuid;

/// Macro to implement `PartialEq`, `Eq`, and `Hash` based on `id` for structs.
#[macro_export]
macro_rules! id_based_impls {
    ($struct_name:ident) => {
        impl PartialEq for $struct_name {
            fn eq(&self, other: &Self) -> bool {
                self.id == other.id
            }
        }

        impl Eq for $struct_name {}

        impl Hash for $struct_name {
            fn hash<H: Hasher>(&self, state: &mut H) {
                self.id.hash(state);
            }
        }
    };
}

#[derive(Debug, Clone)]
pub struct InputArc {
    pub id: Uuid,
    pub source_place_id: Uuid, // Place ID
    pub target_transition_id: Uuid, // Transition ID
    pub variable: bool,
    pub weight: i32,
}

#[derive(Debug, Clone)]
pub struct OutputArc {
    pub id: Uuid,
    pub source_transition_id: Uuid, // Transition ID
    pub target_place_id: Uuid, // Place ID
    pub variable: bool,
    pub weight: i32,
}

impl InputArc {
    pub fn new(
        source_place_id: Uuid,
        target_transition_id: Uuid,
        variable: bool,
        weight: i32,
    ) -> Self {
        InputArc {
            id: Uuid::new_v4(),
            source_place_id,
            target_transition_id,
            variable,
            weight,
        }
    }
}

impl OutputArc {
    pub fn new(
        source_transition_id: Uuid,
        target_place_id: Uuid,
        variable: bool,
        weight: i32,
    ) -> Self {
        OutputArc {
            id: Uuid::new_v4(),
            source_transition_id,
            target_place_id,
            variable,
            weight,
        }
    }
}

id_based_impls!(InputArc);
id_based_impls!(OutputArc);

#[derive(Debug, Clone)]
pub struct Place {
    pub id: Uuid,
    pub name: Option<String>,
    pub object_type: String,
    pub initial: bool,
    pub final_place: bool,
    pub input_arcs: HashSet<Arc<OutputArc>>, // Incoming arcs (OutputArcs)
    pub output_arcs: HashSet<Arc<InputArc>>, // Outgoing arcs (InputArcs)
}

#[derive(Debug, Clone)]
pub struct Transition {
    pub id: Uuid,
    pub name: String,
    pub label: Option<String>,
    pub silent: bool,
    pub input_arcs: HashSet<Arc<InputArc>>, // Incoming arcs (InputArcs)
    pub output_arcs: HashSet<Arc<OutputArc>>, // Outgoing arcs (OutputArcs)
}

id_based_impls!(Place);
id_based_impls!(Transition);

#[derive(Debug, Clone)]
pub struct ObjectCentricPetriNet {
    // Stores entities by their IDs for quick access
    pub places: HashMap<Uuid, Place>,
    pub transitions: HashMap<Uuid, Transition>,
    // Stores arcs by their IDs for easy access and management
    pub input_arcs: HashMap<Uuid, Arc<InputArc>>,
    pub output_arcs: HashMap<Uuid, Arc<OutputArc>>,
}

impl ObjectCentricPetriNet {
    pub fn new() -> Self {
        ObjectCentricPetriNet {
            places: HashMap::new(),
            transitions: HashMap::new(),
            input_arcs: HashMap::new(),
            output_arcs: HashMap::new(),
        }
    }

    // Place Operations
    pub fn add_place(
        &mut self,
        name: Option<String>,
        object_type: String,
        initial: bool,
        final_state: bool,
    ) -> Place {
        let place = Place {
            id: Uuid::new_v4(),
            name,
            object_type,
            initial,
            final_place: final_state,
            input_arcs: HashSet::new(),
            output_arcs: HashSet::new(),
        };
        self.places.insert(place.id, place.clone());
        place
    }

    pub fn get_place(&self, id: &Uuid) -> Option<&Place> {
        self.places.get(id)
    }

    pub fn get_initial_places(&self) -> Vec<Place> {
        self.places
            .values()
            .filter(|place| place.initial)
            .cloned()
            .collect()
    }

    pub fn get_final_places(&self) -> Vec<Place> {
        self.places
            .values()
            .filter(|place| place.final_place)
            .cloned()
            .collect()
    }

    // Transition Operations
    pub fn add_transition(
        &mut self,
        name: String,
        label: Option<String>,
        silent: bool,
    ) -> Transition {
        let transition = Transition {
            id: Uuid::new_v4(),
            name,
            label,
            silent,
            input_arcs: HashSet::new(),
            output_arcs: HashSet::new(),
        };
        self.transitions.insert(transition.id, transition.clone());
        transition
    }

    pub fn get_transition(&self, id: &Uuid) -> Option<&Transition> {
        self.transitions.get(id)
    }

    // Arc Operations
    pub fn add_input_arc(
        &mut self,
        source_place_id: Uuid,
        target_transition_id: Uuid,
        variable: bool,
        weight: i32,
    ) -> Arc<InputArc> {
        // Validate existence
        let source_place = match self.places.get(&source_place_id) {
            Some(place) => place.clone(),
            None => panic!("Source place with ID {:?} does not exist.", source_place_id),
        };
        let target_transition = match self.transitions.get(&target_transition_id) {
            Some(transition) => transition.clone(),
            None => panic!(
                "Target transition with ID {:?} does not exist.",
                target_transition_id
            ),
        };
        let arc = Arc::new(InputArc::new(
            source_place_id,
            target_transition_id,
            variable,
            weight,
        ));
        self.input_arcs.insert(arc.id, Arc::clone(&arc));
        // Update relationships
        let place_arc = Arc::clone(&arc);
        self.places.get_mut(&source_place_id).unwrap().output_arcs.insert(place_arc.clone());
        self.transitions
            .get_mut(&target_transition_id)
            .unwrap()
            .input_arcs
            .insert(place_arc);
        arc
    }

    pub fn add_output_arc(
        &mut self,
        source_transition_id: Uuid,
        target_place_id: Uuid,
        variable: bool,
        weight: i32,
    ) -> Arc<OutputArc> {
        // Validate existence
        let source_transition = match self.transitions.get(&source_transition_id) {
            Some(transition) => transition.clone(),
            None => panic!(
                "Source transition with ID {:?} does not exist.",
                source_transition_id
            ),
        };
        let target_place = match self.places.get(&target_place_id) {
            Some(place) => place.clone(),
            None => panic!(
                "Target place with ID {:?} does not exist.",
                target_place_id
            ),
        };
        let arc = Arc::new(OutputArc::new(
            source_transition_id,
            target_place_id,
            variable,
            weight,
        ));
        self.output_arcs.insert(arc.id, Arc::clone(&arc));
        // Update relationships
        let transition_arc = Arc::clone(&arc);
        self.transitions
            .get_mut(&source_transition_id)
            .unwrap()
            .output_arcs
            .insert(transition_arc.clone());
        self.places
            .get_mut(&target_place_id)
            .unwrap()
            .input_arcs
            .insert(transition_arc);
        arc
    }

    pub fn get_input_arc(&self, id: &Uuid) -> Option<Arc<InputArc>> {
        self.input_arcs.get(id).cloned()
    }

    pub fn get_output_arc(&self, id: &Uuid) -> Option<Arc<OutputArc>> {
        self.output_arcs.get(id).cloned()
    }

    // Pre and Post Set Methods for Places
    pub fn get_pre_set_of_place(&self, place_id: &Uuid) -> Vec<Transition> {
        self.places.get(place_id).map_or(Vec::new(), |place| {
            place
                .input_arcs
                .iter()
                .filter_map(|arc| self.transitions.get(&arc.source_transition_id).cloned())
                .collect()
        })
    }

    pub fn get_post_set_of_place(&self, place_id: &Uuid) -> Vec<Transition> {
        self.places.get(place_id).map_or(Vec::new(), |place| {
            place
                .output_arcs
                .iter()
                .filter_map(|arc| self.transitions.get(&arc.target_transition_id).cloned())
                .collect()
        })
    }

    pub fn get_pre_set_of_transition(&self, transition_id: &Uuid) -> Vec<Place> {
        self.transitions.get(transition_id).map_or(Vec::new(), |transition| {
            transition
                .input_arcs
                .iter()
                .filter_map(|arc| self.places.get(&arc.source_place_id).cloned())
                .collect()
        })
    }

    pub fn get_post_set_of_transition(&self, transition_id: &Uuid) -> Vec<Place> {
        self.transitions.get(transition_id).map_or(Vec::new(), |transition| {
            transition
                .output_arcs
                .iter()
                .filter_map(|arc| self.places.get(&arc.target_place_id).cloned())
                .collect()
        })
    }

    // Additional Helper Methods (Optional)
    /// Retrieves all input arcs for a given place
    pub fn get_input_arcs_for_place(&self, place_id: &Uuid) -> HashSet<Arc<OutputArc>> {
        self.places
            .get(place_id)
            .expect("Place not found")
            .input_arcs
            .clone()
    }

    /// Retrieves all output arcs for a given place
    pub fn get_output_arcs_for_place(&self, place_id: &Uuid) -> HashSet<Arc<InputArc>> {
        self.places
            .get(place_id)
            .expect("Place not found")
            .output_arcs
            .clone()
    }

    /// Retrieves all input arcs for a given transition
    pub fn get_input_arcs_for_transition(&self, transition_id: &Uuid) -> HashSet<Arc<InputArc>> {
        self.transitions
            .get(transition_id)
            .expect("Transition not found")
            .input_arcs
            .clone()
    }

    /// Retrieves all output arcs for a given transition
    pub fn get_output_arcs_for_transition(&self, transition_id: &Uuid) -> HashSet<Arc<OutputArc>> {
        self.transitions
            .get(transition_id)
            .expect("Transition not found")
            .output_arcs
            .clone()
    }
}

// Implement functionality for Place and Transition
impl Place {
    pub fn new(
        name: Option<String>,
        object_type: String,
        initial: bool,
        final_state: bool,
    ) -> Self {
        Place {
            id: Uuid::new_v4(),
            name,
            object_type,
            initial,
            final_place: final_state,
            input_arcs: HashSet::new(),
            output_arcs: HashSet::new(),
        }
    }
}

impl Transition {
    pub fn new(name: String, label: Option<String>, silent: bool) -> Self {
        Transition {
            id: Uuid::new_v4(),
            name,
            label,
            silent,
            input_arcs: HashSet::new(),
            output_arcs: HashSet::new(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_petri_net() {
        let mut net = ObjectCentricPetriNet::new();

        // Add places
        let p1 = net.add_place(Some("P1".to_string()), "Token".to_string(), true, false);
        let p2 = net.add_place(Some("P2".to_string()), "Token".to_string(), false, true);

        // Add transitions
        let t1 = net.add_transition("T1".to_string(), Some("Transition 1".to_string()), false);
        let t2 = net.add_transition("T2".to_string(), None, true);

        // Add arcs
        let arc1 = net.add_input_arc(p1.id, t1.id, false, 1);
        let arc2 = net.add_output_arc(t1.id, p2.id, false, 1);
        let arc3 = net.add_input_arc(p2.id, t2.id, true, 2);
        let arc4 = net.add_output_arc(t2.id, p1.id, true, 2);

        // Retrieve pre and post sets for Transition T1
        let pre_t1 = net.get_pre_set_of_transition(&t1.id);
        let post_t1 = net.get_post_set_of_transition(&t1.id);
        assert_eq!(pre_t1.len(), 1);
        assert_eq!(pre_t1[0].id, p1.id);
        assert_eq!(post_t1.len(), 1);
        assert_eq!(post_t1[0].id, p2.id);

        // Retrieve pre and post sets for Place P1
        let pre_p1 = net.get_pre_set_of_place(&p1.id);
        let post_p1 = net.get_post_set_of_place(&p1.id);
        assert_eq!(pre_p1.len(), 1);
        assert_eq!(pre_p1[0].id, t2.id);
        assert_eq!(post_p1.len(), 1);
        assert_eq!(post_p1[0].id, t1.id);

        // Additional Assertions
        // Verify arc details
        let retrieved_arc1 = net.get_input_arc(&arc1.id).unwrap();
        assert_eq!(retrieved_arc1.source_place_id, p1.id);
        assert_eq!(retrieved_arc1.target_transition_id, t1.id);
        assert!(!retrieved_arc1.variable);
        assert_eq!(retrieved_arc1.weight, 1);

        let retrieved_arc4 = net.get_output_arc(&arc4.id).unwrap();
        assert_eq!(retrieved_arc4.source_transition_id, t2.id);
        assert_eq!(retrieved_arc4.target_place_id, p1.id);
        assert!(retrieved_arc4.variable);
        assert_eq!(retrieved_arc4.weight, 2);
    }

    #[test]
    #[should_panic(expected = "Source place with ID")]
    fn test_add_input_arc_invalid_place() {
        let mut net = ObjectCentricPetriNet::new();
        let p_id = Uuid::new_v4(); // Non-existent place
        let t_id = net.add_transition("T1".to_string(), None, false).id;
        net.add_input_arc(p_id, t_id, false, 1);
    }

    #[test]
    #[should_panic(expected = "Target transition with ID")]
    fn test_add_input_arc_invalid_transition() {
        let mut net = ObjectCentricPetriNet::new();
        let t_id = Uuid::new_v4(); // Non-existent transition
        let p_id = net.add_place(None, "Token".to_string(), false, false).id;
        net.add_input_arc(p_id, t_id, false, 1);
    }

    #[test]
    #[should_panic(expected = "Source transition with ID")]
    fn test_add_output_arc_invalid_transition() {
        let mut net = ObjectCentricPetriNet::new();
        let t_id = Uuid::new_v4(); // Non-existent transition
        let p_id = net.add_place(None, "Token".to_string(), false, false).id;
        net.add_output_arc(t_id, p_id, false, 1);
    }

    #[test]
    #[should_panic(expected = "Target place with ID")]
    fn test_add_output_arc_invalid_place() {
        let mut net = ObjectCentricPetriNet::new();
        let t_id = net.add_transition("T1".to_string(), None, false).id;
        let p_id = Uuid::new_v4(); // Non-existent place
        net.add_output_arc(t_id, p_id, false, 1);
    }
}