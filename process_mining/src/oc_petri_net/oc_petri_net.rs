use std::collections::{HashMap, HashSet};
use std::collections::hash_set::Iter;
use std::hash::{Hash, Hasher};
use std::iter::Filter;
use std::sync::{Arc, Mutex};
use uuid::Uuid;

#[macro_export] macro_rules! id_based_impls {
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

#[derive(Debug)]
pub struct Place {
    id: Uuid,
    name: Option<String>,
    object_type: String,
    pub initial: bool,
    pub final_place: bool,
    pub in_arcs: Mutex<HashSet<Arc<OutputArc>>>,
    pub out_arcs: Mutex<HashSet<Arc<InputArc>>>,
}

#[derive(Debug)]
pub struct Transition {
    id: Uuid,
    name: String,
    label: Option<String>,
    pub in_arcs: Mutex<HashSet<Arc<InputArc>>>,
    pub out_arcs: Mutex<HashSet<Arc<OutputArc>>>,
    silent: bool,
}

#[derive(Debug, Clone, Eq, Hash, PartialEq)]
enum PNArc {
    InputArc(Arc<InputArc>),
    OutputArc(Arc<OutputArc>),
}

#[derive(Debug, Clone)]
pub struct InputArc {
    id: Uuid,
    source: Arc<Place>,
    target: Arc<Transition>,
    variable: bool,
    weight: i32,
}

#[derive(Debug, Clone)]
pub struct OutputArc {
    id: Uuid,
    source: Arc<Transition>,
    target: Arc<Place>,
    variable: bool,
    weight: i32,
}

// Node enum to handle both Place and Transition
#[derive(Debug, Clone)]
enum Node {
    Place(Arc<Place>),
    Transition(Arc<Transition>),
}

#[derive(Clone, Debug)]
pub struct ObjectCentricPetriNet {
    places: HashSet<Arc<Place>>,
    transitions: HashSet<Arc<Transition>>,
    arcs: HashSet<Arc<PNArc>>,
    place_mapping: HashMap<Uuid, Arc<Place>>,
    transition_mapping: HashMap<Uuid, Arc<Transition>>,
    arc_mapping: HashMap<Uuid, Arc<PNArc>>,
}


// Implement functionality for these structs
impl Place {
    pub fn new(name: Option<String>, object_type: String, initial: bool, final_state: bool) -> Self {
        Place {
            id: Uuid::new_v4(),
            name,
            object_type,
            initial,
            final_place: final_state,
            in_arcs: HashSet::new().into(),
            out_arcs: HashSet::new().into(),
        }
    }
}

impl Transition {
    pub fn new(name: String, label: Option<String>, silent: bool) -> Self {
        Transition {
            id: Uuid::new_v4(),
            name,
            label,
            in_arcs: HashSet::new().into(),
            out_arcs: HashSet::new().into(),
            silent,
        }
    }
}

impl InputArc {
    pub fn new(source: Arc<Place>, target: Arc<Transition>, variable: bool, weight: i32) -> Self {
        InputArc {
            id: Uuid::new_v4(),
            source,
            target,
            variable,
            weight,
        }
    }
}
impl OutputArc {
    pub fn new(source: Arc<Transition>, target: Arc<Place>, variable: bool, weight: i32) -> Self {
        OutputArc {
            id: Uuid::new_v4(),
            source,
            target,
            variable,
            weight,
        }
    }
}

impl ObjectCentricPetriNet {
    pub fn new() -> Self {
        ObjectCentricPetriNet {
            places: HashSet::new(),
            transitions: HashSet::new(),
            arcs: HashSet::new(),
            place_mapping: HashMap::new(),
            transition_mapping: HashMap::new(),
            arc_mapping: HashMap::new(),
        }
    }
    
    pub fn get_place(&self, id: Uuid) -> Option<Arc<Place>> {
        self.place_mapping.get(&id).cloned()
    }
    
    pub fn get_transition(&self, id: Uuid) -> Option<Arc<Transition>> {
        self.transition_mapping.get(&id).cloned()
    }
    
    pub fn get_initial_places(&mut self) -> Vec<Arc<Place>>{
        self.places.iter().filter(|place| place.initial).cloned().collect()
    }
    
    pub fn get_final_places(&mut self) -> Vec<Arc<Place>>{
        self.places.iter().filter(|place| place.final_place).cloned().collect()
    }
    
    
    // Additional methods to manipulate the petri net

    pub fn add_place(&mut self, place: Arc<Place>) {
        self.places.insert(place.clone());
        self.place_mapping.insert(place.id, place);
    }

    pub fn add_transition(&mut self, transition: Arc<Transition>) {
        self.transitions.insert(transition.clone());
        self.transition_mapping.insert(transition.id, transition);
    }

    pub fn add_arc_between(&mut self, from: Arc<Node>, to: Arc<Node>, is_variable: bool, weight: Option<i32>) {
        let weight = weight.unwrap_or(0);
        let arc = match (from.as_ref(), to.as_ref()) {
            (Node::Place(from), Node::Transition(to)) => {
                let arc = InputArc::new(from.clone(), to.clone(), is_variable, weight);
                
                let arc = Arc::new(arc);

                from.out_arcs.lock().unwrap().insert(arc.clone());
                to.in_arcs.lock().unwrap().insert(arc.clone());
                
                PNArc::InputArc(arc.clone())
            }
            (Node::Transition(from), Node::Place(to)) => {
                let arc = OutputArc::new(from.clone(), to.clone(), is_variable, weight);
                
                let arc = Arc::new(arc);
                
                from.out_arcs.lock().unwrap().insert(arc.clone());
                to.in_arcs.lock().unwrap().insert(arc.clone());
                
                PNArc::OutputArc(arc)
            }
            _ => {
                panic!("Invalid place assignment");
            }
        };
        let arc = Arc::from(arc);
        
        self.arcs.insert(arc.clone());
    }
}


id_based_impls!(Place);
id_based_impls!(Transition);
id_based_impls!(InputArc);
id_based_impls!(OutputArc);