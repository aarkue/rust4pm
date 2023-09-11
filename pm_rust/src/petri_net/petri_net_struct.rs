use std::collections::HashMap;
use uuid::Uuid;
#[derive(Debug, Clone, PartialEq)]
struct Place {
    id: Uuid,
}

#[derive(Debug, Clone, PartialEq)]
struct Transition {
    label: Option<String>,
    id: Uuid,
}

#[derive(Debug)]
enum PetriNetNodes<'a> {
    None,
    Places(Vec<&'a Place>),
    Transitions(Vec<&'a Transition>),
}

#[derive(Debug)]
enum ArcType {
    PlaceTransition(Uuid, Uuid),
    TransitionPlace(Uuid, Uuid),
}

#[derive(Debug)]
struct Arc {
    from_to: ArcType,
    weight: u32,
}

#[derive(Debug)]
struct PetriNet {
    pub places: HashMap<Uuid, Place>,
    pub transitions: HashMap<Uuid, Transition>,
    pub arcs: Vec<Arc>,
}

impl PetriNet {
    fn new() -> Self {
        Self {
            places: HashMap::new(),
            transitions: HashMap::new(),
            arcs: Vec::new(),
        }
    }
    fn add_place(&mut self, place_id: Option<Uuid>) -> Place {
        let place_id = place_id.unwrap_or(Uuid::new_v4());
        let place = Place { id: place_id };
        self.places.insert(place_id, place.clone());
        return place;
    }

    fn add_transition(&mut self, label: Option<String>, transition_id: Option<Uuid>) -> Transition {
        let transition_id = transition_id.unwrap_or(Uuid::new_v4());
        let transition = Transition {
            id: transition_id,
            label,
        };
        self.transitions.insert(transition_id, transition.clone());
        return transition;
    }
    fn add_arc(&mut self, from_to: ArcType, weight: Option<u32>) {
        self.arcs.push(Arc {
            from_to: from_to,
            weight: weight.unwrap_or(1),
        });
    }

    fn preset_of(&self, id: Uuid) -> PetriNetNodes {
        if self.places.contains_key(&id) {
            let p = self.places.get(&id).unwrap();
            PetriNetNodes::Transitions(self.preset_of_place(p))
        } else if self.transitions.contains_key(&id) {
            let t = self.transitions.get(&id).unwrap();
            PetriNetNodes::Places(self.preset_of_transition(t))
        } else {
            PetriNetNodes::None
        }
    }

    fn preset_of_place(&self, p: &Place) -> Vec<&Transition> {
        self.arcs
            .iter()
            .filter_map(|x: &Arc| match x.from_to {
                ArcType::PlaceTransition(from, to) if from == p.id => self.transitions.get(&to),
                ArcType::TransitionPlace(from, to) if to == p.id => self.transitions.get(&from),
                _ => None,
            })
            .collect()
    }

    fn preset_of_transition(&self, t: &Transition) -> Vec<&Place> {
        self.arcs
            .iter()
            .filter_map(|x: &Arc| match x.from_to {
                ArcType::PlaceTransition(from, to) if from == t.id => self.places.get(&to),
                ArcType::TransitionPlace(from, to) if to == t.id => self.places.get(&from),
                _ => None,
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn petri_nets() {
        let mut net = PetriNet::new();
        let p1 = net.add_place(None);
        let t1 = net.add_transition(Some("Have fun".into()), None);
        let t2 = net.add_transition(Some("Sleep".into()), None);
        net.add_arc(ArcType::PlaceTransition(p1.id, t1.id), None);
        println!("Constructed petri net: {:?}", net);

        assert!(net.preset_of_transition(&t1).is_empty());
        assert!(net.preset_of_place(&p1) == vec![&t1]);
        assert!(net.preset_of_place(&p1) != vec![&t2]);
        assert!(net.preset_of_transition(&t2).is_empty());
    }
}
