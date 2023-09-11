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
enum PetriNetNodes {
    None,
    Places(Vec<PlaceID>),
    Transitions(Vec<TransitionID>),
}

#[derive(Debug)]
enum ArcType {
    PlaceTransition(Uuid, Uuid),
    TransitionPlace(Uuid, Uuid),
}

impl ArcType {
  fn place_to_transition(from: PlaceID, to: TransitionID) -> ArcType{
    return ArcType::PlaceTransition(from.0, to.0)
  }
  fn transition_to_place(from: TransitionID, to: PlaceID) -> ArcType{
    return ArcType::TransitionPlace(from.0, to.0)
  }
}

#[derive(Debug)]
struct Arc {
    from_to: ArcType,
    weight: u32,
}

#[derive(Debug, PartialEq, Clone, Copy)]
struct PlaceID(Uuid);
impl From<&Place> for PlaceID {
    fn from(value: &Place) -> Self {
        PlaceID(value.id)
    }
}

#[derive(Debug, PartialEq, Clone, Copy)]
struct TransitionID(Uuid);
impl From<&Transition> for TransitionID {
    fn from(value: &Transition) -> Self {
        TransitionID(value.id)
    }
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
    fn add_place(&mut self, place_id: Option<Uuid>) -> PlaceID {
        let place_id = place_id.unwrap_or(Uuid::new_v4());
        let place = Place { id: place_id };
        self.places.insert(place_id, place);
        return PlaceID(place_id)
    }

    fn add_transition(&mut self, label: Option<String>, transition_id: Option<Uuid>) -> TransitionID {
        let transition_id = transition_id.unwrap_or(Uuid::new_v4());
        let transition = Transition {
            id: transition_id,
            label,
        };
        self.transitions.insert(transition_id, transition);
        return TransitionID(transition_id)
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
            PetriNetNodes::Transitions(self.preset_of_place(p.into()))
        } else if self.transitions.contains_key(&id) {
            let t = self.transitions.get(&id).unwrap();
            PetriNetNodes::Places(self.preset_of_transition(t.into()))
        } else {
            PetriNetNodes::None
        }
    }

    fn preset_of_place(&self, p: PlaceID) -> Vec<TransitionID> {
        self.arcs
            .iter()
            .filter_map(|x: &Arc| match x.from_to {
                ArcType::TransitionPlace(from, to) if to == p.0 =>Some(TransitionID(from)),
                _ => None,
            })
            .collect()
    }

    fn preset_of_transition(&self, t: TransitionID) -> Vec<PlaceID> {
        self.arcs
            .iter()
            .filter_map(|x: &Arc| match x.from_to {
                ArcType::PlaceTransition(from, to) if to == t.0 => Some(PlaceID(from)),
                _ => None,
            })
            .collect()
    }
 
    fn postset_of(&self, id: Uuid) -> PetriNetNodes {
      if self.places.contains_key(&id) {
          let p = self.places.get(&id).unwrap();
          PetriNetNodes::Transitions(self.postset_of_place(p.into()))
      } else if self.transitions.contains_key(&id) {
          let t = self.transitions.get(&id).unwrap();
          PetriNetNodes::Places(self.postset_of_transition(t.into()))
      } else {
          PetriNetNodes::None
      }
  }

    fn postset_of_place(&self, p: PlaceID) -> Vec<TransitionID> {
        self.arcs
            .iter()
            .filter_map(|x: &Arc| match x.from_to {
                ArcType::PlaceTransition(from, to) if from == p.0 => Some(TransitionID(to)),
                _ => None,
            })
            .collect()
    }

    fn postset_of_transition(&self, t: TransitionID) -> Vec<PlaceID> {
        self.arcs
            .iter()
            .filter_map(|x: &Arc| match x.from_to {
                ArcType::TransitionPlace(from, to) if from == t.0 => Some(PlaceID(to)),
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
        net.add_arc(ArcType::place_to_transition(p1, t1), None);
        net.add_arc(ArcType::transition_to_place(t2, p1), None);
        println!("Constructed petri net: {:?}", net);

        assert!(net.postset_of_transition(t1).is_empty());
        assert!(net.preset_of_transition(t1) == vec![p1]);
        assert!(net.postset_of_place(p1) == vec![t1]);
        assert!(net.preset_of_place(p1) == vec![t2]);
        assert!(net.preset_of_transition(t2).is_empty());
    }
}
