//! Structural reduction of Petri nets

use uuid::Uuid;

use super::petri_net_struct::{ArcType, Marking, PetriNet, PlaceID, TransitionID};

/// A silent transition to fuse into the only transition feeding it.
struct Fusion {
    silent: Uuid,
    /// The only input place of `silent`, removed along with it.
    place: Uuid,
    /// The only transition producing `place`, which takes over the outputs of `silent`.
    producer: Uuid,
    outputs: Vec<Uuid>,
}

impl PetriNet {
    /// Removes silent transitions that only pass a token on, and returns how many were removed.
    ///
    /// A silent transition qualifies if its only input place is unmarked, is produced by exactly
    /// one other transition and is consumed by nothing else. It can then always fire right after
    /// that transition without being observed, so the producer may produce its outputs directly.
    ///
    /// The language is unchanged. Nets with weighted arcs are left as they are.
    pub fn reduce_silent_transitions(&mut self) -> usize {
        if self.arcs.iter().any(|arc| arc.weight != 1) {
            return 0;
        }
        let mut removed = 0;
        while let Some(fusion) = self.next_fusion() {
            self.arcs.retain(|arc| {
                let (place, transition) = match arc.from_to {
                    ArcType::PlaceTransition(place, transition)
                    | ArcType::TransitionPlace(transition, place) => (place, transition),
                };
                place != fusion.place && transition != fusion.silent
            });
            for output in fusion.outputs {
                self.add_arc(ArcType::TransitionPlace(fusion.producer, output), None);
            }
            self.transitions.remove(&fusion.silent);
            self.places.remove(&fusion.place);
            removed += 1;
        }
        removed
    }

    fn next_fusion(&self) -> Option<Fusion> {
        let marked = |place: PlaceID| {
            let holds = |marking: &Marking| marking.contains_key(&place);
            self.initial_marking.iter().any(holds)
                || self.final_markings.iter().flatten().any(holds)
        };

        self.transitions
            .iter()
            .filter(|(_, transition)| transition.label.is_none())
            .find_map(|(&silent, _)| {
                let [place] = self.preset_of_transition(TransitionID(silent))[..] else {
                    return None;
                };
                let [producer] = self.preset_of_place(place)[..] else {
                    return None;
                };
                if producer.0 == silent || self.postset_of_place(place).len() != 1 || marked(place)
                {
                    return None;
                }
                let outputs: Vec<Uuid> = self
                    .postset_of_transition(TransitionID(silent))
                    .into_iter()
                    .map(|output| output.0)
                    .collect();
                // Producing a place twice would need an arc of weight two.
                if self
                    .postset_of_transition(producer)
                    .iter()
                    .any(|output| outputs.contains(&output.0))
                {
                    return None;
                }
                Some(Fusion {
                    silent,
                    place: place.0,
                    producer: producer.0,
                    outputs,
                })
            })
    }
}

#[cfg(test)]
mod test_reduction {
    use super::*;

    /// `p0 -a-> p1 -tau-> p2 -b-> p3`: the silent transition can only pass the token on, so `a` may
    /// produce `p2` directly.
    #[test]
    fn test_pass_through_is_fused() {
        let mut net = PetriNet::new();
        let places: Vec<PlaceID> = (0..4).map(|_| net.add_place(None)).collect();
        let a = net.add_transition(Some("a".to_string()), None);
        let tau = net.add_transition(None, None);
        let b = net.add_transition(Some("b".to_string()), None);
        net.add_arc(ArcType::place_to_transition(places[0], a), None);
        net.add_arc(ArcType::transition_to_place(a, places[1]), None);
        net.add_arc(ArcType::place_to_transition(places[1], tau), None);
        net.add_arc(ArcType::transition_to_place(tau, places[2]), None);
        net.add_arc(ArcType::place_to_transition(places[2], b), None);
        net.add_arc(ArcType::transition_to_place(b, places[3]), None);
        net.initial_marking = Some(Marking::from([(places[0], 1)]));
        net.final_markings = Some(vec![Marking::from([(places[3], 1)])]);

        assert_eq!(net.reduce_silent_transitions(), 1);
        assert_eq!(net.transitions.len(), 2);
        assert_eq!(net.places.len(), 3);
        assert_eq!(net.arcs.len(), 4);
        assert_eq!(net.reduce_silent_transitions(), 0);
    }

    /// The same silent transition, but another transition also reads its input place, so firing it
    /// is a choice and fusing it away would lose that.
    #[test]
    fn test_choice_is_kept() {
        let mut net = PetriNet::new();
        let places: Vec<PlaceID> = (0..4).map(|_| net.add_place(None)).collect();
        let a = net.add_transition(Some("a".to_string()), None);
        let tau = net.add_transition(None, None);
        let b = net.add_transition(Some("b".to_string()), None);
        net.add_arc(ArcType::place_to_transition(places[0], a), None);
        net.add_arc(ArcType::transition_to_place(a, places[1]), None);
        net.add_arc(ArcType::place_to_transition(places[1], tau), None);
        net.add_arc(ArcType::transition_to_place(tau, places[2]), None);
        net.add_arc(ArcType::place_to_transition(places[1], b), None);
        net.add_arc(ArcType::transition_to_place(b, places[3]), None);

        assert_eq!(net.reduce_silent_transitions(), 0);
    }

    /// A place the net starts or ends in is a state of its own and cannot be fused away.
    #[test]
    fn test_marked_place_is_kept() {
        let mut net = PetriNet::new();
        let places: Vec<PlaceID> = (0..3).map(|_| net.add_place(None)).collect();
        let a = net.add_transition(Some("a".to_string()), None);
        let tau = net.add_transition(None, None);
        net.add_arc(ArcType::place_to_transition(places[0], a), None);
        net.add_arc(ArcType::transition_to_place(a, places[1]), None);
        net.add_arc(ArcType::place_to_transition(places[1], tau), None);
        net.add_arc(ArcType::transition_to_place(tau, places[2]), None);
        net.final_markings = Some(vec![Marking::from([(places[1], 1)])]);

        assert_eq!(net.reduce_silent_transitions(), 0);
    }
}
