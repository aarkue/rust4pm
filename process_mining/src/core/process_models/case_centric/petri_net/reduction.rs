//! Structural reduction of Petri nets

use super::net::{AcceptingPetriNet, ArcType, NodeID, PlaceID, TransitionID};

/// A silent transition to fuse into the only transition feeding it.
struct Fusion {
    silent: TransitionID,
    /// The only input place of `silent`, removed along with it.
    place: PlaceID,
    /// The only transition producing `place`, which takes over the outputs of `silent`.
    producer: TransitionID,
    outputs: Vec<PlaceID>,
}

impl AcceptingPetriNet {
    /// Removes silent transitions that only pass a token on, and returns how many were removed.
    ///
    /// A silent transition qualifies if its only input place is unmarked, is produced by exactly
    /// one other transition and is consumed by nothing else. It can then always fire right after
    /// that transition without being observed, so the producer may produce its outputs directly.
    ///
    /// The language is unchanged. Nets with weighted arcs are left as they are. Removing nodes
    /// renumbers the remaining ones, as with [`AcceptingPetriNet::retain_nodes`].
    pub fn reduce_silent_transitions(&mut self) -> usize {
        if self.net.arcs().any(|arc| arc.weight != 1) {
            return 0;
        }
        let mut removed = 0;
        while let Some(fusion) = self.next_fusion() {
            for output in fusion.outputs {
                self.net
                    .add_arc(ArcType::TransitionPlace(fusion.producer, output), None);
            }
            self.retain_nodes(|node| {
                node != NodeID::Place(fusion.place) && node != NodeID::Transition(fusion.silent)
            });
            removed += 1;
        }
        removed
    }

    fn next_fusion(&self) -> Option<Fusion> {
        let net = &self.net;
        net.transitions()
            .filter(|(_, transition)| transition.label.is_none())
            .find_map(|(silent, _)| {
                let [place] = net.preset_of_transition(silent).collect::<Vec<_>>()[..] else {
                    return None;
                };
                let [producer] = net.preset_of_place(place).collect::<Vec<_>>()[..] else {
                    return None;
                };
                if producer == silent
                    || net.postset_of_place(place).len() != 1
                    || self.is_in_initial_marking(place)
                    || self.is_in_final_marking(place)
                {
                    return None;
                }
                let outputs: Vec<PlaceID> = net.postset_of_transition(silent).collect();
                // Producing a place twice would need an arc of weight two.
                if net
                    .postset_of_transition(producer)
                    .any(|output| outputs.contains(&output))
                {
                    return None;
                }
                Some(Fusion {
                    silent,
                    place,
                    producer,
                    outputs,
                })
            })
    }
}

#[cfg(test)]
mod test_reduction {
    use super::*;
    use crate::core::process_models::case_centric::petri_net::{Marking, PetriNet};

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
        let mut net = AcceptingPetriNet::new(
            net,
            Marking::from([(places[0], 1)]),
            Marking::from([(places[3], 1)]),
        );

        assert_eq!(net.reduce_silent_transitions(), 1);
        assert_eq!(net.net.num_transitions(), 2);
        assert_eq!(net.net.num_places(), 3);
        assert_eq!(net.net.arcs().count(), 4);
        assert_eq!(net.initial_marking, Marking::from([(PlaceID(0), 1)]));
        assert_eq!(net.final_marking, Marking::from([(PlaceID(2), 1)]));
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
        let mut net = AcceptingPetriNet::new(net, Marking::new(), Marking::new());

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
        let mut net = AcceptingPetriNet::new(net, Marking::new(), Marking::from([(places[1], 1)]));

        assert_eq!(net.reduce_silent_transitions(), 0);
    }
}
