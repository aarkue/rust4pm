use crate::oc_petri_net::oc_petri_net::{InputArc, ObjectCentricPetriNet, Transition};
use crate::oc_petri_net::util::intersect_hashbag::intersect_hashbags;
use hashbag::HashBag;
use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use uuid::Uuid;
static COUNTER: AtomicUsize = AtomicUsize::new(0);

#[derive(Debug, Copy, Clone, Eq, PartialEq, Hash)]
pub struct OCToken {
    id: usize,
    //obj_id: str,
}

impl OCToken {
    pub fn new() -> Self {
        Self {
            id: COUNTER.fetch_add(1, Ordering::Relaxed),
        }
    }
}

#[derive(Debug, Clone)]
pub struct Marking {
    petri_net: Arc<ObjectCentricPetriNet>,
    assignments: HashMap<Uuid, HashBag<OCToken>>,
}

impl Marking {
    pub fn new(petri_net: ObjectCentricPetriNet) -> Self {
        Marking {
            petri_net: petri_net.into(),
            assignments: HashMap::new(),
        }
    }

    pub fn add_initial_token_count(&mut self, place_id: &Uuid, count: u64) {
        if !self
            .petri_net
            .get_place(place_id)
            .expect("Place not found")
            .initial
        {
            panic!("Place {} is not an initial place", place_id);
        }

        // create a new token count times and add it to a new hashbag
        let bag = self
            .assignments
            .entry(place_id.clone())
            .or_insert_with(|| HashBag::new());

        for _ in 0..count {
            let token = OCToken::new();
            bag.insert(token);
        }
    }

    pub fn add_initial_tokens(&mut self, place_id: &Uuid, tokens: &HashBag<OCToken>) {
        if !self
            .petri_net
            .get_place(place_id)
            .expect("Place not found")
            .initial
        {
            panic!("Place {} is not an initial place", place_id);
        }

        self._add_all_tokens_unsafe(place_id, tokens);
    }

    /// Adds a token to a place, regardless of the permissibility of the operation.
    /// It is strictly recommended to use add_initial_tokens instead
    pub fn _add_all_tokens_unsafe(&mut self, place_id: &Uuid, tokens: &HashBag<OCToken>) {
        let bag = self
            .assignments
            .entry(place_id.clone())
            .or_insert_with(|| HashBag::new());

        tokens.set_iter().for_each(|(token, count)| {
            bag.insert_many(token.clone(), count);
        });
    }

    pub fn add_token_unsafe(&mut self, place_id: Uuid, token: OCToken) {
        self.assignments
            .entry(place_id)
            .or_insert_with(|| HashBag::new())
            .insert(token);
    }

    /// Checks if a transition is enabled in the current marking
    /// A transition is enabled, if all input places have at least one tokens,
    /// and for any set of input places of type t, there is at least one token
    /// of id x in the marking
    pub fn is_enabled(&self, transition: &Transition) -> bool {
        let arcs_to_place: HashMap<Uuid, Vec<&InputArc>> =
            transition
                .input_arcs
                .iter()
                .fold(HashMap::new(), |mut acc, arc| {
                    acc.entry(arc.source_place_id)
                        .or_insert_with(Vec::new)
                        .push(arc);
                    acc
                });
        // Step 2: Process each group to build input_place_map
        let default: HashBag<OCToken> = HashBag::new();
        let input_place_map: HashMap<String, Vec<HashBag<OCToken>>> =
            arcs_to_place
                .iter()
                .fold(HashMap::new(), |mut acc, (place_id, arcs)| {
                    // Retrieve the place associated with the current place_id
                    let place = self.petri_net.get_place(place_id).expect("Place not found");

                    let obj_type = place.object_type.clone();

                    // Retrieve the assignment bag; default to an empty HashBag if not found
                    // fixme we can already early exit on default here because the transition
                    // is not firable. find a clean way for this
                    let bag = self.assignments.get(place_id).unwrap_or(&default);

                    // Determine how many arcs are consuming this place
                    let consuming_arc_count = arcs.len();

                    // Filter the bag to retain only tokens with a count >= consuming_arc_count
                    let mut filtered_bag = bag.clone();
                    filtered_bag.retain(|_, count| {
                        if count >= consuming_arc_count {
                            return count;
                        }
                        return 0;
                    });

                    // Insert the filtered bag into the input_place_map grouped by object_type
                    acc.entry(obj_type)
                        .or_insert_with(Vec::new)
                        .push(filtered_bag);
                    acc
                });

        for (obj_type, bags) in input_place_map {
            // for each object type, we need to check if there is at least one token of each id
            if (bags.len() == 0) {
                return false;
            }
            // Collect references to the HashBag<OCToken>
            let bag_refs: Vec<&HashBag<OCToken>> = bags.iter().collect();
            let intersection = intersect_hashbags(&*bag_refs);
            if intersection.set_len() == 0 {
                return false;
            }
        }
        return true;
    }

    /*    pub fn compute_possible_firings(&self) -> Vec<Uuid> {
        self.petri_net
            .transitions
            .iter()
            .filter(|t| self.is_enabled(t))
            .map(|t| t.id.clone())
            .collect()
    }*/
}
