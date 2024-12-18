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

    /// Returns all possible firing combinations for the given transition.
    pub fn get_firing_combinations(
        &self,
        transition: &Transition,
    ) -> Vec<Binding> {
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

        let default: HashBag<OCToken> = HashBag::new();
        let mut input_place_map: HashMap<String, Vec<(&Uuid, HashBag<OCToken>, usize)>> =
            HashMap::new();

        // Group input places by object_type along with their required token counts
        for (place_id, arcs) in arcs_to_place.iter() {
            let place = self.petri_net.get_place(place_id).expect("Place not found");
            let obj_type = place.object_type.clone();
            let consuming_arc_count = arcs.len();
            let bag = self.assignments.get(place_id).unwrap_or(&default);

            // Filter the bag to retain only tokens with a count >= consuming_arc_count
            let mut filtered_bag = bag.clone();
            filtered_bag.retain(|_, count| {
                if count >= consuming_arc_count {
                    return count;
                }
                return 0;
            });

            input_place_map
                .entry(obj_type)
                .or_insert_with(Vec::new)
                .push((place_id, filtered_bag, consuming_arc_count));
        }
        
        let mut obj_type_tokens: Vec<Vec<Vec<PlaceBindingInfo>>> = Vec::new();

        // For each object type, find tokens that satisfy all input places
        for (_obj_type, places) in input_place_map.iter() {
            let common_tokens =
                intersect_hashbags(&*places.iter().map(|(_, bag, _)| bag).collect::<Vec<_>>());

            // If there are no common tokens, we can't fire the transition
            if (common_tokens.len() == 0) {
                return vec![];
            }

            // TODO add variable arc support

            obj_type_tokens.push(
                common_tokens
                    .set_iter()
                    .map(|(token, _)| {
                        places
                            .iter()
                            .map(|(place_id, _, req)| {
                                (PlaceBindingInfo {
                                    consumed: req.clone(),
                                    token: token.clone(),
                                    place_id: *place_id.clone(),
                                })
                            })
                            .collect()
                    })
                    .collect(),
            )
        }

        // Compute cartesian product of tokens across all object types
        if obj_type_tokens.is_empty() {
            return vec![];
        }

        let product = cartesian_product_iter(obj_type_tokens);

        // Convert each product into a firing combination map

        product.into_iter().map(|combination| {
            Binding::from_combinations(transition.id, combination)
        }).collect()
    }

    /// Checks if the transition is enabled by verifying if there is at least one firing combination.
    pub fn is_enabled(&self, transition: &Transition) -> bool {
        !self.get_firing_combinations(transition).is_empty()
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

impl Binding {
    fn from_combinations(transition_id: Uuid, combinations: Vec<Vec<PlaceBindingInfo>>) -> Self { 
        Binding {
            tokens: combinations
                .iter()
                .fold(HashMap::new(), |mut acc, bindings| {
                    bindings.into_iter().for_each(|binding| {
                        acc.insert(binding.place_id, binding.clone()); // fixme clone
                    });
                    acc
                }),
            transition_id,
        }
    }
}

#[derive(Debug, Clone)]
struct PlaceBindingInfo {
    pub place_id: Uuid,
    pub consumed: usize,
    pub token: OCToken,
}
struct Binding {
    /// Tokens to take out of the place
    pub tokens: HashMap<Uuid, PlaceBindingInfo>,
    pub transition_id: Uuid,
}

fn cartesian_product<'a, T>(inputs: Vec<Vec<&'a T>>) -> Vec<Vec<&'a T>> {
    inputs.into_iter().fold(vec![Vec::new()], |acc, pool| {
        acc.into_iter()
            .flat_map(|combination| {
                pool.iter().map(move |&item| {
                    let mut new_combination = combination.clone();
                    new_combination.push(item);
                    new_combination
                })
            })
            .collect()
    })
}

fn cartesian_product_iter<T: Clone>(inputs: Vec<Vec<T>>) -> Vec<Vec<T>> {
    inputs.into_iter().fold(vec![Vec::new()], |acc, pool| {
        acc.into_iter()
            .flat_map(|combination| {
                pool.iter().map(move |item| {
                    let mut new_combination = combination.clone();
                    new_combination.push(item.clone());
                    new_combination
                })
            })
            .collect()
    })
}
