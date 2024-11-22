use crate::oc_petri_net::oc_petri_net::ObjectCentricPetriNet;
use hashbag::HashBag;
use std::collections::HashMap;
use std::sync::Arc;
use uuid::Uuid;

//static COUNTER: AtomicUsize = AtomicUsize::new(0);

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub struct OCToken {
    //id: usize,
    obj_id: String,
}

impl OCToken {
    pub fn new(obj_id: Option<String>) -> Self {
        Self {
            //id: COUNTER.fetch_add(1, Ordering::Relaxed),
            obj_id: obj_id.unwrap_or(Uuid::new_v4().to_string()),
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

    pub fn add_initial_token_count(&mut self, place_id: Uuid, count: u64) {
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
            .entry(place_id)
            .or_insert_with(|| HashBag::new());
        
        for _ in 0..count {
            let token = OCToken::new(None);
            bag.insert(token);
        }
    }

    pub fn add_initial_tokens(&mut self, place_id: Uuid, tokens: &HashBag<OCToken>) {
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
    pub fn _add_all_tokens_unsafe(&mut self, place_id: Uuid, tokens: &HashBag<OCToken>) {
        let bag = self
            .assignments
            .entry(place_id)
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
}
