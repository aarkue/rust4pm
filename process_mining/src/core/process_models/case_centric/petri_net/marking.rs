//! Markings of Petri nets
use std::collections::BTreeMap;

use schemars::JsonSchema;
use serde::{Deserialize, Deserializer, Serialize};

use super::net::PlaceID;

/// Tokens per place of a Petri net
///
/// Places without tokens are not stored, so equal markings compare (and hash) equal. Iteration
/// is in place order. In JSON, a marking is an object from place index to tokens (e.g., `{"0": 1}`).
#[derive(Debug, Clone, Default, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, JsonSchema)]
#[serde(transparent)]
pub struct Marking(BTreeMap<PlaceID, u32>);

impl Marking {
    /// Create an empty marking
    pub fn new() -> Self {
        Self::default()
    }

    /// The tokens in `place`
    pub fn tokens(&self, place: PlaceID) -> u32 {
        self.0.get(&place).copied().unwrap_or(0)
    }

    /// Put exactly `tokens` tokens into `place`
    pub fn set(&mut self, place: PlaceID, tokens: u32) {
        if tokens == 0 {
            self.0.remove(&place);
        } else {
            self.0.insert(place, tokens);
        }
    }

    /// Add `tokens` tokens to `place`
    ///
    /// Panics if the place would hold more than `u32::MAX` tokens.
    pub fn add(&mut self, place: PlaceID, tokens: u32) {
        let total = self
            .tokens(place)
            .checked_add(tokens)
            .expect("token count overflows u32");
        self.set(place, total);
    }

    /// The marked places with their tokens, in place order
    pub fn iter(&self) -> impl ExactSizeIterator<Item = (PlaceID, u32)> + '_ {
        self.0.iter().map(|(place, tokens)| (*place, *tokens))
    }

    /// Whether no place holds a token
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

/// Sums the tokens of places that occur more than once
impl FromIterator<(PlaceID, u32)> for Marking {
    fn from_iter<I: IntoIterator<Item = (PlaceID, u32)>>(iter: I) -> Self {
        let mut marking = Self::new();
        for (place, tokens) in iter {
            marking.add(place, tokens);
        }
        marking
    }
}

impl<'de> Deserialize<'de> for Marking {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        Ok(BTreeMap::<PlaceID, u32>::deserialize(deserializer)?
            .into_iter()
            .collect())
    }
}

impl<const N: usize> From<[(PlaceID, u32); N]> for Marking {
    fn from(tokens: [(PlaceID, u32); N]) -> Self {
        tokens.into_iter().collect()
    }
}

#[cfg(test)]
mod tests {
    use super::Marking;
    use crate::core::process_models::case_centric::petri_net::net::PlaceID;

    #[test]
    fn zero_tokens_are_not_stored() {
        let mut m = Marking::from([(PlaceID(0), 1), (PlaceID(1), 0)]);
        assert_eq!(m, Marking::from([(PlaceID(0), 1)]));
        m.set(PlaceID(0), 0);
        assert!(m.is_empty());
        assert_eq!(m, Marking::new());
    }

    #[test]
    fn duplicates_sum_and_iteration_is_in_place_order() {
        let m: Marking = [(PlaceID(2), 1), (PlaceID(0), 3), (PlaceID(2), 1)]
            .into_iter()
            .collect();
        assert_eq!(
            m.iter().collect::<Vec<_>>(),
            vec![(PlaceID(0), 3), (PlaceID(2), 2)]
        );
        assert_eq!(m.tokens(PlaceID(1)), 0);
    }
}
