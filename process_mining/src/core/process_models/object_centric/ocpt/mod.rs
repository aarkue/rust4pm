pub mod object_centric_process_tree_struct;

use std::collections::HashSet;
/// Object-centric process trees (OCPT)

/// OCEL object type
pub type ObjectType = String;

/// OCEL event type
pub type EventType = String;

///
/// Returns the directly-follows relations of the shuffle language
///
pub fn compute_shuffle_dfr_language<'a>(
    alphabets: &Vec<HashSet<&'a EventType>>,
) -> HashSet<(&'a EventType, &'a EventType)> {
    let mut result = HashSet::new();

    // Iterates through the alphabets and adds directly-follows relations to all other alphabets
    (0..alphabets.len()).for_each(|pos| {
        let basis_alphabet: &HashSet<&EventType> = alphabets.get(pos).unwrap();
        let remainder_alphabet: HashSet<&EventType> = alphabets
            .iter()
            .enumerate()
            .flat_map(|(i, alphabet)| {
                if i != pos {
                    alphabet.clone()
                } else {
                    HashSet::new()
                }
            })
            .collect();

        add_all_dfr_from_to_alphabets(&mut result, basis_alphabet, &remainder_alphabet)
    });

    result
}

///
/// Adds directly follows relations from one set to the other set to the given mutable set
///
pub fn add_all_dfr_from_to_alphabets<'a>(
    target_set: &mut HashSet<(&'a EventType, &'a EventType)>,
    alphabet_1: &HashSet<&'a EventType>,
    alphabet_2: &HashSet<&'a EventType>,
) {
    alphabet_1.iter().for_each(|from| {
        alphabet_2.iter().for_each(|&to| {
            target_set.insert((from, to));
        });
    });
}
