use hashbrown::HashMap;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// A group of schemas that connect the same set of (source, target) pairs.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct ConnectionEquivalenceClass {
    /// Representative schema (shortest display string in the class).
    pub representative: String,
    /// All schemas in this class (display strings).
    pub schemas: Vec<String>,
    /// Number of unique (source, target) connections shared by every schema in the class.
    pub connection_count: usize,
}

/// Group schemas by connection equivalence using a (support, hash) key.
///
/// Each input is `(display, support, hash)` where `hash` is the
/// order-independent 64-bit hash of the schema's (source, target) set produced by
/// discovery. Two schemas land in the same class when both their support and hash
/// match.
///
/// Returns the classes (sorted by representative for stable output) and, for each input
/// schema in order, the index of its class.
pub fn group_by_hash(
    schemas: &[(String, usize, u64)],
) -> (Vec<ConnectionEquivalenceClass>, Vec<usize>) {
    let mut key_to_class: HashMap<(usize, u64), usize> = HashMap::new();
    let mut classes: Vec<ConnectionEquivalenceClass> = Vec::new();
    for (display, support, hash) in schemas {
        match key_to_class.get(&(*support, *hash)) {
            Some(&ci) => {
                let class = &mut classes[ci];
                if display.len() < class.representative.len() {
                    class.representative = display.clone();
                }
                class.schemas.push(display.clone());
            }
            None => {
                key_to_class.insert((*support, *hash), classes.len());
                classes.push(ConnectionEquivalenceClass {
                    representative: display.clone(),
                    schemas: vec![display.clone()],
                    connection_count: *support,
                });
            }
        }
    }

    // Sort classes by representative for deterministic output, remapping the indices.
    let mut order: Vec<usize> = (0..classes.len()).collect();
    order.sort_by(|&a, &b| classes[a].representative.cmp(&classes[b].representative));
    let mut remap = vec![0usize; classes.len()];
    for (new_idx, &old_idx) in order.iter().enumerate() {
        remap[old_idx] = new_idx;
    }
    let sorted: Vec<ConnectionEquivalenceClass> =
        order.into_iter().map(|old| classes[old].clone()).collect();

    let class_of: Vec<usize> = schemas
        .iter()
        .map(|(_, support, hash)| remap[key_to_class[&(*support, *hash)]])
        .collect();
    (sorted, class_of)
}
