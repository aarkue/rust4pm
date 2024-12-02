use hashbag::HashBag;
use std::hash::{BuildHasher, Hash};
use std::cmp::min;

/// Intersects multiple `HashBag` instances, returning a new `HashBag` containing only the elements
/// present in **all** input `HashBag`s. The count for each element in the resulting `HashBag`
/// is the minimum count found across all input bags.
///
/// # Type Parameters
///
/// - `T`: The type of elements in the `HashBag`. Must implement `Clone`, `Eq`, and `Hash`.
/// - `S`: The hasher used by the first `HashBag`.
/// - `OtherS`: The hasher used by the other `HashBag`s.
///
/// # Arguments
///
/// - `bags`: A slice of `HashBag` references to intersect.
///
/// # Returns
///
/// A new `HashBag` representing the intersection of all input `HashBag`s.
///
/// # Panics
///
/// - If the `bags` slice is empty, it returns an empty `HashBag`.
///
/// # Examples
///
/// ```rust
/// use hashbag::HashBag;
///
/// fn main() {
///     let mut a = HashBag::new();
///     a.insert("apple");
///     a.insert("banana");
///     a.insert("banana");
///     a.insert("cherry");
///
///     let mut b = HashBag::new();
///     b.insert("banana");
///     b.insert("banana");
///     b.insert("dragonfruit");
///
///     let mut c = HashBag::new();
///     c.insert("banana");
///     c.insert("banana");
///     c.insert("banana"); // Extra banana
///
///     let intersection = intersect_hashbags(&[&a, &b, &c]);
///
///     // The intersection should contain "banana" with a count of 2
///     assert_eq!(intersection.len(), 2);
///     assert_eq!(intersection.contains("banana"), 2);
///     assert_eq!(intersection.contains("apple"), 0);
///     assert_eq!(intersection.contains("cherry"), 0);
///     assert_eq!(intersection.contains("dragonfruit"), 0);
///
///     println!("Intersection contains:");
///     for (item, count) in intersection.set_iter() {
///         println!("{}: {}", item, count);
///     }
/// }
/// ```
pub fn intersect_hashbags<T, S>(
    bags: &[&HashBag<T, S>],
) -> HashBag<T, S>
where
    T: Clone + Eq + Hash,
    S: BuildHasher + Clone
{
    // Find the bag with the smallest set_len to minimize iterations
    let smallest_bag = bags
        .iter()
        .min_by_key(|bag| bag.set_len())
        .expect("At least one bag must be provided");

    // Initialize the intersection HashBag with the same hasher as the smallest bag
    let mut intersection = HashBag::with_hasher(smallest_bag.hasher().clone());

    // Iterate over the distinct elements of the smallest bag
    for (item, count) in smallest_bag.set_iter() {
        // Initialize min_count with the count from the smallest bag
        let mut min_count = count;

        // Check the count of the current item in all other bags
        for bag in bags.iter() {
            if *bag == *smallest_bag {
                continue; // Skip the smallest bag itself
            }

            let other_count = bag.contains(item);

            if other_count == 0 {
                min_count = 0;
                break; // Item not present in one of the bags; no need to check further
            }

            // Update min_count to be the minimum so far
            min_count = min(min_count, other_count);

            // Early termination if min_count reaches zero
            if min_count == 0 {
                break;
            }
        }

        // If the item is present in all bags, insert it with min_count
        if min_count > 0 {
            intersection.insert_many(item.clone(), min_count);
        }
    }
 
    intersection
}

#[cfg(test)]
mod tests {
    use super::*;
    use hashbag::HashBag;

    /// Test intersecting two HashBags with overlapping elements.
    #[test]
    fn test_basic_intersection() {
        let mut a = HashBag::new();
        a.insert("apple");
        a.insert("banana");
        a.insert("banana");
        a.insert("cherry");

        let mut b = HashBag::new();
        b.insert("banana");
        b.insert("banana");
        b.insert("dragonfruit");

        let intersection = intersect_hashbags(&[&a, &b]);

        // "banana" appears twice in both bags
        assert_eq!(intersection.len(), 2);
        assert_eq!(intersection.contains("banana"), 2);

        // Elements not common to both should not appear
        assert_eq!(intersection.contains("apple"), 0);
        assert_eq!(intersection.contains("cherry"), 0);
        assert_eq!(intersection.contains("dragonfruit"), 0);
    }

    /// Test intersecting three HashBags with overlapping and non-overlapping elements.
    #[test]
    fn test_multiple_intersection() {
        let mut a = HashBag::new();
        a.insert("apple");
        a.insert("banana");
        a.insert("banana");
        a.insert("cherry");

        let mut b = HashBag::new();
        b.insert("banana");
        b.insert("banana");
        b.insert("dragonfruit");

        let mut c = HashBag::new();
        c.insert("banana");
        c.insert("banana");
        c.insert("banana"); // Extra banana

        let intersection = intersect_hashbags(&[&a, &b, &c]);

        // "banana" appears twice in both `a` and `b`, and three times in `c`
        // Minimum count is 2
        assert_eq!(intersection.len(), 2); // 2 counts of "banana"
        assert_eq!(intersection.contains("banana"), 2);

        // Elements not common to all should not appear
        assert_eq!(intersection.contains("apple"), 0);
        assert_eq!(intersection.contains("cherry"), 0);
        assert_eq!(intersection.contains("dragonfruit"), 0);
    }

    /// Test intersecting HashBags with no common elements results in an empty HashBag.
    #[test]
    fn test_no_common_elements() {
        let mut a = HashBag::new();
        a.insert("apple");
        a.insert("banana");

        let mut b = HashBag::new();
        b.insert("cherry");
        b.insert("dragonfruit");

        let intersection = intersect_hashbags(&[&a, &b]);

        // No common elements, intersection should be empty
        assert!(intersection.is_empty());
    }

    /// Test intersecting identical HashBags results in the same HashBag.
    #[test]
    fn test_identical_bags() {
        let mut a = HashBag::new();
        a.insert("apple");
        a.insert("banana");
        a.insert("banana");
        a.insert("cherry");

        let mut b = HashBag::new();
        b.insert("apple");
        b.insert("banana");
        b.insert("banana");
        b.insert("cherry");

        let intersection = intersect_hashbags(&[&a, &b]);

        // The intersection should be identical to the original bags
        assert_eq!(intersection.len(), a.len());
        assert_eq!(intersection, a);
        assert_eq!(intersection, b);
    }

    /// Test that intersect_hashbags panics when provided with an empty list.
    #[test]
    #[should_panic(expected = "At least one bag must be provided")]
    fn test_empty_list_panic() {
        // Attempting to intersect an empty list should panic
        let intersection: HashBag<&str> = intersect_hashbags(&[]);
    }

    /// Test intersecting multiple HashBags where some have varying counts of a common element.
    #[test]
    fn test_varying_counts() {
        let mut a = HashBag::new();
        a.insert("kiwi");
        a.insert("kiwi");
        a.insert("kiwi");
        a.insert("melon");
        a.insert("mango");
        a.insert("mango");

        let mut b = HashBag::new();
        b.insert("kiwi");
        b.insert("kiwi");
        b.insert("melon");

        let mut c = HashBag::new();
        c.insert("kiwi");
        c.insert("melon");
        c.insert("melon");

        let intersection = intersect_hashbags(&[&a, &b, &c]);

        // "kiwi" appears 3 times in `a`, 2 times in `b`, and 1 time in `c`
        // Minimum count is 1
        assert_eq!(intersection.len(), 2); // 1 count of "kiwi" 1 count of "melon"
        assert_eq!(intersection.contains("kiwi"), 1);
        assert_eq!(intersection.contains("melon"), 1);

        // "mango" and "melon" are not common to all
        assert_eq!(intersection.contains("mango"), 0);
    }

    /// Test intersecting with a single HashBag returns a copy of that HashBag.
    #[test]
    fn test_single_bag_intersection() {
        let mut a = HashBag::new();
        a.insert("apple");
        a.insert("banana");
        a.insert("banana");

        let intersection = intersect_hashbags(&[&a]);

        // Intersection with a single bag should be identical to that bag
        assert_eq!(intersection.len(), a.len());
        assert_eq!(intersection, a);
    }
}