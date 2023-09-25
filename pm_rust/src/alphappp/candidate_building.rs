use std::collections::HashSet;

use rayon::prelude::*;

use crate::event_log::activity_projection::{ActivityProjectionDFG, EventLogActivityProjection};

fn no_df_between(df_rel: &HashSet<(usize, usize)>, a: &HashSet<usize>, b: &HashSet<usize>) -> bool {
    for &a1 in a {
        for &b1 in b {
            if df_rel.contains(&(a1, b1)) {
                return false;
            }
        }
    }
    return true;
}

fn all_dfs_between(
    df_rel: &HashSet<(usize, usize)>,
    a: &HashSet<usize>,
    b: &HashSet<usize>,
) -> bool {
    for &a1 in a {
        for &b1 in b {
            if !df_rel.contains(&(a1, b1)) {
                return false;
            }
        }
    }
    return true;
}
fn not_all_dfs_between(
    df_rel: &HashSet<(usize, usize)>,
    a: &HashSet<usize>,
    b: &HashSet<usize>,
) -> bool {
    for &a1 in a {
        for &b1 in b {
            if !df_rel.contains(&(a1, b1)) {
                return true;
            }
        }
    }
    return true;
}

fn satisfies_cnd_condition(
    df_rel: &HashSet<(usize, usize)>,
    a: &Vec<usize>,
    b: &Vec<usize>,
) -> bool {
    let a_set: HashSet<usize> = a.iter().map(|act| *act).collect();
    let b_set: HashSet<usize> = b.iter().map(|act| *act).collect();
    let a_without_b: HashSet<usize> = a_set.difference(&b_set).map(|act| *act).collect();
    let b_without_a: HashSet<usize> = b_set.difference(&a_set).map(|act| *act).collect();

    // HashSet::from(&a.clone()).difference(HashSet::from(b.clone()));
    return no_df_between(df_rel, &a_set, &a_without_b)
        && no_df_between(df_rel, &b_without_a, &b_set)
        && all_dfs_between(df_rel, &a_set, &b_set)
        && not_all_dfs_between(df_rel, &b_without_a, &a_without_b);
}
pub fn build_candidates(log: &EventLogActivityProjection) -> HashSet<(Vec<usize>, Vec<usize>)> {
    let df_relations: HashSet<(usize, usize)> =
        ActivityProjectionDFG::from_event_log_projection(&log)
            .edges
            .into_iter()
            .filter_map(|((a, b), w)| if w > 0 { Some((a, b)) } else { None })
            .collect();

    let mut cnds: HashSet<(Vec<usize>, Vec<usize>)> = HashSet::new();
    let mut final_cnds: HashSet<(Vec<usize>, Vec<usize>)> = HashSet::new();
    // let mut expand_cnds: HashSet<(Vec<usize>, Vec<usize>)> = HashSet::new();
    (0..log.activities.len()).for_each(|a| {
        (0..log.activities.len()).for_each(|b| {
            if df_relations.contains(&(a, b))
                && !df_relations.contains(&(b, a))
                && df_relations.contains(&(a, a))
                && df_relations.contains(&(b, b))
            {
                final_cnds.insert((vec![a], vec![b]));
                cnds.insert((vec![a], vec![b]));
            } else {
                // expand_cnds.insert((vec![a], vec![b]));
                cnds.insert((vec![a], vec![b]));
            }
        });
    });

    let mut changed = true;
    while changed {
        changed = false;
        let new_cnds: HashSet<(Vec<usize>, Vec<usize>)> = cnds.iter().flat_map(|(a1, b1)| {
          cnds.par_iter().filter_map(|(a2, b2)| {
                let mut a = [a1.as_slice(), a2.as_slice()].concat();
                let mut b = [b1.as_slice(), b2.as_slice()].concat();
                a.sort();
                b.sort();
                if a != b && satisfies_cnd_condition(&df_relations, &a, &b) {
                    Some((a,b))
                }else {
                  None
                }
            }).collect::<HashSet<(Vec<usize>,Vec<usize>)>>()
        }).collect();
        if new_cnds.len() > 0 {
            changed = true;
            println!("Changed! New cnds: {:?}",new_cnds.len());
            for cnd in new_cnds {
                final_cnds.insert(cnd.clone());
                cnds.insert(cnd);
            }
        }
      }
      return final_cnds;
}
