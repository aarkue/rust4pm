use std::collections::HashSet;

use rayon::prelude::*;

use crate::event_log::activity_projection::ActivityProjectionDFG;

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

fn all_dfs_between_vec(df_rel: &HashSet<(usize, usize)>, a: &Vec<usize>, b: &Vec<usize>) -> bool {
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
    return false;
}

pub fn satisfies_cnd_condition(
    df_rel: &HashSet<(usize, usize)>,
    a: &Vec<usize>,
    b: &Vec<usize>,
) -> bool {
    let a_set: HashSet<usize> = a.iter().map(|act| *act).collect();
    let b_set: HashSet<usize> = b.iter().map(|act| *act).collect();
    let a_without_b: HashSet<usize> = a_set.difference(&b_set).map(|act| *act).collect();
    let b_without_a: HashSet<usize> = b_set.difference(&a_set).map(|act| *act).collect();

    return no_df_between(df_rel, &a_set, &a_without_b)
        && no_df_between(df_rel, &b_without_a, &b_set)
        && all_dfs_between(df_rel, &a_set, &b_set)
        && not_all_dfs_between(df_rel, &b_without_a, &a_without_b);
}

pub fn build_candidates(dfg: &ActivityProjectionDFG) -> HashSet<(Vec<usize>, Vec<usize>)> {
    let df_relations: HashSet<(usize, usize)> = dfg
        .edges
        .par_iter()
        .filter_map(|((a, b), w)| if w > &0 { Some((*a, *b)) } else { None })
        .collect();
    println!("DF #{:?}", df_relations.len());
    let mut cnds: HashSet<(Vec<usize>, Vec<usize>)> = HashSet::new();
    let mut final_cnds: HashSet<(Vec<usize>, Vec<usize>)> = HashSet::new();
    (0..dfg.nodes.len()).for_each(|a| {
        (0..dfg.nodes.len()).for_each(|b| {
            if df_relations.contains(&(a, b))
                && !df_relations.contains(&(b, a))
                && !df_relations.contains(&(a, a))
                && !df_relations.contains(&(b, b))
            {
                final_cnds.insert((vec![a], vec![b]));
                cnds.insert((vec![a], vec![b]));
            } else {
                cnds.insert((vec![a], vec![b]));
            }
        });
    });

    let mut changed = true;
    let mut new_cnds: HashSet<(Vec<usize>, Vec<usize>)> = cnds.clone();
    while changed {
        changed = false;
        let added_cnds: HashSet<(Vec<usize>, Vec<usize>)> = new_cnds
            .par_iter()
            .flat_map(|(a1, b1)| {
                cnds.par_iter()
                    .filter_map(|(a2, b2)| {
                        if !all_dfs_between_vec(&df_relations, &a1, &b2)
                            || !all_dfs_between_vec(&df_relations, &a2, &b1)
                        {
                            return None;
                        }
                        let mut a = [a1.as_slice(), a2.as_slice()].concat();
                        let mut b = [b1.as_slice(), b2.as_slice()].concat();
                        if all_dfs_between_vec(&df_relations, &b, &a) {
                            return None;
                        }
                        a.sort();
                        a.dedup();
                        b.sort();
                        b.dedup();
                        if a != b
                            && !cnds.contains(&(a.clone(), b.clone()))
                            && satisfies_cnd_condition(&df_relations, &a, &b)
                        {
                            return Some((a, b));
                        }
                        return None;
                    })
                    .collect::<HashSet<(Vec<usize>, Vec<usize>)>>()
            })
            .collect();
        if added_cnds.len() > 0 {
            changed = true;
            for cnd in &added_cnds {
                final_cnds.insert(cnd.clone());
                cnds.insert(cnd.clone());
            }
            new_cnds = added_cnds;
        }
    }
    return final_cnds;
}
