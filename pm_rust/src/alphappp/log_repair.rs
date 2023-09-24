use std::collections::{HashMap, HashSet};

use rayon::prelude::{IntoParallelRefIterator, ParallelIterator};

use crate::{
    event_log::activity_projection::{ActivityProjectionDFG, EventLogActivityProjection},
    END_EVENT, START_EVENT,
};

pub fn add_artificial_acts_for_skips(
    log: EventLogActivityProjection,
    df_threshold: u64,
) -> EventLogActivityProjection {
    let mut ret = log.clone();
    let dfg = ActivityProjectionDFG::from_event_log_projection(&log);
    let mut skips: HashMap<&usize, HashSet<&usize>> = HashMap::new();
    dfg.nodes.iter().for_each(|a| {
        if dfg.df_between(*a, *a) == 0 {
            let out_from_a: HashSet<&usize> = dfg
                .nodes
                .iter()
                .filter(|x| dfg.df_between(*a, **x) >= df_threshold)
                .collect();
            // Here we consider any (a,b)'s in DFG (i.e. not just >= df_threshold)
            let can_skip: HashSet<&usize> = dfg
                .nodes
                .iter()
                .filter(|x| dfg.df_between(*a, **x) > 0)
                .filter(|b| {
                    if log.activities[**b] != START_EVENT
                        && log.activities[**b] != END_EVENT
                        && dfg.df_between(**b, **b) < df_threshold
                        && dfg.df_between(**b, *a) < df_threshold
                    {
                        let out_from_b: HashSet<&usize> = dfg
                            .nodes
                            .iter()
                            .filter(|x| dfg.df_between(**b, **x) >= df_threshold)
                            .collect();
                        return out_from_a.is_superset(&out_from_b);
                    } else {
                        return false;
                    }
                })
                .collect();
            if can_skip.len() > 0 {
                skips.insert(a, can_skip);
            }
        }
    });
    // Map (skippable) activity a to the new artificial activity for this skip
    let new_artificial_acts: HashMap<usize, usize> = skips
        .iter()
        .enumerate()
        .map(|(i, (e, _))| (**e, i + ret.activities.len()))
        .collect();
    println!(
        "Adding new artificial activities ({:?} total): {:?}",
        new_artificial_acts.len(),
        new_artificial_acts.values()
    );
    let mut new_art_acts_sorted: Vec<(usize, usize)> =
        new_artificial_acts.clone().into_iter().collect();
    new_art_acts_sorted.sort_by(|(_, new_act1), (_, new_acts2)| new_act1.cmp(new_acts2));
    for (a, new_act) in new_art_acts_sorted {
        let act_name = format!("skip_after_{}", ret.activities[a]);
        ret.activities.push(act_name.clone());
        ret.act_to_index.insert(act_name, new_act);
    }

    skips.iter().for_each(|(a, _)| {
        println!(
            "Skippable: '{:?}': ({:?}) {:?}",
            ret.activities[**a],
            skips.get(a).unwrap().len(),
            skips
                .get(a)
                .unwrap()
                .iter()
                .map(|act| ret.activities[**act].clone())
                .collect::<Vec<String>>()
        );
    });

    // Modify traces by inserting new artificial activities at appropriate places
    ret.traces = ret
        .traces
        .par_iter()
        .map(|trace| {
            // Insert activity new_act at position i : (i,new_act)
            let mut insert_at_pos: HashMap<usize, usize> = HashMap::new();
            let mut prev: Option<&usize> = None;
            trace.iter().enumerate().for_each(|(i, e)| {
                match prev {
                    Some(prev_e) => {
                        if skips.contains_key(prev_e) && !skips.get(prev_e).unwrap().contains(e) {
                            // Note that insert_at_pos can only be set one time for a position i
                            // As new_artificial_acts.get(prev_e) is unique for an previous event prev_e (and thus i)
                            insert_at_pos.insert(i, *new_artificial_acts.get(prev_e).unwrap());
                        }
                    }
                    None => {}
                }
                prev = Some(e);
            });
            trace
                .iter()
                .enumerate()
                .map(|(i, e)| {
                    if insert_at_pos.contains_key(&i) {
                        return vec![*(insert_at_pos.get(&i).unwrap()), *e];
                    } else {
                        return vec![*e];
                    }
                })
                .flatten()
                .collect()
        })
        .collect();
    return ret;
}

pub fn get_reachable_bf(
    act: usize,
    dfg: &ActivityProjectionDFG,
    df_threshold: u64,
) -> HashSet<Vec<usize>> {
    let mut current_paths: HashSet<Vec<usize>> = dfg
        .df_postset_of(act, df_threshold)
        .map(|b| vec![act, b])
        .collect();
    let mut finished_paths: HashSet<Vec<usize>> = HashSet::new();
    let mut expanded = true;
    while expanded {
        expanded = false;
        current_paths = current_paths
            .into_iter()
            .flat_map(|path| {
                let new_paths: Vec<Vec<usize>> = dfg
                    .df_postset_of(*path.last().unwrap(), df_threshold)
                    .filter_map(|b| {
                        let mut new_path = path.clone();
                        new_path.push(b);

                        if path.contains(&b) {
                            finished_paths.insert(new_path);
                            // Loop found!
                            None
                        } else {
                            Some(new_path)
                        }
                    })
                    .collect();
                if new_paths.is_empty() {
                    // Can't expand any further
                    finished_paths.insert(path);
                } else {
                    expanded = true
                }
                new_paths
            })
            .collect();
    }
    return finished_paths;
}

pub fn add_artificial_acts_for_loops(
    log: EventLogActivityProjection,
    df_threshold: u64,
) -> EventLogActivityProjection {
    let mut ret = log.clone();
    let dfg = ActivityProjectionDFG::from_event_log_projection(&log);
    if !log.activities.contains(&START_EVENT.to_string())
        || !log.activities.contains(&END_EVENT.to_string())
    {
        panic!("No Artificial START/END Activities ")
    }
    let reachable_paths = get_reachable_bf(
        *log.act_to_index.get(&START_EVENT.to_string()).unwrap(),
        &dfg,
        df_threshold,
    );
    let end_act = log.act_to_index.get(&END_EVENT.to_string()).unwrap();
    let loops: Vec<Vec<usize>> = reachable_paths
        .into_iter()
        .filter(|path| path.last().unwrap() != end_act)
        .collect();
    let taus: HashSet<(usize, usize)> = loops
        .iter()
        .filter_map(|path| {
            if path.len() >= 2 {
                Some((
                    *path.get(path.len() - 2).unwrap(),
                    *path.get(path.len() - 1).unwrap(),
                ))
            } else {
                None
            }
        })
        .collect();
    let insert_taus_between: HashMap<(usize, usize), usize> = taus
        .into_iter()
        .enumerate()
        .map(|(i, e)| (e, log.activities.len() + i))
        .collect();
    // Add artificial activities to ret
    ret.activities
        .append(&mut vec![String::new(); insert_taus_between.len()]);
    println!(
        "before: {:?} {:?} {:?}",
        insert_taus_between.len(),
        log.activities.len(),
        ret.activities.len()
    );
    println!("art acts: {:?}", insert_taus_between);
    insert_taus_between.iter().for_each(|((a, b), art_act)| {
        let art_act_name = format!("skip_loop_{}_{}", log.activities[*a], log.activities[*b]);
        ret.activities[*art_act] = art_act_name.clone();
        ret.act_to_index.insert(art_act_name, *art_act);
    });
    // Update traces to insert new artificial acts
    ret.traces = ret
        .traces
        .par_iter()
        .map(|trace| {
            trace
                .iter()
                .enumerate()
                .flat_map(|(i, e)| {
                    if i > 0 {
                        // Pair consists of previous activity and current activity
                        let pair = &(*trace.get(i - 1).unwrap(), *e);
                        if insert_taus_between.contains_key(pair) {
                            return vec![*(insert_taus_between.get(pair).unwrap()), *e];
                        }
                    }
                    return vec![*e];
                })
                .collect()
        })
        .collect();
    return ret;
}
