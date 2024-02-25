use std::collections::{HashMap, HashSet};

use rayon::prelude::{IntoParallelRefIterator, ParallelIterator};

use crate::event_log::activity_projection::{
    ActivityProjectionDFG, EventLogActivityProjection, END_ACTIVITY, START_ACTIVITY,
};

/// Prefix for silent activities (used inside place candidates)
pub const SILENT_ACT_PREFIX: &str = "__SILENT__";

/// Filter weighted DFG based on absolute and relative thresholds
pub fn filter_dfg(
    dfg: &ActivityProjectionDFG,
    absolute_df_thresh: u64,
    relative_df_thresh: f32,
) -> ActivityProjectionDFG {
    let mut ret = ActivityProjectionDFG {
        nodes: dfg.nodes.clone(),
        edges: HashMap::new(),
    };
    ret.edges = dfg
        .edges
        .iter()
        .filter_map(|((a, b), v_u64)| {
            let df_inc: Vec<u64> = dfg
                .edges
                .iter()
                .filter_map(|((x, _), w)| if x == a { Some(*w) } else { None })
                .collect();
            let df_out: Vec<u64> = dfg
                .edges
                .iter()
                .filter_map(|((_, y), w)| if y == b { Some(*w) } else { None })
                .collect();
            let df_inc_sum: u64 = df_inc.iter().sum();
            let df_out_sum: u64 = df_out.iter().sum();
            let v = *v_u64 as f32;
            if *v_u64 >= absolute_df_thresh
                && ((v >= relative_df_thresh * (df_inc_sum as f32) / (df_inc.len() as f32))
                    || (v >= relative_df_thresh * (df_out_sum as f32) / (df_out.len() as f32)))
            {
                Some(((*a, *b), *v_u64))
            } else {
                None
            }
        })
        .collect();

    ret
}

/// Add artificial activities to event log projection for _skips_
pub fn add_artificial_acts_for_skips(
    log: &EventLogActivityProjection,
    df_threshold: u64,
) -> (EventLogActivityProjection, Vec<String>) {
    let mut ret = log.clone();
    let dfg = ActivityProjectionDFG::from_event_log_projection(log);
    let start_act = log.act_to_index.get(START_ACTIVITY).unwrap();
    let end_act = log.act_to_index.get(END_ACTIVITY).unwrap();
    let out_from_act: HashMap<usize, HashSet<&usize>> = dfg
        .nodes
        .iter()
        .map(|act| {
            (
                *act,
                dfg.nodes
                    .iter()
                    .filter(|x| dfg.df_between(*act, **x) >= df_threshold)
                    .collect(),
            )
        })
        .collect();

    let skips: HashMap<&usize, HashSet<&usize>> = dfg
        .nodes
        .iter()
        .filter_map(|a| {
            if dfg.df_between(*a, *a) == 0 && a != start_act {
                let out_from_a: &HashSet<&usize> = out_from_act.get(a).unwrap();
                if !out_from_a.is_empty() {
                    // Here we consider any (a,b)'s in DFG (i.e. not just >= df_threshold)
                    let can_skip: HashSet<&usize> = dfg
                        .nodes
                        .iter()
                        .filter(|x| dfg.df_between(*a, **x) > 0)
                        .filter(|b| {
                            if *b != end_act
                                && dfg.df_between(**b, **b) < df_threshold
                                && dfg.df_between(**b, *a) < df_threshold
                            {
                                let out_from_b: &HashSet<&usize> = out_from_act.get(b).unwrap();
                                out_from_a.is_superset(out_from_b)
                            } else {
                                false
                            }
                        })
                        .collect();
                    if !can_skip.is_empty() {
                        return Some((a, can_skip));
                    }
                }
            }
            None
        })
        .collect();
    // Map (skippable) activity a to the new artificial activity for this skip
    let new_artificial_acts: HashMap<usize, usize> = skips
        .iter()
        .enumerate()
        .map(|(i, (e, _))| (**e, i + ret.activities.len()))
        .collect();
    let mut new_art_acts_sorted: Vec<(usize, usize)> =
        new_artificial_acts.clone().into_iter().collect();
    let mut new_acts: Vec<String> = Vec::new();
    new_art_acts_sorted.sort_by(|(_, new_act1), (_, new_acts2)| new_act1.cmp(new_acts2));
    for (a, new_act) in new_art_acts_sorted {
        let act_name = format!("{}skip_after_{}", SILENT_ACT_PREFIX, ret.activities[a]);
        ret.activities.push(act_name.clone());
        new_acts.push(act_name.clone());
        ret.act_to_index.insert(act_name, new_act);
    }
    // Modify traces by inserting new artificial activities at appropriate places
    ret.traces = ret
        .traces
        .par_iter()
        .map(|(trace, weight)| {
            // Insert activity new_act at position i : (i,new_act)
            let mut insert_at_pos: HashMap<usize, usize> = HashMap::new();
            let mut prev: Option<&usize> = None;
            trace.iter().enumerate().for_each(|(i, e)| {
                if let Some(prev_e) = prev {
                    if skips.contains_key(prev_e) && !skips.get(prev_e).unwrap().contains(e) {
                        // Note that insert_at_pos can only be set one time for a position i
                        // As new_artificial_acts.get(prev_e) is unique for an previous event prev_e (and thus i)
                        insert_at_pos.insert(i, *new_artificial_acts.get(prev_e).unwrap());
                    }
                }
                prev = Some(e);
            });
            (
                trace
                    .iter()
                    .enumerate()
                    .flat_map(|(i, e)| {
                        if insert_at_pos.contains_key(&i) {
                            vec![*(insert_at_pos.get(&i).unwrap()), *e]
                        } else {
                            vec![*e]
                        }
                    })
                    .collect(),
                *weight,
            )
        })
        .collect();
    (ret, new_acts)
}

/// Breadth first search in DFG
/// 
/// Constructs visited sequences, stopping when encoutering a loop
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
    let mut dead_ends: HashSet<Vec<usize>> = HashSet::new();
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
                    dead_ends.insert(path.clone());
                } else {
                    expanded = true
                }
                new_paths
            })
            .collect();
    }
    finished_paths
}

/// Add artificial activities to event log projection for _loops_
pub fn add_artificial_acts_for_loops(
    log: &EventLogActivityProjection,
    df_threshold: u64,
) -> (EventLogActivityProjection, Vec<String>) {
    let mut ret = log.clone();
    let dfg = ActivityProjectionDFG::from_event_log_projection(log);
    if !log.activities.contains(&START_ACTIVITY.to_string())
        || !log.activities.contains(&END_ACTIVITY.to_string())
    {
        panic!("No Artificial START/END Activities ")
    }
    let reachable_paths = get_reachable_bf(
        *log.act_to_index.get(&START_ACTIVITY.to_string()).unwrap(),
        &dfg,
        df_threshold,
    );
    let end_act = log.act_to_index.get(&END_ACTIVITY.to_string()).unwrap();
    let taus: HashSet<(usize, usize)> = reachable_paths
        .into_iter()
        .filter(|path| path.last().unwrap() != end_act)
        .filter_map(|path| {
            if path.len() >= 2 {
                let pair = (*path.get(path.len() - 2).unwrap(), *path.last().unwrap());
                if pair.0 != pair.1 {
                    Some(pair)
                } else {
                    None
                }
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
    let mut new_acts: Vec<String> = Vec::new();
    insert_taus_between.iter().for_each(|((a, b), art_act)| {
        let art_act_name = format!(
            "{}skip_loop_{}_{}",
            SILENT_ACT_PREFIX, log.activities[*a], log.activities[*b]
        );
        ret.activities[*art_act] = art_act_name.clone();
        new_acts.push(art_act_name.clone());
        ret.act_to_index.insert(art_act_name, *art_act);
    });
    // Update traces to insert new artificial acts
    ret.traces = ret
        .traces
        .par_iter()
        .map(|(trace, weight)| {
            (
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
                        vec![*e]
                    })
                    .collect(),
                *weight,
            )
        })
        .collect();
    (ret, new_acts)
}
