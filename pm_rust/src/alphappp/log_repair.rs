use std::collections::{HashMap, HashSet};

use crate::{
    event_log::activity_projection::{ActivityProjectionDFG, EventLogActivityProjection},
    END_EVENT, START_EVENT,
};


pub fn add_artificial_acts_for_skips(
    log: EventLogActivityProjection,
    dfg_threshold: u64,
) -> EventLogActivityProjection {
    let mut ret = log.clone();
    let dfg = ActivityProjectionDFG::from_event_log_projection(&log);
    let mut skips: HashMap<&usize, HashSet<&usize>> = HashMap::new();
    dfg.nodes.iter().for_each(|a| {
        if dfg.df_between(*a, *a) == 0 {
            let out_from_a: HashSet<&usize> = dfg
                .nodes
                .iter()
                .filter(|x| dfg.df_between(*a, **x) >= dfg_threshold)
                .collect();
              // Here we consider any (a,b)'s in DFG (i.e. not just >= dfg_threshold)
            let can_skip: HashSet<&usize> = dfg
                .nodes
                .iter()
                .filter(|x| dfg.df_between(*a, **x) > 0)
                .filter(|b| {
                    if log.activities[**b] != START_EVENT
                        && log.activities[**b] != END_EVENT
                        && dfg.df_between(**b, **b) < dfg_threshold
                        && dfg.df_between(**b, *a) < dfg_threshold
                    {
                        let out_from_b: HashSet<&usize> = dfg
                            .nodes
                            .iter()
                            .filter(|x| dfg.df_between(**b, **x) >= dfg_threshold)
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
        .map(|(i, (e, _))| (**e, i + ret.activities.len() + 1))
        .collect();
    println!(
        "Adding new artificial activities ({:?} total): {:?}",
        new_artificial_acts.len(),
        new_artificial_acts.values()
    );
    skips.iter().for_each(|(a, _)| {
        println!("Skippable: {:?}", ret.activities[**a]);
    });
    // Modify traces by inserting new artificial activities at appriate places
    ret.traces = ret
        .traces
        .iter()
        .map(|trace| {
            // Insert activity new_act at position i : (i,new_act)
            let mut insert_at_pos: HashMap<usize, usize> = HashMap::new();
            let mut prev: Option<&usize> = None;
            trace.iter().enumerate().for_each(|(i, e)| {
                match prev {
                    Some(prev_e) => {
                        if skips.contains_key(prev_e) && skips.get(prev_e).unwrap().contains(e) {
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
