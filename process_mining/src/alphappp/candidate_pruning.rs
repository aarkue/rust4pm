use std::{
    cmp::max,
    collections::{HashMap, HashSet},
};

use rayon::prelude::{IntoParallelIterator, IntoParallelRefIterator, ParallelIterator};

use crate::event_log::activity_projection::{
    EventLogActivityProjection, END_ACTIVITY, START_ACTIVITY,
};

fn compute_balance(a: &[usize], b: &[usize], act_count: &[i128]) -> f32 {
    let mut ai: i128 = 0;
    let mut bi: i128 = 0;
    for inc in a {
        ai += act_count[*inc];
    }
    for out in b {
        bi += act_count[*out];
    }
    let diff: f32 = (ai - bi).abs() as f32;
    let max_freq = max(ai, bi) as f32;
    diff / max_freq
}

fn compute_local_fitness(
    a: &[usize],
    b: &[usize],
    log: &EventLogActivityProjection,
    strict: bool,
) -> (f32, f32) {
    let mut relevant_variants_with_freq: HashMap<Vec<&usize>, u64> = HashMap::new();
    let proc_vars: Vec<(Vec<&usize>, &u64)> = log
        .traces
        .par_iter()
        .filter_map(|(var, weight)| {
            let filtered_var: Vec<&usize> = var
                .iter()
                .filter(|v| a.contains(v) || b.contains(v))
                .collect();
            if filtered_var.is_empty() {
                None
            } else {
                Some((filtered_var, weight))
            }
        })
        .collect();

    proc_vars.into_iter().for_each(|(var, w)| {
        let val: &u64 = relevant_variants_with_freq.get(&var).unwrap_or(&0);
        let new_val = val + w;
        relevant_variants_with_freq.insert(var, new_val);
    });
    let mut num_traces_containg_act = vec![0; log.activities.len()];
    let mut num_fitting_traces_containg_act = vec![0; log.activities.len()];

    let _start_act = log.act_to_index.get(&START_ACTIVITY.to_string()).unwrap();
    let _end_act = log.act_to_index.get(&END_ACTIVITY.to_string()).unwrap();

    let num_fitting_traces: i128 = relevant_variants_with_freq
        .iter()
        .map(|(var, freq)| -> i128 {
            let mut num_tokens = 0;
            let mut var_copy = var.clone();
            // if strict {
            //     // Do not consider START/END as "relevant acts" in strict mode
            //     if var_copy.contains(&start_act) {
            //         var_copy.remove(0);
            //     }
            //     if var_copy.contains(&end_act) {
            //         assert!(var_copy.pop().unwrap() == end_act);
            //     }
            //     if var_copy.is_empty() {
            //         // return 0;
            //     }
            // }
            var_copy.sort();
            var_copy.dedup();
            for act in &var_copy {
                num_traces_containg_act[**act] += freq;
            }

            for act in var {
                // Check below would make replay more restrictive for self loops...
                if strict && a.contains(act) && b.contains(act) {
                    if num_tokens <= 0 {
                        return 0;
                    }
                } else {
                    if a.contains(act) {
                        num_tokens += 1;
                    }
                    if b.contains(act) {
                        num_tokens -= 1;
                    }
                    if num_tokens < 0 {
                        return 0;
                    }
                }
            }
            if num_tokens > 0 {
                0
            } else {
                if num_tokens < 0 {
                    return 0;
                }
                for act in &var_copy {
                    num_fitting_traces_containg_act[**act] += *freq;
                }
                *freq as i128
            }
        })
        .sum();

    let num_relevant_traces: u64 = match false {
        true => relevant_variants_with_freq.into_values().sum(),
        false => relevant_variants_with_freq.into_values().sum(),
    };
    if num_relevant_traces == 0 {
        return (0.0, 0.0);
    }
    let min_fitness_per_act = num_traces_containg_act
        .into_iter()
        .zip(num_fitting_traces_containg_act)
        .filter(|(num, _)| *num > 0)
        .map(|(num, num_fit)| num_fit as f32 / num as f32)
        .min_by(|a, b| a.partial_cmp(b).expect("Per activity fitness contains NaN"))
        .unwrap_or(0.0);
    (
        (num_fitting_traces as f32) / (num_relevant_traces as f32),
        min_fitness_per_act,
    )
}

pub fn prune_candidates(
    cnds: &HashSet<(Vec<usize>, Vec<usize>)>,
    balance_threshold: f32,
    fitness_threshold: f32,
    replay_threshold: f32,
    act_count: Vec<i128>,
    log: &EventLogActivityProjection,
) -> Vec<(Vec<usize>, Vec<usize>)> {
    // let end_act = log.act_to_index.get(END_EVENT).unwrap();
    let filtered_cnds: Vec<&(Vec<usize>, Vec<usize>)> = cnds
        .par_iter()
        .filter(|(a, b)| {
            let balance = compute_balance(a, b, &act_count);
            balance <= balance_threshold
        })
        .collect();
    println!("After balance: {}", filtered_cnds.len());
    let filtered_cnds: Vec<&(Vec<usize>, Vec<usize>)> = filtered_cnds
        .into_par_iter()
        .filter(|(a, b)| {
            let (fitness, min_per_act_fitness) = compute_local_fitness(a, b, log, false);
            fitness >= fitness_threshold && min_per_act_fitness >= fitness_threshold
        })
        .collect();
    println!("After fitness: {}", filtered_cnds.len());

    let sel: Vec<(Vec<usize>, Vec<usize>)> = filtered_cnds
        .par_iter()
        .filter(|(a, b)| {
            let is_dominated = filtered_cnds.iter().any(|(a2, b2)| {
                if a2.len() >= a.len() && b2.len() >= b.len() && (a != a2 || b != b2) {
                    let a_contained = a.iter().all(|e| a2.contains(e));
                    if a_contained {
                        let b_contained = b.iter().all(|e| b2.contains(e));
                        return b_contained;
                    }
                }
                false
            });
            !is_dominated
        })
        .map(|(a, b)| (a.clone(), b.clone()))
        .collect();

    println!("After maximal (sel): {}", sel.len());
    sel.into_iter()
        .filter(|(a, b)| {
            let strict_fit = compute_local_fitness(a, b, log, true);
            // let mut a = log.acts_to_names(a);
            // let mut b = log.acts_to_names(b);
            // a.sort();
            // b.sort();
            // println!("{:?} => {:?}: {:?}", a, b, strict_fit);
            strict_fit > (replay_threshold, -1.0)
        })
        .collect()
}
