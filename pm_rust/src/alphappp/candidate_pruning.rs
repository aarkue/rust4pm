use std::{
    cmp::max,
    collections::{HashMap, HashSet},
};

use rayon::prelude::{IntoParallelRefIterator, ParallelIterator};

use crate::event_log::activity_projection::EventLogActivityProjection;

fn compute_balance(a: &Vec<usize>, b: &Vec<usize>, act_count: &Vec<u64>) -> f32 {
    let mut ai: u64 = 0;
    let mut bi: u64 = 0;
    for inc in a {
        ai += act_count[*inc];
    }
    for out in b {
        bi += act_count[*out];
    }
    let ai = ai as i32;
    let bi = bi as i32;
    let diff = (ai - bi).abs() as f32;
    let max_freq = max(ai, bi) as f32;
    return diff / max_freq;
}

fn compute_local_fitness(a: &Vec<usize>, b: &Vec<usize>, log: &EventLogActivityProjection) -> f32 {
    let mut relevant_variants_with_freq: HashMap<Vec<&usize>, u64> = HashMap::new();
    let proc_vars: Vec<Vec<&usize>> = log
        .traces
        .iter()
        .filter_map(|var| {
            let filtered_var: Vec<&usize> = var
                .iter()
                .filter(|v| a.contains(v) || b.contains(v))
                .collect();
            if filtered_var.is_empty() {
                return None;
            } else {
                return Some(filtered_var);
            }
        })
        .collect();

    proc_vars.into_iter().for_each(|var| {
        let val: &u64 = relevant_variants_with_freq.get(&var).unwrap_or(&0);
        let new_val = val + 1;
        relevant_variants_with_freq.insert(var, new_val);
    });

    let num_fitting_traces: u64 = relevant_variants_with_freq
        .iter()
        .map(|(var, freq)| -> u64 {
            let mut num_tokens = 0;
            for act in var {
                if a.contains(act) && b.contains(act) {
                    if num_tokens <= 0 {
                        return 0;
                    }
                } else {
                    if b.contains(act) {
                        num_tokens -= 1;
                    }
                    if a.contains(act) {
                        num_tokens += 1;
                    }
                }
                if num_tokens < 0 {
                    return 0;
                }
            }
            if num_tokens > 0 {
                return 0;
            } else if num_tokens < 0 {
                return 0;
            } else {
                return *freq;
            }
        })
        .sum();
    let num_relevant_traces: u64 = relevant_variants_with_freq
        .into_iter()
        .map(|(_, f)| f)
        .sum();
    return (num_fitting_traces as f32) / (num_relevant_traces as f32);
}

pub fn prune_candidates(
    cnds: &HashSet<(Vec<usize>, Vec<usize>)>,
    balance_threshold: f32,
    fitness_threshold: f32,
    log: &EventLogActivityProjection,
) -> Vec<(Vec<usize>, Vec<usize>)> {
    let mut act_count = vec![0 as u64; log.activities.len()];
    log.traces.iter().for_each(|trace| {
        trace.iter().for_each(|act| {
            act_count[*act] += 1;
        })
    });
    let filtered_cnds: Vec<(Vec<usize>, Vec<usize>)> = cnds
        .par_iter()
        .filter(|(a, b)| {
            return compute_balance(a, b, &act_count) >= balance_threshold
                && compute_local_fitness(a, b, &log) >= fitness_threshold;
        })
        .map(|(a, b)| (a.clone(), b.clone()))
        .collect();
    return filtered_cnds;
}
