use std::{
    cmp::max,
    collections::{HashMap, HashSet},
};

use rayon::prelude::{IntoParallelRefIterator, ParallelIterator};

use crate::{event_log::activity_projection::EventLogActivityProjection, END_EVENT};

fn compute_balance(a: &Vec<usize>, b: &Vec<usize>, act_count: &Vec<u64>) -> (f32, i128, i128) {
    let mut ai: u64 = 0;
    let mut bi: u64 = 0;
    for inc in a {
        ai += act_count[*inc];
    }
    for out in b {
        bi += act_count[*out];
    }
    let ai = ai as i128;
    let bi = bi as i128;
    let diff: f32 = (ai - bi).abs() as f32;
    let max_freq = max(ai, bi) as f32;
    return (diff / max_freq, ai, bi);
}

fn compute_local_fitness(
    a: &Vec<usize>,
    b: &Vec<usize>,
    log: &EventLogActivityProjection,
) -> (f32, f32) {
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
    let mut num_traces_containg_act = vec![0; log.activities.len()];
    let mut num_fitting_traces_containg_act = vec![0; log.activities.len()];
    let num_fitting_traces: u64 = relevant_variants_with_freq
        .iter()
        .map(|(var, freq)| -> u64 {
            let mut num_tokens = 0;
            let mut var_copy = var.clone();
            var_copy.sort();
            var_copy.dedup();
            for act in &var_copy {
                num_traces_containg_act[**act] += freq;
            }

            for act in var {
                // Check below would make replay more restrictive for self loops...
                // if a.contains(act) && b.contains(act) {
                //     if num_tokens <= 0 {
                //         return 0;
                //     }
                // } else {
                    if a.contains(act) {
                        num_tokens += 1;
                    }
                    if b.contains(act) {
                        num_tokens -= 1;
                    }
                }
                if num_tokens < 0 {
                    return 0;
                }
            // }
            if num_tokens > 0 {
                return 0;
            } else if num_tokens < 0 {
                return 0;
            } else {
                for act in &var_copy {
                    num_fitting_traces_containg_act[**act] += freq;
                }
                return *freq;
            }
        })
        .sum();
    let num_relevant_traces: u64 = relevant_variants_with_freq
        .into_iter()
        .map(|(_, f)| f)
        .sum();
    let min_fitness_per_act = num_traces_containg_act
        .into_iter()
        .zip(num_fitting_traces_containg_act.into_iter())
        .filter(|(num, num_fit)| num > &0)
        .map(|(num, num_fit)| num_fit as f32 / num as f32)
        .min_by(|a, b| a.partial_cmp(b).expect("Per activity fitness contains NaN"))
        .unwrap_or(0.0);
    return (
        (num_fitting_traces as f32) / (num_relevant_traces as f32),
        min_fitness_per_act,
    );
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
    // let end_act = log.act_to_index.get(END_EVENT).unwrap();
    let filtered_cnds: Vec<&(Vec<usize>, Vec<usize>)> = cnds
        .par_iter()
        .filter(|(a, b)| {
            let balance = compute_balance(a, b, &act_count);
            if balance.0 <= balance_threshold {
                let (fitness,min_per_act_fitness) = compute_local_fitness(a, b, &log);
                // if fitness >= fitness_threshold && min_per_act_fitness >= fitness_threshold {    
                //     println!("Fitness: {}, Min per act: {}", fitness, min_per_act_fitness);
                // }
                // if b.contains(&end_act) {
                //     println!(
                //         "Contains END: {:?} Balance: {}, Fitness: {}\t {:?}",
                //         (a, b),
                //         balance.0,
                //         fitness,
                //         log.activities
                //     );
                //     println!("--> : {:?}", balance);
                // }
                return fitness >= fitness_threshold && min_per_act_fitness >= fitness_threshold;
            } else {
                return false;
            }
        })
        .collect();

    let sel: Vec<(Vec<usize>, Vec<usize>)> = filtered_cnds
        .par_iter()
        .filter(|(a, b)| {
            let is_dominated = filtered_cnds.iter().any(|(a2, b2)| {
                if a2.len() >= a.len() && b2.len() >= b.len() && (a != a2 || b != b2) {
                    let a_contained = a.into_iter().all(|e| a2.contains(e));
                    if a_contained {
                        let b_contained = b.into_iter().all(|e| b2.contains(e));
                        if b_contained {
                            return true;
                        } else {
                            return false;
                        }
                    }
                }
                return false;
            });
            return !is_dominated;
        })
        .map(|(a, b)| (a.clone(), b.clone()))
        .collect();

    return sel;
}
