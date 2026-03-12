//! Fallthrough detection utilities for the Inductive Miner.
//!
//! This module contains utilities of the fallthrough rules used by the Inductive Miner when no
//! standard cut can be discovered in the event log.
use crate::core::event_data::case_centric::EventLogClassifier;
use crate::discovery::case_centric::inductive_miner_app::fallthrough::activity_concurrent::activity_concurrent_wrapper;
use crate::discovery::case_centric::inductive_miner_app::fallthrough::activity_once_per_trace::activity_once_per_trace_wrapper;
use crate::discovery::case_centric::inductive_miner_app::fallthrough::empty_traces::empty_traces_wrapper;
use crate::discovery::case_centric::inductive_miner_app::fallthrough::fallthrough::Fallthrough;
use crate::discovery::case_centric::inductive_miner_app::fallthrough::flower_model::flower_model;
use crate::discovery::case_centric::inductive_miner_app::fallthrough::strict_tau_loop::strict_tau_loop_wrapper;
use crate::discovery::case_centric::inductive_miner_app::fallthrough::tau_loop::tau_loop_wrapper;
use crate::discovery::case_centric::inductive_miner_app::structures::parameter::{Parameter, Parameters};
use crate::EventLog;

mod activity_concurrent;
pub mod fallthrough;
mod activity_once_per_trace;
mod empty_traces;
mod flower_model;
mod strict_tau_loop;
mod tau_loop;



/// Applies the sequence of *fallthrough rules* used by the Inductive Miner to an event log.
///
/// This function iteratively evaluates predefined fallthrough in the following order:
/// - [empty_traces]
/// - [activity_once_per_trace]
/// - [activity_concurrent]
/// - [strict_tau_loop]
/// - [tau_loop]
/// - [flower_model]
///
/// Whether a Fallthrough is applied at all, is controlled by the provided parameters.
/// Note, that the Flower Model is applied nevertheless.
///
/// # Parameters
/// - log: The event log to which a Fallthrough rules are applied.
/// - event_log_classifier: classifier to identify activities in event log events
/// - parameters: the provided parameters
///
/// # Returns
/// A `Fallthrough` value representing either:
/// - a discovered process model produced by a fallthrough, or
/// - the flower model if no fallthrough applies or fallthroughs are disabled.
pub fn apply_fallthrough(
    mut log: EventLog,
    event_log_classifier: &EventLogClassifier,
    parameters: &Parameters,
) -> Fallthrough {
    let funcs: Vec<fn(EventLog, &EventLogClassifier, &Parameters) -> Fallthrough> = vec![
        empty_traces_wrapper,
        activity_once_per_trace_wrapper,
        activity_concurrent_wrapper,
        strict_tau_loop_wrapper,
        tau_loop_wrapper,
    ];

    // check if Fallthrough shall be applied by provided parameters
    if parameters.contains(&Parameter::ApplyFallthrough){
        // iterate over all fall throughs
        for apply_fallthrough in funcs {
            let ft = apply_fallthrough(log, event_log_classifier, parameters);
            if let Fallthrough::Return(returned_log) = ft {
                log = returned_log;
                continue;
            } else {
                return ft;
            }
        }
    } // else the flower model is applied

    // last possible Option: Flower Model
    flower_model(log, event_log_classifier)
}