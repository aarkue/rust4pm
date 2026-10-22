//! Cheap check of whether a net can reach its final marking at all.

use crate::conformance::alignments::sync_prod_net::ModelNet;

/// Whether the final marking can be reached from the initial marking.
///
/// Reaching it means firing each transition some number of times, such that the tokens they
/// consume and produce add up to `final - initial` in every place.
/// A return value of `false` proves the final marking is unreachable,
/// while `true` only means that reachability could not be disproved (still may be unreachable).
pub(crate) fn final_marking_reachable(net: &ModelNet) -> bool {
    let num_transitions = net.transitions.len();
    // One equation per place: how much each transition changes its token count,
    // and the change the place needs overall (last entry)
    let mut equations = vec![vec![0i128; num_transitions + 1]; net.num_places];
    for (transition_index, arcs) in net.arcs.iter().enumerate() {
        for (place, weight) in &arcs.inputs {
            equations[*place][transition_index] -= i128::from(*weight);
        }
        for (place, weight) in &arcs.outputs {
            equations[*place][transition_index] += i128::from(*weight);
        }
    }
    for (place, equation) in equations.iter_mut().enumerate() {
        equation[num_transitions] =
            i128::from(net.final_marking[place]) - i128::from(net.initial_marking[place]);
    }

    // Gaussian elimination into row echelon form:
    // Each transition picks an equation with a nonzero entry for it (its pivot)
    // and subtracts a multiple of it from every equation below, zeroing the transition there.
    // `pivots` counts the equations retired as a pivot.
    // Equations above a pivot are never touched again, which is fine:
    // An unsatisfiable equation is never a pivot itself, so the downward pass alone empties it.

    let mut pivots = 0;
    for transition_index in 0..num_transitions {
        let Some(candidate) =
            (pivots..equations.len()).find(|eq| equations[*eq][transition_index] != 0)
        else {
            // No equation left mentions this transition, so it is unconstrained
            continue;
        };
        equations.swap(pivots, candidate);
        let (claimed, rest) = equations.split_at_mut(pivots + 1);
        let pivot = &claimed[pivots][transition_index..];
        for equation in rest {
            if equation[transition_index] == 0 {
                continue;
            }
            // Scale both equations so that this transition cancels out of the second one. Whole
            // numbers keep that exact; once they no longer multiply, give up and claim nothing.
            let (pivot_coefficient, coefficient) = (pivot[0], equation[transition_index]);
            for (value, pivot_value) in equation[transition_index..].iter_mut().zip(pivot) {
                let Some(eliminated) = value
                    .checked_mul(pivot_coefficient)
                    .zip(pivot_value.checked_mul(coefficient))
                    .and_then(|(value, pivot_value)| value.checked_sub(pivot_value))
                else {
                    return true;
                };
                *value = eliminated;
            }
        }
        pivots += 1;
        if pivots == equations.len() {
            break;
        }
    }

    // An equation left without any transition, yet still demanding a change, cannot be satisfied
    !equations.iter().any(|equation| {
        equation[num_transitions] != 0 && equation[..num_transitions].iter().all(|v| *v == 0)
    })
}
