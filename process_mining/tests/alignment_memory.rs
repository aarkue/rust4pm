//! Checks the memory limit of alignments against the bytes actually allocated
use std::{
    alloc::{GlobalAlloc, Layout, System},
    sync::{
        atomic::{AtomicUsize, Ordering::Relaxed},
        Mutex,
    },
};

use process_mining::{
    conformance::alignments::{
        align_variants, align_variants_with_retry, petri_net::AlignmentError, AlignmentOptions,
        VariantAlignmentResult,
    },
    core::{
        event_data::case_centric::utils::activity_projection::EventLogActivityProjection,
        process_models::petri_net::{ArcType, Marking},
    },
    utils::dijkstra_search::SearchError,
    PetriNet,
};

/// Counts live heap bytes and their peak. A reallocation counts the old and the new block until
/// it is done, as the old one may still be held while the data is copied.
struct Counting;

static LIVE: AtomicUsize = AtomicUsize::new(0);
static PEAK: AtomicUsize = AtomicUsize::new(0);

fn add(bytes: usize) {
    let now = LIVE.fetch_add(bytes, Relaxed) + bytes;
    PEAK.fetch_max(now, Relaxed);
}

unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let ptr = unsafe { System.alloc(layout) };
        if !ptr.is_null() {
            add(layout.size());
        }
        ptr
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        let ptr = unsafe { System.alloc_zeroed(layout) };
        if !ptr.is_null() {
            add(layout.size());
        }
        ptr
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) };
        LIVE.fetch_sub(layout.size(), Relaxed);
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        add(new_size);
        let new_ptr = unsafe { System.realloc(ptr, layout, new_size) };
        let freed = if new_ptr.is_null() {
            new_size
        } else {
            layout.size()
        };
        LIVE.fetch_sub(freed, Relaxed);
        new_ptr
    }
}

#[global_allocator]
static GLOBAL: Counting = Counting;

/// The counters are global, so measurements must not overlap
static MEASURING: Mutex<()> = Mutex::new(());

const MIB: usize = 1 << 20;
/// The model, the results and rayon's bookkeeping, which the limit does not cover
const ALLOWANCE: usize = MIB;

/// A silent split into `branches` branches of two activities each, then a silent join
fn concurrent_net(branches: usize) -> PetriNet {
    let mut net = PetriNet::new();
    let (start, end) = (net.add_place(None), net.add_place(None));
    let (split, join) = (
        net.add_transition(None, None),
        net.add_transition(None, None),
    );
    net.add_arc(ArcType::place_to_transition(start, split), None);
    net.add_arc(ArcType::transition_to_place(join, end), None);
    for b in 0..branches {
        let mut place = net.add_place(None);
        net.add_arc(ArcType::transition_to_place(split, place), None);
        for step in 0..2 {
            let activity = net.add_transition(Some(format!("a{b}_{step}")), None);
            net.add_arc(ArcType::place_to_transition(place, activity), None);
            place = net.add_place(None);
            net.add_arc(ArcType::transition_to_place(activity, place), None);
        }
        net.add_arc(ArcType::place_to_transition(place, join), None);
    }
    net.initial_marking = Some(Marking::from([(start, 1)]));
    net.final_markings = Some(vec![Marking::from([(end, 1)])]);
    net
}

/// Variants that list the second activities of all branches in reverse, then the first ones,
/// then `noise` unknown events, which makes the search explore many interleavings
fn hard_log(branches: usize, noise: std::ops::Range<usize>) -> EventLogActivityProjection {
    let activities: Vec<String> = (0..branches)
        .flat_map(|b| [format!("a{b}_0"), format!("a{b}_1")])
        .chain(["noise".to_string()])
        .collect();
    let act_to_index = activities
        .iter()
        .enumerate()
        .map(|(i, a)| (a.clone(), i))
        .collect();
    let traces = noise
        .map(|n| {
            let trace = (0..branches)
                .rev()
                .map(|b| 2 * b + 1)
                .chain((0..branches).map(|b| 2 * b))
                .chain(std::iter::repeat_n(2 * branches, n))
                .collect();
            (trace, 1)
        })
        .collect();
    EventLogActivityProjection {
        activities,
        act_to_index,
        traces,
    }
}

fn ran_out(results: &[VariantAlignmentResult]) -> usize {
    results
        .iter()
        .filter(|v| {
            matches!(
                v.result,
                Err(AlignmentError::SearchError(SearchError::MemoryLimitReached))
            )
        })
        .count()
}

/// Peak bytes above the live bytes before `run`, and the bytes still allocated after it
fn measure<T>(run: impl FnOnce() -> T) -> (T, usize, usize) {
    let base = LIVE.load(Relaxed);
    PEAK.store(base, Relaxed);
    let result = run();
    let peak = PEAK.load(Relaxed) - base;
    let left = LIVE.load(Relaxed).saturating_sub(base);
    (result, peak, left)
}

#[test]
fn parallel_alignments_stay_within_budget() {
    let _guard = MEASURING.lock().unwrap();
    let (net, log) = (concurrent_net(14), hard_log(14, 0..8));
    let pool = rayon::ThreadPoolBuilder::new()
        .num_threads(4)
        .build()
        .unwrap();
    let options = AlignmentOptions {
        max_memory_mib: Some(32),
        ..AlignmentOptions::default()
    };
    let (results, peak, left) = measure(|| pool.install(|| align_variants(&net, &log, &options)));
    assert!(peak <= 32 * MIB + ALLOWANCE, "peak {peak} bytes");
    assert!(ran_out(&results) > 0, "the limit was never reached");
    assert!(left <= ALLOWANCE, "{left} bytes left after the call");
}

#[test]
fn retry_stays_within_budget() {
    let _guard = MEASURING.lock().unwrap();
    let (net, log) = (concurrent_net(10), hard_log(10, 0..2));
    let pool = rayon::ThreadPoolBuilder::new()
        .num_threads(4)
        .build()
        .unwrap();
    let options = AlignmentOptions {
        max_memory_mib: Some(32),
        ..AlignmentOptions::default()
    };
    let parallel = pool.install(|| align_variants(&net, &log, &options));
    assert!(ran_out(&parallel) > 0, "the share was never exhausted");
    let (results, peak, left) =
        measure(|| pool.install(|| align_variants_with_retry(&net, &log, &options)));
    assert!(peak <= 32 * MIB + ALLOWANCE, "peak {peak} bytes");
    assert!(
        ran_out(&results) < ran_out(&parallel),
        "the retry aligned nothing more"
    );
    assert!(left <= ALLOWANCE, "{left} bytes left after the call");
}
