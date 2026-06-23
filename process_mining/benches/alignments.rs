//! Benchmark the time taken to compute optimal alignments of a log against a Petri net
use criterion::{black_box, criterion_group, criterion_main, Criterion};
use process_mining::{
    conformance::alignments::{align_log, AlignmentOptions},
    core::event_data::case_centric::utils::activity_projection::{
        log_to_activity_projection, EventLogActivityProjection,
    },
    test_utils::get_test_data_path,
    EventLog, Importable, PetriNet,
};
use std::time::Duration;

fn load(log_name: &str, net_name: &str) -> (PetriNet, EventLogActivityProjection) {
    let root = get_test_data_path();
    let log = EventLog::import_from_path(root.join("xes").join(log_name)).unwrap();
    let net = PetriNet::import_pnml(root.join("petri-net").join(net_name)).unwrap();
    (net, log_to_activity_projection(&log))
}

fn bench_alignments(c: &mut Criterion) {
    let options = AlignmentOptions::default();

    let (sepsis_net, sepsis_proj) =
        load("Sepsis Cases - Event Log.xes.gz", "sepsis-DISCovered.apnml");
    let mut sepsis = c.benchmark_group("alignments");
    sepsis.sample_size(25);
    sepsis.measurement_time(Duration::from_secs(20));
    sepsis.bench_function("sepsis", |b| {
        b.iter(|| black_box(align_log(&sepsis_net, &sepsis_proj, &options)))
    });
    sepsis.finish();

    let (rtfm_net, rtfm_proj) = load(
        "Road_Traffic_Fine_Management_Process.xes.gz",
        "rtfm-imf-02.apnml",
    );
    let mut rtfm = c.benchmark_group("alignments");
    rtfm.sample_size(10);
    rtfm.measurement_time(Duration::from_secs(60));
    rtfm.bench_function("rtfm", |b| {
        b.iter(|| black_box(align_log(&rtfm_net, &rtfm_proj, &options)))
    });
    rtfm.finish();
}

criterion_group!(benches, bench_alignments);
criterion_main!(benches);
