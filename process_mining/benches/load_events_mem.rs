//! Benchmark the allocations and memory usage taken to load from disk to EventLog
use process_mining::{test_utils::get_test_data_path, EventLog, Importable};
#[global_allocator]
static ALLOC: dhat::Alloc = dhat::Alloc;

fn main() {
    let root = get_test_data_path();
    let path = root
        .join("xes")
        .join("Road_Traffic_Fine_Management_Process.xes.gz");
    let mut _profiler = dhat::Profiler::builder()
        .file_name("dhat-load_events.json")
        .build();
    let _log = EventLog::import_from_path(&path).unwrap();
}
