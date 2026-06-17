//! Benchmark the allocations and memory usage to convert EventLog to polars::DataFrame
use process_mining::core::event_data::case_centric::dataframe::convert_log_to_dataframe;
use process_mining::{test_utils::get_test_data_path, EventLog, Importable};

#[global_allocator]
static ALLOC: dhat::Alloc = dhat::Alloc;

fn main() {
    let root = get_test_data_path();
    let path = root
        .join("xes")
        .join("Road_Traffic_Fine_Management_Process.xes.gz");
    let log = EventLog::import_from_path(&path).unwrap();
    let _profiler = dhat::Profiler::builder()
        .file_name("dhat-load_dataframe.json")
        .build();

    let _df = convert_log_to_dataframe(&log, false).unwrap();
    drop(_profiler);
}
