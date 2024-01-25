use process_mining::import_xes_file;

fn main() {
    let log = import_xes_file("/home/aarkue/dow/event_logs/DomesticDeclarations.xes", None);
    println!("Imported event log with {} traces", log.traces.len());
}
