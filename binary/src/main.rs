use std::{time::Instant};


use process_mining::{event_log::ocel::xml_ocel_import::import_ocel_xml_file};

fn main() {
    let now = Instant::now();
    let ocel = import_ocel_xml_file("/home/aarkue/dow/angular_github_commits_ocel.xml");
    println!(
        "Imported OCEL with {} objects and {} events in {:#?}",
        ocel.objects.len(),
        ocel.events.len(),
        now.elapsed()
    );
}
