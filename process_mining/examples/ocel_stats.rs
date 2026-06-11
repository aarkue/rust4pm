use process_mining::core::event_data::object_centric::linked_ocel::{
    LinkedOCELAccess, SlimLinkedOCEL,
};
use process_mining::Importable;
use std::env;
use std::error::Error;
use std::path::PathBuf;
use std::time::Instant;

fn main() -> Result<(), Box<dyn Error>> {
    let args: Vec<String> = env::args().collect();
    if args.len() != 2 {
        eprintln!("Usage: {} <path_to_ocel_file>", args[0]);
        std::process::exit(1);
    }

    let path = PathBuf::from(&args[1]);
    println!("Importing OCEL from {:?}", path);
    let now = Instant::now();
    let ocel = SlimLinkedOCEL::import_from_path(&path)?;
    println!("Successfully imported OCEL in {:?}.", now.elapsed());
    println!("Number of events: {}", ocel.get_all_evs().count());
    println!("Number of objects: {}", ocel.get_all_obs().count());

    println!("Event Types: {:?}", ocel.get_ev_types().collect::<Vec<_>>());
    println!(
        "Object Types: {:?}",
        ocel.get_ob_types().collect::<Vec<_>>()
    );

    let preview_n = 10;
    println!("First {} events:", preview_n);
    for ev in ocel.get_all_evs().take(preview_n) {
        let ev_type = ocel.get_ev_type_of(ev);
        let timestamp = ocel.get_ev_time(ev);
        println!(
            "Event {:?}: Type: {}, Timestamp: {}",
            ev, ev_type, timestamp
        );
        let attrs = ocel.get_ev_attrs(ev);
        for attr in attrs {
            let val = ocel.get_ev_attr_val(ev, attr);
            println!("  Attribute: {} = {:?}", attr, val);
        }
    }
    Ok(())
}
