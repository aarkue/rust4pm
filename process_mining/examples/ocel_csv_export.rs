//! Example: Export OCEL to CSV format
//!
//! This example demonstrates importing an OCEL file (XML/JSON) and exporting it to
//! the CSV-based OCEL format.
//!
//! # CSV Format Behavior
//!
//! The CSV export follows these rules:
//!
//! ## Object Attribute Changes
//!
//! - **At event time**: If an object has attribute changes at the exact same timestamp as an
//!   event that involves that object, the attributes are included inline in the object reference
//!   (e.g., `i1#qualifier{"attr": "value"}`).
//!
//! - **Standalone changes**: If object attributes change at a time when no event involves that
//!   object, a separate row is created with empty `id` and `activity` columns.
//!
//! - **Multiple attributes at same time**: All attribute changes for an object at the same
//!   timestamp are combined into a single JSON object.
//!
//! - **Unchanged values**: The format does not track whether values actually changed - it records
//!   all attribute values at their specified timestamps. If the OCEL source has duplicate entries
//!   with the same value, they will all be exported.
//!
//! ## Object-to-Object (O2O) Relationships
//!
//! - O2O relationships are exported as rows with `o2o` in the activity column.
//! - The source object ID is in the `id` column.
//! - Target objects are in the appropriate `ot:` columns with their qualifiers.
//!
//! - Event attribute types are inferred as strings during CSV import
//! - Column ordering may differ
//!
//! # Usage
//!
//! ```bash
//! cargo run --example ocel_csv_export -- <input_ocel.xml|json> <output.ocel.csv>
//! ```

use process_mining::core::event_data::object_centric::ocel_csv::export_ocel_csv_to_path;
use process_mining::{Importable, OCEL};
use std::env;
use std::error::Error;
use std::path::PathBuf;

fn main() -> Result<(), Box<dyn Error>> {
    let args: Vec<String> = env::args().collect();
    if args.len() < 2 || args.len() > 3 {
        eprintln!("Usage: {} <input_ocel_file> [output.ocel.csv]", args[0]);
        eprintln!();
        eprintln!("Imports an OCEL file (XML/JSON) and exports it to CSV format.");
        eprintln!();
        eprintln!("Arguments:");
        eprintln!("  input_ocel_file   Path to input OCEL file (.xml, .json, .jsonocel)");
        eprintln!("  output.ocel.csv   Optional output path (default: input name + .ocel.csv)");
        std::process::exit(1);
    }

    let input_path = PathBuf::from(&args[1]);
    let output_path = if args.len() == 3 {
        PathBuf::from(&args[2])
    } else {
        // Default output: same name with .ocel.csv extension
        let stem = input_path
            .file_stem()
            .and_then(|s| s.to_str())
            .unwrap_or("output");
        input_path.with_file_name(format!("{}.ocel.csv", stem))
    };

    println!("Importing OCEL from {:?}", input_path);
    let ocel = OCEL::import_from_path(&input_path)?;

    println!("Successfully imported OCEL:");
    println!("  Events: {}", ocel.events.len());
    println!("  Objects: {}", ocel.objects.len());
    println!(
        "  Event Types: {}",
        ocel.event_types
            .iter()
            .map(|et| et.name.as_str())
            .collect::<Vec<_>>()
            .join(", ")
    );
    println!(
        "  Object Types: {}",
        ocel.object_types
            .iter()
            .map(|ot| ot.name.as_str())
            .collect::<Vec<_>>()
            .join(", ")
    );

    // Count object attributes and O2O relationships
    let total_obj_attrs: usize = ocel.objects.iter().map(|o| o.attributes.len()).sum();
    let total_o2o_rels: usize = ocel.objects.iter().map(|o| o.relationships.len()).sum();
    println!("  Object Attributes: {}", total_obj_attrs);
    println!("  O2O Relationships: {}", total_o2o_rels);

    println!();
    println!("Exporting to CSV: {:?}", output_path);

    // Use default options (includes O2O and object attribute changes)
    export_ocel_csv_to_path(&ocel, &output_path)?;

    println!("Successfully exported OCEL to CSV format.");

    // Print a preview of the file
    println!();
    println!("Preview of output (first 10 lines):");
    println!("---");
    let content = std::fs::read_to_string(&output_path)?;
    for (i, line) in content.lines().take(10).enumerate() {
        // Truncate long lines for display
        let display_line = if line.len() > 120 {
            format!("{}...", &line[..117])
        } else {
            line.to_string()
        };
        println!("{}: {}", i + 1, display_line);
    }
    if content.lines().count() > 10 {
        println!("... ({} more lines)", content.lines().count() - 10);
    }
    println!("---");

    Ok(())
}
