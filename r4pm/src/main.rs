use std::{fs::File, io::BufWriter, path::PathBuf};

pub use process_mining::bindings;
use process_mining::{
    bindings::{RegistryItem, get_fn_binding},
    core::event_data::{
        case_centric::xes::{XESImportOptions, import_xes_file},
        object_centric::{linked_ocel::IndexLinkedOCEL, ocel_json::import_ocel_json_from_path},
    },
};
use serde_json::Value;

fn main() {
    let functions = bindings::list_functions();
    println!("Available functions: {:?}", functions);
    let mut state = bindings::AppState::default();

    let args: Vec<String> = std::env::args().collect();
    let func_name = &args[1];
    let binding = get_fn_binding(func_name).expect("Unknown function name!");
    let fn_args = (binding.args)();
    println!("Running '{}'", func_name);

    let mut params = serde_json::Map::new();
    let mut output_path: Option<PathBuf> = None;

    let mut args_iter = args.iter().peekable();
    while let Some(arg) = args_iter.next() {
        if arg.starts_with("--") {
            if let Some(value) = args_iter.peek() {
                let arg_name = &arg[2..arg.len()];
                if let Some(arg_info) = fn_args
                    .get(arg_name)
                    .and_then(|arg_info| arg_info.as_object())
                {
                    let mut value_to_use = serde_json::from_str::<Value>(value)
                        // .inspect_err(|e| println!("Could not parse as JSON: {}", e))
                        .unwrap_or_else(|_| value.to_string().into());
                    if let Some(arg_refs) = arg_info
                        .get("x-registry-ref")
                        .and_then(|arg_ref| arg_ref.as_str())
                    {
                        let stored_name = format!("A{arg_name}");
                        match arg_refs {
                            "IndexLinkedOCEL" => {
                                let path = value;
                                let ocel = import_ocel_json_from_path(path).unwrap();
                                let locel = IndexLinkedOCEL::from_ocel(ocel);
                                state.add(
                                    &stored_name,
                                    bindings::RegistryItem::IndexLinkedOCEL(locel),
                                );
                            }
                            "EventLogActivityProjection" => {
                                let path = value;
                                println!("Path: {path}");
                                let xes =
                                    import_xes_file(path, XESImportOptions::default()).unwrap();
                                state.add(
                                    &stored_name,
                                    RegistryItem::EventLogActivityProjection((&xes).into()),
                                )
                            }
                            "EventLog" => {
                                let path = value;
                                println!("Path: {path}");
                                let xes =
                                    import_xes_file(path, XESImportOptions::default()).unwrap();
                                state.add(&stored_name, RegistryItem::EventLog(xes))
                            }
                            _ => todo!(),
                        }
                        value_to_use = stored_name.into();
                    }
                    params.insert(arg_name.to_string(), value_to_use);
                }
                // Skip next element (as it is the value!)
                args_iter.next();
            }
        } else {
            // Might be output path?
            if args_iter.peek().is_none() {
                // If not starting with -- and is last argument: assume output path!
                output_path = Some(PathBuf::from(arg));
            }
        }
    }

    let fn_args = serde_json::Value::Object(params);
    match bindings::call(func_name, &fn_args, &state) {
        Ok(res) => {
            if let Some(output_path) = output_path {
                let writer = BufWriter::new(File::create(&output_path).unwrap());
                // Right now we just write to JSON, but of course here we could also support other formats :)
                serde_json::to_writer(writer, &res).unwrap();
                println!("Wrote output to {:?}", output_path);
            } else {
                // If not output path is specified, print result
                println!("\n\nOutput:\n{:#}", res);
            }
        }
        Err(e) => eprintln!("Error: {}", e),
    }
}
