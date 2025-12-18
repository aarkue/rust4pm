use std::{collections::HashSet, path::PathBuf, process::ExitCode, sync::LazyLock};

use anstyle::AnsiColor;
pub use process_mining::bindings;
use process_mining::bindings::Binding;

static SPACE: &str = "  ";
static CLI_NAME: &str = "r4pm";

static PRIMARY: LazyLock<anstyle::Style> = LazyLock::new(|| {
    anstyle::Style::new()
        .bold()
        .underline()
        .fg_color(Some(AnsiColor::BrightBlue.into()))
});
static MUTED: LazyLock<anstyle::Style> = LazyLock::new(|| {
    anstyle::Style::new()
        // .bold()
        .fg_color(Some(AnsiColor::BrightBlack.into()))
});
static WARN: LazyLock<anstyle::Style> = LazyLock::new(|| {
    anstyle::Style::new()
        .bold()
        .fg_color(Some(AnsiColor::BrightRed.into()))
});

static INFO: LazyLock<anstyle::Style> = LazyLock::new(|| {
    anstyle::Style::new()
        // .bold()
        .fg_color(Some(AnsiColor::BrightGreen.into()))
});

static BOLD: LazyLock<anstyle::Style> = LazyLock::new(|| anstyle::Style::new().bold());

fn primary(s: impl std::fmt::Display) -> String {
    let sty = &*PRIMARY;
    format!("{sty}{s}{sty:#}")
}
fn muted(s: impl std::fmt::Display) -> String {
    let sty = &*MUTED;
    format!("{sty}{s}{sty:#}")
}
fn warn(s: impl std::fmt::Display) -> String {
    let sty = &*WARN;
    format!("{sty}{s}{sty:#}")
}
fn info(s: impl std::fmt::Display) -> String {
    let sty = &*INFO;
    format!("{sty}{s}{sty:#}")
}
fn bold(s: impl std::fmt::Display) -> String {
    let sty = &*BOLD;
    format!("{sty}{s}{sty:#}")
}

fn main() -> ExitCode {
    let functions = bindings::list_functions();
    let args: Vec<String> = std::env::args().collect();
    if args.len() <= 1 {
        println!(
            "{}\nAvailable functions: {}",
            warn(format!("Usage: {CLI_NAME} fun_name --arg1 'abc' --arg2 4")),
            functions
                .iter()
                .map(|f| f.name)
                .collect::<Vec<_>>()
                .join(", "),
        );
        return ExitCode::FAILURE;
    }
    let mut state = bindings::AppState::default();

    let func_name = &args[1];
    let binding = *functions
        .iter()
        .find(|f| f.name == func_name)
        .expect("Unknown function name!");
    let required_fn_args: HashSet<String> = ((binding.required_args)()).into_iter().collect();
    print_function_info(binding, &required_fn_args);
    let fn_args = (binding.args)();

    let mut params = serde_json::Map::new();
    let mut output_path: Option<PathBuf> = None;

    let mut args_iter = args.iter().skip(2).peekable();
    while let Some(arg) = args_iter.next() {
        if arg.starts_with("--") {
            if let Some(value_str) = args_iter.peek() {
                let arg_name = &arg[2..arg.len()];
                if let Some((_, schema)) = fn_args.iter().find(|(an, _)| an == arg_name) {
                    // Initial value is just the string from CLI
                    let initial_value = serde_json::Value::String(value_str.to_string());

                    // Resolve the argument using the bindings helper
                    match bindings::resolve_argument(arg_name, initial_value, schema, &mut state) {
                        Ok(resolved_value) => {
                            params.insert(arg_name.to_string(), resolved_value);
                        }
                        Err(e) => {
                            eprintln!(
                                "{}",
                                warn(format!("Error resolving argument '{}': {}", arg_name, e))
                            );
                            return ExitCode::FAILURE;
                        }
                    }
                }
                // Skip next element (as it is the value!)
                args_iter.next();
            }
        } else {
            // Might be output path?
            if args_iter.peek().is_none() {
                // If not starting with -- and is last argument: assume output path!
                output_path = Some(PathBuf::from(arg));
            } else {
                // Unknown argument?!
                eprintln!("{}", warn(format!("Unknown argument: {:?}", arg)));
                return ExitCode::FAILURE;
            }
        }
    }
    // Check if all parameters are there
    let missing_args: Vec<_> = required_fn_args
        .into_iter()
        .filter(|k| !params.contains_key(k))
        .collect();
    if !missing_args.is_empty() {
        eprintln!(
            "{}",
            warn(format!(
                "Missing required arguments: {}",
                missing_args.join(", ")
            ))
        );
        return ExitCode::FAILURE;
    }
    let fn_args = serde_json::Value::Object(params);
    match bindings::call(binding, &fn_args, &state) {
        Ok(res) => {
            if let Some(output_path) = output_path {
                if let Some(id) = res.as_str() {
                    let state_guard = state.items.read().unwrap();
                    if let Some(item) = state_guard.get(id) {
                        match item.export_to_path(&output_path, None) {
                            Ok(_) => {
                                println!("Exported registry item '{}' to {:?}", id, output_path);
                            }
                            Err(e) => {
                                eprintln!(
                                    "{}",
                                    warn(format!("Failed to export registry item: {}", e))
                                );
                                return ExitCode::FAILURE;
                            }
                        }
                    } else {
                        // Not a registry item, just write the JSON
                        let file = std::fs::File::create(output_path).unwrap();
                        serde_json::to_writer_pretty(file, &res).unwrap();
                    }
                } else {
                    // Not a string (so not a registry ID), just write the JSON
                    let file = std::fs::File::create(output_path).unwrap();
                    serde_json::to_writer_pretty(file, &res).unwrap();
                }
            } else {
                // No output path, print to stdout
                let mut final_res = res.clone();
                if let Some(id) = res.as_str() {
                    let state_guard = state.items.read().unwrap();
                    if let Some(item) = state_guard.get(id)
                        && let Ok(val) = item.to_value() {
                            final_res = val;
                        }
                }
                println!("{}", serde_json::to_string_pretty(&final_res).unwrap());
            }
        }
        Err(e) => {
            eprintln!("{}", warn(format!("Error calling function: {}", e)));
            return ExitCode::FAILURE;
        }
    }
    ExitCode::SUCCESS
}

fn print_function_info(binding: &Binding, required_fn_args: &HashSet<String>) {
    let name = binding.name;

    let docs = (binding.docs)()
        .into_iter()
        .map(|s| format!("{SPACE}{s}"))
        .collect::<Vec<_>>()
        .join("\n");

    let args: Vec<_> = ((binding.args)())
        .iter()
        .map(|(s, v)| {
            let type_name = v
                .as_object()
                .unwrap()
                .get("title")
                .unwrap()
                .as_str()
                .unwrap();
            if required_fn_args.contains(s.as_str()) {
                format!("{s}: {}", type_name)
            } else {
                format!("[{s}: {}]", type_name)
            }
        })
        .collect();
    let arg_hints = format!("{SPACE}{}: {}", bold("Arguments"), args.join(", "));
    let ret_hint = format!(
        "{SPACE}{}: {}",
        bold("Returns"),
        (binding.return_type)()
            .as_object()
            .unwrap()
            .get("title")
            .unwrap()
            .as_str()
            .unwrap()
    );
    let source_hints = format!(
        "{SPACE}Source: {}:{}\n{SPACE}Module: {}",
        binding.source_path, binding.source_line, binding.module
    );

    println!(
        "\n{}\n{}\n{}\n{}\n{}\n",
        primary(name),
        info(docs),
        arg_hints,
        ret_hint,
        muted(source_hints)
    );
}
