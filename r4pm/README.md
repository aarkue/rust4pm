# r4pm - Rust for Process Mining CLI

`r4pm` is a command-line interface for the [`process_mining`](../process_mining/README.MD) library, allowing you to access various process mining algorithms and utilities directly from your terminal.

Currently, the r4pm CLI is in beta mode, meaning that the exposed command and API surface is not stable and might change, also in minor releases.

## Installation

You can build r4pm from source:

```bash
git clone https://github.com/aarkue/rust4pm.git
cd rust4pm
cargo build --release -p r4pm
./target/release/r4pm --help
```

## Usage

The general syntax for `r4pm` is:

```bash
r4pm <function_name> [arguments] [output_path]
```

- `<function_name>`: The name of the function to execute (e.g., `num_events`, `discover_dfg`).
- `[arguments]`: Arguments required by the function, passed as `--arg_name value`.
- `[output_path]`: (Optional) Path to write the result to. If omitted, the result is printed to stdout.

To see a list of available functions, run `r4pm` without any arguments.
To see help for a specific function (including its arguments), run `r4pm <function_name>`.

## Examples

### 1. Count events in an OCEL file

```bash
r4pm num_events --ocel process_mining/test_data/ocel/order-management.json
```

### 2. Discover a Directly-Follows Graph (DFG)

Discover a DFG from an XES event log and save the result to `dfg.json`:

```bash
r4pm discover_dfg --event_log process_mining/test_data/xes/small-example.xes dfg.json
```

### 3. Export a DFG to SVG

Take the `dfg.json` from the previous step and export it as an SVG image:

```bash
r4pm export_dfg_image_svg --dfg dfg.json --path dfg.svg
```

## Argument Resolution

`r4pm` is smart about resolving arguments:
- **Simple values** (numbers, strings, booleans) are parsed directly.
- **Complex objects** (like `EventLog` or `OCEL`) can be loaded from files by passing the file path.
- **JSON objects** can be loaded from a `.json` file by passing the file path.
