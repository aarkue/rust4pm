# Rust Bridge for Process Mining

This repository is a mono-repo containing the following parts:
- `process_mining/` Process Mining library in Rust (See also https://crates.io/crates/process_mining)
- `python_bridge/` Python Bindings
- `java_bridge/` Java Bindings
- `pm_wasm/` WASM / JavaScript Bindings PoC
- `binary/` Rust Binary Executable

The Java, Python and JavaScript bindings make use of the main `process_mining` library, which is written in Rust.
Thus, complex algorithms or logic can be __implemented only once in Rust__. This implementation can then be exposed and used from Java, Python or JavaScript, in different enviroments.
This not only reduces implementation effort (_write only once, use everywhere_), but also enables great performance.

<img width="40%" src="https://github.com/aarkue/rust-bridge-process-mining/assets/20766652/fab66ce3-2547-4527-af2d-b5a25b3ca446" alt="Figure showcasing that one main Rust implementation can be used from Java, Python, or other languages."/>
  
Additional information, as well as evaluation results, are published in a written report titled _Developing a High-Performance Process Mining Library with Java and Python Bindings in Rust_.

Mostly, `cargo` is used to build and run this project. For more detailed instructions see https://github.com/aarkue/rust-bridge-template, which provides more details on how to build or run this and similar projects.

If you are looking for the Rust-based XES event log importer, see https://github.com/aarkue/rustxes, which contains the XES importer from `process_mining/` extracted to its own crate with full Python bindings.

If you are interested in a __starter kit template__ to bootstrap similar Rust libraries with Java and Python bindings, see https://github.com/aarkue/rust-bridge-template.

There you will find more information on how to get started, as well as build and run such projects.

## Main Rust Library (`process_mining`)
`process_mining` is a collection of functions, structs and utilitities related to Process Mining.

See [process_mining/README.md](./process_mining/README.md) to view the dedicated README for the `process_mining` Rust library.

Further information is also available at https://crates.io/crates/process_mining and https://docs.rs/process_mining.

## Bindings
Here you can find some preliminary information about the binding implementations.
For more details, please take a look at the source code or the full written report.

### Java
- Uses `jni` to allow Java code to call shared Rust library
- Object Passing:
  - `byte[]` JSON-encoded data [faster]
  - Writing `File` to disk (containing JSON-encoded data)
- Required Java libraries:
  - `gson` for efficient JSON encoding/decoding
  
### Python
- Uses [maturin](https://github.com/PyO3/maturin) with the corresponding [PyO3 FFI Bindings](https://github.com/PyO3/PyO3)
- Can pass dicts and other types rather easily; but slow for larger data
- JSON-encoding/decoding possible (using faster `orjson`), but still no great passing performance
  - `bytes` encoding/decoding e.g., Event Logs is implemented__
- __Polars__: Allows converting PM4Py's pandas DataFrame to a Polars DataFrame; Can then easily be used on Rust side
  - But: Requires Polars dependency (also in python!)


### WASM Proof of Concept
- `pm_wasm/` contains a proof of concept of WASM bindings for the main library, e.g., exposed to JavaScript and executed in the browser.
- After building with `wasm-pack build --target web --release` the `index.html` file can be served (at root): The resulting webpage contains a file input allowing an XES event log to be selected.
After selecting a file, clicking the 'Go' button will execute the generated WASM and discover a Petri net, which will be logged in the console as JSON once the call finishes. Note that the in-browser WASM execution performance is limited, so some patience might be required.  

- For easy usage of `wasm-bindgen-rayon` we use `vite`. The dev server can be started with `npm run dev`.


## LICENSE
This project is licensed under either Apache License Version 2.0 or MIT License at your option.
