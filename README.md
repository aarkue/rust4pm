<div align="center">
<h1>Rust4PM</h1>
  <p><strong><code>process_mining</code> Rust Crate</strong></p>
  <p>
    <a href="https://crates.io/crates/process_mining">
        <img src="https://img.shields.io/crates/v/process_mining.svg" alt="Crates.io"/></a>
    <a href="https://crates.io/crates/process_mining">
        <img src="https://img.shields.io/crates/d/process_mining" alt="Downloads"/></a>
	<a href="https://docs.rs/process_mining"">
        <img src="https://img.shields.io/badge/docs.rs-process_mining-blue" alt="Documentation"/></a>
  </p>
</div>


# `process_mining`

This crate contains basic data structures, functions and utilities for Process Mining.

Full documentation of the modules, structs and functions of this crate is available at **[docs.rs/process_mining/](https://docs.rs/process_mining/)**.

_As this crate is still in very active development, expect larger API changes also in minor (or even patch) version updates._


## Contributing

### Test Data

The data (OCEL2, XES, etc. files) used for the tests of this crate are available for download at <https://rwth-aachen.sciebo.de/s/4cvtTU3lLOgtxt1>.
Simply download this zip and extract it into the `test_data` folder.

### Linting and Formatting

We use automatic CI pipelines for checking lint and formatting rules of the `process_mining` crate.
See the corresponding .yml file for the exact checks.
You can and should test your changes also locally, e.g., using `cargo clippy --all-targets --all-features -- -D warnings`, `cargo fmt --all --check`
and ` cargo test --verbose --all-features` inside the `process_mining` folder.
To test integrity of the documentation, use `RUSTDOCFLAGS="-D warnings" cargo doc --all-features --no-deps` (on Windows `PowerShell` you might need to set the `RUSTDOCFLAGS` env variable differently).

To apply compatible lint and formatting rules automatically, run `cargo clippy --all-targets --all-features --fix --allow-staged` and `cargo fmt --all` in the `process_mining` folder.

## LICENSE
This project is licensed under either Apache License Version 2.0 or MIT License at your option.
