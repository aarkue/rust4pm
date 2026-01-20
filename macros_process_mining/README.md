# `macros_process_mining`

**Note: This crate is intended for internal use only.**

`macros_process_mining` provides the procedural macros required by the `rust4pm` project.

These macros (i.e., `register_binding`) automatically register an annotated function for dynamic introspection and execution, effectively creating dynamic function bindings.
This functionality, for example, powers the CLI.

Through the use of macros, new functions can be added to this dynamic function collection without introducing duplicate code or requiring much effort.
