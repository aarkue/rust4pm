# process_mining_wasm

NPM package containing based on a WASM and the `process_mining` Rust crate.
See also https://crates.io/crates/process_mining and https://github.com/aarkue/rust4pm.

Note: This package was built for the web (`wasm-pack build --target web`) specifically and thus will likely not work in Node.JS environments.

## Development
### 🛠️ Build with `wasm-pack build`

```
wasm-pack build --scope aarkue --release --target web
```


### 🎁 Publish to NPM with `wasm-pack publish`

```
wasm-pack publish --access=public
```