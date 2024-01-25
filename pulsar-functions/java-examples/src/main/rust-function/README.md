# How to build the wasm file

1. install rustup

2. install rust

3. generate the wasm file

```shell
cd {pulsar}/pulsar-functions/java-examples/src/main/rust-function
cargo build --target wasm32-wasi --release
```

then you will see the wasm file
in `{pulsar}/pulsar-functions/java-examples/src/main/rust-function/target/wasm32-wasi/release/rust_filter.wasm`

4. rename the wasm file

rename the file to `org.apache.pulsar.functions.api.examples.WasmFunction.wasm`
