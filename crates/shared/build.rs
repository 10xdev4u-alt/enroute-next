//! Generate the protocol's types from `proto/` with protox (a protobuf
//! compiler in Rust): no `protoc` needed at build time.

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("cargo:rerun-if-changed=proto");
    let fds = protox::compile(["state.proto", "ledger.proto"], ["proto"])?;
    tonic_prost_build::configure()
        // Byte fields share their buffers when cloned (inline layers).
        .bytes(".")
        // Refs in name order, as git lists them.
        .btree_map(".")
        // Object ids decode straight into our type.
        .extern_path(".enroute.internal.v1.Oid", "crate::oid::Oid")
        .compile_fds(fds)?;
    Ok(())
}
