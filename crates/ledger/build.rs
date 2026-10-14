//! Generate the ledger's own protocol types (its log records and the Raft
//! service between ledger nodes) from `proto/`, against enroute-shared's
//! protocol: its types are enroute-shared's, not generated again here.

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("cargo:rerun-if-changed=proto");
    println!("cargo:rerun-if-changed=../shared/proto");
    let fds = protox::compile(["raft.proto"], ["proto", "../shared/proto"])?;
    tonic_prost_build::configure()
        .bytes(".")
        // The enroute-shared type the Raft service carries.
        .extern_path(
            ".enroute.internal.v1.Record",
            "::enroute_shared::proto::Record",
        )
        .compile_fds(fds)?;
    Ok(())
}
