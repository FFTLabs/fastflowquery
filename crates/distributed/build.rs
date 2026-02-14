fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("cargo:rerun-if-changed=proto/ffq_distributed.proto");

    if std::env::var_os("CARGO_FEATURE_GRPC").is_none() {
        return Ok(());
    }

    let protoc = protoc_bin_vendored::protoc_bin_path()?;
    std::env::set_var("PROTOC", protoc);

    tonic_build::configure().compile_protos(&["proto/ffq_distributed.proto"], &["proto"])?;
    Ok(())
}
