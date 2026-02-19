fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("cargo:rerun-if-changed=proto/ffq_distributed.proto");

    if std::env::var_os("CARGO_FEATURE_GRPC").is_none() {
        return Ok(());
    }

    let protoc = protoc_bin_vendored::protoc_bin_path()?;
    tonic_build::configure().compile_protos_with_config(
        {
            let mut cfg = prost_build::Config::new();
            cfg.protoc_executable(protoc);
            cfg
        },
        &["proto/ffq_distributed.proto"],
        &["proto"],
    )?;
    Ok(())
}
