fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("cargo:rerun-if-changed=proto/calvinfs.proto");
    let protoc = protoc_bin_vendored::protoc_bin_path()?;
    std::env::set_var("PROTOC", protoc);

    #[cfg(madsim)]
    {
        madsim_tonic_build::configure()
            .build_server(true)
            .build_client(true)
            .compile_protos(&["proto/calvinfs.proto"], &["proto"])?;
    }

    #[cfg(not(madsim))]
    {
        tonic_build::configure()
            .build_server(true)
            .build_client(true)
            .compile_protos(&["proto/calvinfs.proto"], &["proto"])?;
    }

    Ok(())
}
