fn main() -> std::io::Result<()> {
    // TODO: Audit that the environment access only happens in single-threaded code.
    unsafe { std::env::set_var("PROTOC", protobuf_src::protoc()) };

    tonic_prost_build::configure()
        .build_server(false)
        .compile_protos(&["proto/jet-topology.proto"], &["proto"])
}
