use std::env;

fn main() {
    capnpc::CompilerCommand::new()
        .src_prefix("capnp")
        .file("capnp/control.capnp")
        .run()
        .expect("capnpc");

    let protoc = protoc_bin_vendored::protoc_bin_path().expect("vendored protoc");
    env::set_var("PROTOC", protoc);
    prost_build::compile_protos(&["proto/event.proto"], &["proto/"])
        .expect("prost-build");
}
