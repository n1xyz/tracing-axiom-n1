use std::env;
use std::path::PathBuf;

fn main() {
    let manifest_dir: PathBuf =
        PathBuf::from(env::var("CARGO_MANIFEST_DIR").unwrap());
    let proto_dir: PathBuf = manifest_dir.join("proto");

    let proto_files = vec![
        proto_dir.join("opentelemetry/proto/common/v1/common.proto"),
        proto_dir.join("opentelemetry/proto/resource/v1/resource.proto"),
        proto_dir.join("opentelemetry/proto/metrics/v1/metrics.proto"),
        proto_dir.join("opentelemetry/proto/collector/metrics/v1/metrics_service.proto"),
        proto_dir.join("opentelemetry/proto/logs/v1/logs.proto"),
        proto_dir.join("opentelemetry/proto/trace/v1/trace.proto"),
        proto_dir.join("opentelemetry/proto/collector/logs/v1/logs_service.proto"),
        proto_dir.join("opentelemetry/proto/collector/trace/v1/trace_service.proto"),
        proto_dir.join("opentelemetry/proto/profiles/v1development/profiles.proto"),
        proto_dir.join("opentelemetry/proto/collector/profiles/v1development/profiles_service.proto"),
    ];

    for proto_file in &proto_files {
        println!("cargo:rerun-if-changed={}", proto_file.display());
    }

    let proto_paths: Vec<_> =
        proto_files.iter().map(|path| path.as_path()).collect();

    prost_build::Config::new()
        .compile_protos(&proto_paths, &[proto_dir.as_path()])
        .unwrap();
}
