use std::env;
use std::fs;
use std::path::{Path, PathBuf};

fn copy_generated_file(out_dir: &Path, marker: &str, target: &str) {
    let mut matched = None;

    for entry in fs::read_dir(out_dir).unwrap() {
        let path = entry.unwrap().path();
        if !path.is_file() {
            continue;
        }
        if path.extension().and_then(|ext| ext.to_str()) != Some("rs") {
            continue;
        }
        let contents = fs::read_to_string(&path).unwrap();
        if contents.contains(marker) {
            matched = Some(path);
            break;
        }
    }

    let source = matched.expect(&format!(
        "did not find generated proto source containing marker {marker}"
    ));
    let output = out_dir.join(target);
    fs::copy(&source, &output).unwrap();
}

fn main() {
    let manifest_dir = PathBuf::from(env::var("CARGO_MANIFEST_DIR").unwrap());
    let proto_dir = manifest_dir.join("proto");
    let out_dir = PathBuf::from(env::var("OUT_DIR").unwrap());

    let proto_files = vec![
        proto_dir.join("common/v1/common.proto"),
        proto_dir.join("resource/v1/resource.proto"),
        proto_dir.join("metrics/v1/metrics.proto"),
        proto_dir.join("collector/metrics/v1/metrics_service.proto"),
        proto_dir.join("logs/v1/logs.proto"),
        proto_dir.join("trace/v1/trace.proto"),
        proto_dir.join("collector/logs/v1/logs_service.proto"),
        proto_dir.join("collector/trace/v1/trace_service.proto"),
        proto_dir.join("profiles/v1development/profiles.proto"),
        proto_dir.join("collector/profiles/v1development/profiles_service.proto"),
    ];

    for proto_file in &proto_files {
        println!("cargo:rerun-if-changed={}", proto_file.display());
    }

    let proto_paths: Vec<_> = proto_files
        .iter()
        .map(|path| path.as_path())
        .collect();

    prost_build::Config::new()
        .compile_protos(&proto_paths, &[proto_dir.as_ref()])
        .unwrap();

    copy_generated_file(
        &out_dir,
        "pub struct AnyValue",
        "opentelemetry.proto.common.v1.rs",
    );
    copy_generated_file(
        &out_dir,
        "pub struct Resource",
        "opentelemetry.proto.resource.v1.rs",
    );
    copy_generated_file(
        &out_dir,
        "pub struct MetricsData",
        "opentelemetry.proto.metrics.v1.rs",
    );
    copy_generated_file(
        &out_dir,
        "pub struct ExportMetricsServiceRequest",
        "opentelemetry.proto.collector.metrics.v1.rs",
    );
}
