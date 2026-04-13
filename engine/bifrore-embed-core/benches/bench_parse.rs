mod common;

use criterion::{black_box, criterion_group, criterion_main, Criterion};
use prost::Message as _;

use common::{
    build_deep_json_payload, build_large_json_payload, build_normal_json_payload,
    build_typed_payload, parse_json_bytes, parse_protobuf_typed_bytes,
};

fn bench_parse(c: &mut Criterion) {
    let normal_json = serde_json::to_vec(&build_normal_json_payload()).expect("normal json");
    let deep_json = serde_json::to_vec(&build_deep_json_payload(28)).expect("deep json");
    let large_json = serde_json::to_vec(&build_large_json_payload(512)).expect("large json");
    let normal_pb = build_typed_payload(2, 16).encode_to_vec();
    let deep_pb = build_typed_payload(28, 16).encode_to_vec();
    let large_pb = build_typed_payload(4, 512).encode_to_vec();

    c.bench_function("parse_only_normal_json", |b| {
        b.iter(|| {
            let ok = parse_json_bytes(black_box(&normal_json));
            assert!(ok);
        })
    });
    c.bench_function("parse_only_normal_protobuf", |b| {
        b.iter(|| {
            let ok = parse_protobuf_typed_bytes(black_box(&normal_pb));
            assert!(ok);
        })
    });

    c.bench_function("parse_only_deep_json", |b| {
        b.iter(|| {
            let ok = parse_json_bytes(black_box(&deep_json));
            assert!(ok);
        })
    });
    c.bench_function("parse_only_deep_protobuf", |b| {
        b.iter(|| {
            let ok = parse_protobuf_typed_bytes(black_box(&deep_pb));
            assert!(ok);
        })
    });

    c.bench_function("parse_only_large_json", |b| {
        b.iter(|| {
            let ok = parse_json_bytes(black_box(&large_json));
            assert!(ok);
        })
    });
    c.bench_function("parse_only_large_protobuf", |b| {
        b.iter(|| {
            let ok = parse_protobuf_typed_bytes(black_box(&large_pb));
            assert!(ok);
        })
    });
}

criterion_group!(benches, bench_parse);
criterion_main!(benches);
