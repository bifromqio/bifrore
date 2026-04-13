mod common;

use bifrore_embed_core::message::Message;
use bifrore_embed_core::msg_ir::{CompiledPayloadField, MsgIr};
use bifrore_embed_core::payload::{
    decode_payload_ir_with_decoder_and_plan_and_metrics, PayloadDecodePlan, PayloadDecoder,
};
use criterion::{black_box, criterion_group, criterion_main, Criterion};

use common::{build_engine_with_expr, parse_json_object};

fn bench_pipeline(c: &mut Criterion) {
    let decode_fields = [
        CompiledPayloadField::from_key("temp"),
        CompiledPayloadField::from_key("hum"),
    ];
    let decode_field_refs = [&decode_fields[0], &decode_fields[1]];

    let mut engine = build_engine_with_expr(2, |idx| match idx {
        0 => "select temp + hum as score from sensors/room1/temp where temp > 20 and hum < 80"
            .to_string(),
        _ => "select temp as t from sensors/room2/temp where temp > 50".to_string(),
    });
    let payload = serde_json::json!({"temp": 30, "hum": 40});
    let payload_bytes = serde_json::to_vec(&payload).unwrap();
    let message = Message::new("sensors/room1/temp", payload_bytes.clone());
    let parsed_object = parse_json_object(&payload_bytes);
    let msg_ir = MsgIr::from_json_object_with_decode_plan(
        &parsed_object,
        PayloadDecodePlan::Sparse(&decode_field_refs),
    )
    .expect("build sparse msg ir");

    // Prewarm the topic cache so this bench reflects the cache-hot read-heavy path.
    let matched_rule_indexes = engine.match_rule_indexes_for_bench(&message.topic);
    assert_eq!(matched_rule_indexes.len(), 1);

    c.bench_function("pipeline_overall_2_rules_1_match_json", |b| {
        b.iter(|| {
            let results = engine.evaluate(black_box(&message));
            assert_eq!(results.len(), 1);
        })
    });

    c.bench_function("pipeline_topic_match_hot_2_rules_1_match_json", |b| {
        b.iter(|| {
            let matched = engine.match_rule_indexes_for_bench(black_box(&message.topic));
            assert_eq!(matched.len(), 1);
        })
    });

    c.bench_function("pipeline_payload_decode_json_object", |b| {
        b.iter(|| {
            let object = parse_json_object(black_box(&payload_bytes));
            black_box(object);
        })
    });

    c.bench_function("pipeline_msg_ir_build_sparse_temp_hum_json", |b| {
        b.iter(|| {
            let built = MsgIr::from_json_object_with_decode_plan(
                black_box(&parsed_object),
                PayloadDecodePlan::Sparse(&decode_field_refs),
            )
            .expect("build sparse msg ir");
            black_box(built);
        })
    });

    c.bench_function("pipeline_rule_exec_2_rules_1_match_predecoded_json", |b| {
        b.iter(|| {
            let results = engine.evaluate_matched_rules_with_decoded_payload(
                black_box(&message),
                black_box(&msg_ir),
                black_box(&matched_rule_indexes),
            );
            assert_eq!(results.len(), 1);
        })
    });

    c.bench_function("pipeline_json_decode_to_msg_ir_sparse_temp_hum", |b| {
        b.iter(|| {
            let ir = decode_payload_ir_with_decoder_and_plan_and_metrics(
                black_box(&payload_bytes),
                &PayloadDecoder::Json,
                PayloadDecodePlan::Sparse(&decode_field_refs),
                None,
            )
            .expect("decode sparse json");
            black_box(ir);
        })
    });
}

criterion_group!(benches, bench_pipeline);
criterion_main!(benches);
