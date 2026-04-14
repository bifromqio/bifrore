mod common;

use bifrore_embed_core::message::Message;
use criterion::{black_box, criterion_group, criterion_main, Criterion};

use common::{
    build_engine, build_engine_with_expr, build_protobuf_engine, build_protobuf_message,
};

fn bench_e2e(c: &mut Criterion) {
    let mut single_rule_engine = build_engine(1);
    let single_rule_messages: Vec<Message> = (0..100)
        .map(|idx| {
            let payload = serde_json::json!({
                "temp": 30 + (idx % 3),
                "hum": 40 + (idx % 5),
            });
            Message::new("sensors/room1/temp", serde_json::to_vec(&payload).unwrap())
        })
        .collect();

    c.bench_function("rule_eval_100_messages_1_rule_json", |b| {
        b.iter(|| {
            let mut total = 0usize;
            for message in &single_rule_messages {
                total += single_rule_engine.evaluate(black_box(message)).len();
            }
            assert_eq!(total, 100);
        })
    });

    let mut all_match_engine = build_engine(100);
    let all_match_payload = serde_json::json!({"temp": 30, "hum": 40});
    let all_match_message =
        Message::new("sensors/room1/temp", serde_json::to_vec(&all_match_payload).unwrap());

    c.bench_function("rule_eval_100_all_match_json", |b| {
        b.iter(|| {
            let results = all_match_engine.evaluate(&all_match_message);
            assert_eq!(results.len(), 100);
        })
    });

    let where_miss_payload = serde_json::json!({"temp": 10, "hum": 90});
    let where_miss_message =
        Message::new("sensors/room1/temp", serde_json::to_vec(&where_miss_payload).unwrap());

    c.bench_function("rule_eval_100_where_miss_json", |b| {
        b.iter(|| {
            let results = all_match_engine.evaluate(&where_miss_message);
            assert_eq!(results.len(), 0);
        })
    });

    let topic_miss_message =
        Message::new("sensors/room1/hum", serde_json::to_vec(&all_match_payload).unwrap());

    c.bench_function("rule_eval_100_topic_miss_json", |b| {
        b.iter(|| {
            let results = all_match_engine.evaluate(&topic_miss_message);
            assert_eq!(results.len(), 0);
        })
    });

    let mut half_match_engine = build_engine_with_expr(100, |idx| {
        let threshold = if idx % 2 == 0 { 20 } else { 40 };
        format!(
            "select (temp + {idx}) * 2 as t from sensors/+/temp where temp > {threshold} and hum < 80"
        )
    });
    let half_match_payload = serde_json::json!({"temp": 30, "hum": 40});
    let half_match_message =
        Message::new("sensors/room1/temp", serde_json::to_vec(&half_match_payload).unwrap());

    c.bench_function("rule_eval_100_half_match_json", |b| {
        b.iter(|| {
            let results = half_match_engine.evaluate(&half_match_message);
            assert_eq!(results.len(), 50);
        })
    });

    let mut metadata_engine = build_engine_with_expr(100, |idx| {
        format!(
            "select temp as t{idx} from sensors/+/temp where qos >= 1 and retain = false and topic_level(dev, 2) = 'room1'"
        )
    });
    let mut metadata_message =
        Message::new("sensors/room1/temp", serde_json::to_vec(&all_match_payload).unwrap());
    metadata_message.qos = 1;
    metadata_message.retain = false;

    c.bench_function("rule_eval_100_metadata_topic_json", |b| {
        b.iter(|| {
            let results = metadata_engine.evaluate(&metadata_message);
            assert_eq!(results.len(), 100);
        })
    });

    let mut protobuf_engine = build_protobuf_engine(100);
    let protobuf_message = build_protobuf_message("sensors/room1/temp", 30.0, 40.0);

    c.bench_function("rule_eval_100_all_match_protobuf", |b| {
        b.iter(|| {
            let results = protobuf_engine.evaluate(&protobuf_message);
            assert_eq!(results.len(), 100);
        })
    });
}

criterion_group!(benches, bench_e2e);
criterion_main!(benches);
