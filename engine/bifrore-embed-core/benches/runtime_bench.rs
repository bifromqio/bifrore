use bifrore_embed_core::message::Message;
use bifrore_embed_core::payload::PayloadFormat;
use bifrore_embed_core::runtime::RuleEngine;
use bifrore_embed_core::rule::RuleDefinition;
use criterion::{criterion_group, criterion_main, Criterion};
use prost::Message as _;
use prost_types::value::Kind;
use prost_types::{Struct, Value as ProstValue};

fn build_engine(rule_count: usize) -> RuleEngine {
    let mut engine = RuleEngine::new();
    for idx in 0..rule_count {
        let expression = format!(
            "select (temp + {idx}) * 2 as t from sensors/+/temp where temp > 20 and hum < 80"
        );
        engine
            .add_rule(RuleDefinition {
                expression,
                destinations: vec!["dest".to_string()],
            })
            .expect("add rule");
    }
    engine
}

fn build_engine_with_expr(rule_count: usize, expr_builder: impl Fn(usize) -> String) -> RuleEngine {
    let mut engine = RuleEngine::new();
    for idx in 0..rule_count {
        engine
            .add_rule(RuleDefinition {
                expression: expr_builder(idx),
                destinations: vec!["dest".to_string()],
            })
            .expect("add rule");
    }
    engine
}

fn build_protobuf_engine(rule_count: usize) -> RuleEngine {
    let mut engine = RuleEngine::with_payload_format(PayloadFormat::Protobuf);
    for idx in 0..rule_count {
        let expression = format!(
            "select (temp + {idx}) * 2 as t from sensors/+/temp where temp > 20 and hum < 80"
        );
        engine
            .add_rule(RuleDefinition {
                expression,
                destinations: vec!["dest".to_string()],
            })
            .expect("add rule");
    }
    engine
}

fn build_protobuf_message(topic: &str, temp: f64, hum: f64) -> Message {
    let mut fields = std::collections::BTreeMap::new();
    fields.insert(
        "temp".to_string(),
        ProstValue {
            kind: Some(Kind::NumberValue(temp)),
        },
    );
    fields.insert(
        "hum".to_string(),
        ProstValue {
            kind: Some(Kind::NumberValue(hum)),
        },
    );
    let payload = Struct { fields };
    Message::new(topic, payload.encode_to_vec())
}

fn bench_evaluate(c: &mut Criterion) {
    let all_match_engine = build_engine(100);
    let all_match_payload = serde_json::json!({"temp": 30, "hum": 40});
    let all_match_message = Message::new(
        "sensors/room1/temp",
        serde_json::to_vec(&all_match_payload).unwrap(),
    );

    c.bench_function("rule_eval_100_all_match_json", |b| {
        b.iter(|| {
            let results = all_match_engine.evaluate(&all_match_message);
            assert_eq!(results.len(), 100);
        })
    });

    let where_miss_payload = serde_json::json!({"temp": 10, "hum": 90});
    let where_miss_message = Message::new(
        "sensors/room1/temp",
        serde_json::to_vec(&where_miss_payload).unwrap(),
    );

    c.bench_function("rule_eval_100_where_miss_json", |b| {
        b.iter(|| {
            let results = all_match_engine.evaluate(&where_miss_message);
            assert_eq!(results.len(), 0);
        })
    });

    let topic_miss_message = Message::new(
        "sensors/room1/hum",
        serde_json::to_vec(&all_match_payload).unwrap(),
    );

    c.bench_function("rule_eval_100_topic_miss_json", |b| {
        b.iter(|| {
            let results = all_match_engine.evaluate(&topic_miss_message);
            assert_eq!(results.len(), 0);
        })
    });

    let half_match_engine = build_engine_with_expr(100, |idx| {
        let threshold = if idx % 2 == 0 { 20 } else { 40 };
        format!(
            "select (temp + {idx}) * 2 as t from sensors/+/temp where temp > {threshold} and hum < 80"
        )
    });
    let half_match_payload = serde_json::json!({"temp": 30, "hum": 40});
    let half_match_message = Message::new(
        "sensors/room1/temp",
        serde_json::to_vec(&half_match_payload).unwrap(),
    );

    c.bench_function("rule_eval_100_half_match_json", |b| {
        b.iter(|| {
            let results = half_match_engine.evaluate(&half_match_message);
            assert_eq!(results.len(), 50);
        })
    });

    let metadata_engine = build_engine_with_expr(100, |idx| {
        format!(
            "select temp as t{idx} from sensors/+/temp where qos >= 1 and retain = false and topic_level(dev, 2) = 'room1'"
        )
    });
    let mut metadata_message = Message::new(
        "sensors/room1/temp",
        serde_json::to_vec(&all_match_payload).unwrap(),
    );
    metadata_message.qos = 1;
    metadata_message.retain = false;

    c.bench_function("rule_eval_100_metadata_topic_json", |b| {
        b.iter(|| {
            let results = metadata_engine.evaluate(&metadata_message);
            assert_eq!(results.len(), 100);
        })
    });

    let protobuf_engine = build_protobuf_engine(100);
    let protobuf_message = build_protobuf_message("sensors/room1/temp", 30.0, 40.0);

    c.bench_function("rule_eval_100_all_match_protobuf", |b| {
        b.iter(|| {
            let results = protobuf_engine.evaluate(&protobuf_message);
            assert_eq!(results.len(), 100);
        })
    });
}

criterion_group!(benches, bench_evaluate);
criterion_main!(benches);
