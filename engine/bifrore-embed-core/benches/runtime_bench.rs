use bifrore_embed_core::message::Message;
use bifrore_embed_core::payload::typed_protobuf_decoder;
use bifrore_embed_core::runtime::RuleEngine;
use bifrore_embed_core::rule::RuleDefinition;
use criterion::{black_box, criterion_group, criterion_main, Criterion};
use prost::Message as _;
use serde_json::Value as JsonValue;

#[derive(Clone, PartialEq, prost::Message)]
struct EvalPayload {
    #[prost(double, tag = "1")]
    temp: f64,
    #[prost(double, tag = "2")]
    hum: f64,
}

#[derive(Clone, PartialEq, prost::Message)]
struct TypedMetric {
    #[prost(double, tag = "1")]
    value: f64,
    #[prost(bool, tag = "2")]
    ok: bool,
    #[prost(string, tag = "3")]
    label: String,
}

#[derive(Clone, PartialEq, prost::Message)]
struct TypedDeepNode {
    #[prost(message, optional, boxed, tag = "1")]
    child: Option<Box<TypedDeepNode>>,
    #[prost(double, tag = "2")]
    value: f64,
    #[prost(string, tag = "3")]
    label: String,
}

#[derive(Clone, PartialEq, prost::Message)]
struct TypedPayload {
    #[prost(double, tag = "1")]
    temp: f64,
    #[prost(double, tag = "2")]
    hum: f64,
    #[prost(message, optional, tag = "3")]
    deep: Option<TypedDeepNode>,
    #[prost(message, repeated, tag = "4")]
    metrics: Vec<TypedMetric>,
    #[prost(string, tag = "5")]
    device: String,
}

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
    let decoder = typed_protobuf_decoder::<EvalPayload, _>(|message| {
        let mut output = serde_json::Map::new();
        output.insert("temp".to_string(), JsonValue::from(message.temp));
        output.insert("hum".to_string(), JsonValue::from(message.hum));
        Ok(output)
    });
    let mut engine = RuleEngine::with_payload_decoder(decoder);
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
    let payload = EvalPayload { temp, hum };
    Message::new(topic, payload.encode_to_vec())
}

fn build_normal_json_payload() -> JsonValue {
    serde_json::json!({
        "temp": 30,
        "hum": 40,
        "device": "sensor-1",
        "online": true,
        "meta": {
            "site": "lab-a",
            "floor": 3
        }
    })
}

fn build_deep_json_payload(depth: usize) -> JsonValue {
    let mut nested = serde_json::json!({
        "leaf_num": 42,
        "leaf_str": "ok",
        "leaf_bool": true
    });
    for level in (0..depth).rev() {
        nested = serde_json::json!({
            format!("level_{level}"): nested,
            "noise": level as i64
        });
    }
    serde_json::json!({
        "temp": 30,
        "hum": 40,
        "deep": nested
    })
}

fn build_large_json_payload(width: usize) -> JsonValue {
    let mut data = serde_json::Map::with_capacity(width + 3);
    data.insert("temp".to_string(), JsonValue::from(30));
    data.insert("hum".to_string(), JsonValue::from(40));
    data.insert(
        "tags".to_string(),
        JsonValue::Array((0..width).map(|idx| JsonValue::from(idx as i64)).collect()),
    );
    for idx in 0..width {
        data.insert(
            format!("metric_{idx}"),
            serde_json::json!({
                "v": (idx as f64) * 1.01,
                "ok": idx % 2 == 0,
                "label": format!("sensor_{idx}")
            }),
        );
    }
    JsonValue::Object(data)
}

fn build_typed_deep_node(depth: usize) -> TypedDeepNode {
    let mut node = TypedDeepNode {
        child: None,
        value: depth as f64,
        label: format!("level_{depth}"),
    };
    for level in (0..depth).rev() {
        node = TypedDeepNode {
            child: Some(Box::new(node)),
            value: level as f64,
            label: format!("level_{level}"),
        };
    }
    node
}

fn build_typed_payload(depth: usize, width: usize) -> TypedPayload {
    let metrics = (0..width)
        .map(|idx| TypedMetric {
            value: (idx as f64) * 1.01,
            ok: idx % 2 == 0,
            label: format!("sensor_{idx}"),
        })
        .collect();
    TypedPayload {
        temp: 30.0,
        hum: 40.0,
        deep: Some(build_typed_deep_node(depth)),
        metrics,
        device: "sensor-1".to_string(),
    }
}

fn parse_json_bytes(payload: &[u8]) -> bool {
    #[cfg(feature = "simd-json")]
    {
        let mut data = payload.to_vec();
        let parsed = simd_json::serde::from_slice::<JsonValue>(&mut data);
        return parsed
            .ok()
            .and_then(|value| value.as_object().map(|_| true))
            .unwrap_or(false);
    }
    #[cfg(not(feature = "simd-json"))]
    {
        return serde_json::from_slice::<JsonValue>(payload)
            .ok()
            .and_then(|value| value.as_object().map(|_| true))
            .unwrap_or(false);
    }
}

fn parse_protobuf_typed_bytes(payload: &[u8]) -> bool {
    TypedPayload::decode(payload)
        .ok()
        .map(|_| true)
        .unwrap_or(false)
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

fn bench_parse_only(c: &mut Criterion) {
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

criterion_group!(benches, bench_evaluate, bench_parse_only);
criterion_main!(benches);
