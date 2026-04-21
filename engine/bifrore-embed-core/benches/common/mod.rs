#![allow(dead_code)]

use bifrore_embed_core::message::Message;
use bifrore_embed_core::rule::RuleDefinition;
use bifrore_embed_core::runtime::RuleEngine;
use prost::Message as _;
use serde_json::Value as JsonValue;

#[derive(Clone, PartialEq, prost::Message)]
pub struct EvalPayload {
    #[prost(double, tag = "1")]
    pub temp: f64,
    #[prost(double, tag = "2")]
    pub hum: f64,
}

#[derive(Clone, PartialEq, prost::Message)]
pub struct TypedMetric {
    #[prost(double, tag = "1")]
    pub value: f64,
    #[prost(bool, tag = "2")]
    pub ok: bool,
    #[prost(string, tag = "3")]
    pub label: String,
}

#[derive(Clone, PartialEq, prost::Message)]
pub struct TypedDeepNode {
    #[prost(message, optional, boxed, tag = "1")]
    pub child: Option<Box<TypedDeepNode>>,
    #[prost(double, tag = "2")]
    pub value: f64,
    #[prost(string, tag = "3")]
    pub label: String,
}

#[derive(Clone, PartialEq, prost::Message)]
pub struct TypedPayload {
    #[prost(double, tag = "1")]
    pub temp: f64,
    #[prost(double, tag = "2")]
    pub hum: f64,
    #[prost(message, optional, tag = "3")]
    pub deep: Option<TypedDeepNode>,
    #[prost(message, repeated, tag = "4")]
    pub metrics: Vec<TypedMetric>,
    #[prost(string, tag = "5")]
    pub device: String,
}

pub fn build_engine(rule_count: usize) -> RuleEngine {
    let mut engine = RuleEngine::default();
    for idx in 0..rule_count {
        let expression = format!(
            "select (temp + {idx}) * 2 as t from sensors/+/temp where temp > 20 and hum < 80"
        );
        engine
            .add_rule(RuleDefinition {
                expression,
                destinations: vec!["dest".to_string()],
                schema: None,
            })
            .expect("add rule");
    }
    engine
}

pub fn build_engine_with_expr(
    rule_count: usize,
    expr_builder: impl Fn(usize) -> String,
) -> RuleEngine {
    let mut engine = RuleEngine::default();
    for idx in 0..rule_count {
        engine
            .add_rule(RuleDefinition {
                expression: expr_builder(idx),
                destinations: vec!["dest".to_string()],
                schema: None,
            })
            .expect("add rule");
    }
    engine
}

pub fn build_protobuf_engine(rule_count: usize) -> RuleEngine {
    let decoder = bifrore_embed_core::payload::dynamic_protobuf_decoder_from_descriptor_set_bytes(
        include_bytes!("../../testdata/bifrore_test.desc"),
        "bifrore.test.EvalPayload",
    )
    .expect("protobuf decoder");
    let mut engine = RuleEngine::new(decoder);
    for idx in 0..rule_count {
        let expression = format!(
            "select (temp + {idx}) * 2 as t from sensors/+/temp where temp > 20 and hum < 80"
        );
        engine
            .add_rule(RuleDefinition {
                expression,
                destinations: vec!["dest".to_string()],
                schema: Some(String::from("bifrore.test.EvalPayload"))
            })
            .expect("add rule");
    }
    engine
}

pub fn build_protobuf_message(topic: &str, temp: f64, hum: f64) -> Message {
    let payload = EvalPayload { temp, hum };
    Message::new(topic, payload.encode_to_vec())
}

pub fn build_normal_json_payload() -> JsonValue {
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

pub fn build_deep_json_payload(depth: usize) -> JsonValue {
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

pub fn build_large_json_payload(width: usize) -> JsonValue {
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

pub fn build_typed_deep_node(depth: usize) -> TypedDeepNode {
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

pub fn build_typed_payload(depth: usize, width: usize) -> TypedPayload {
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

pub fn parse_json_bytes(payload: &[u8]) -> bool {
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
        serde_json::from_slice::<JsonValue>(payload)
            .ok()
            .and_then(|value| value.as_object().map(|_| true))
            .unwrap_or(false)
    }
}

pub fn parse_json_object(payload: &[u8]) -> serde_json::Map<String, JsonValue> {
    serde_json::from_slice::<JsonValue>(payload)
        .expect("parse json payload")
        .as_object()
        .cloned()
        .expect("json object payload")
}

pub fn parse_protobuf_typed_bytes(payload: &[u8]) -> bool {
    TypedPayload::decode(payload)
        .ok()
        .map(|_| true)
        .unwrap_or(false)
}
