use bifrore_embed_core::message::Message;
use bifrore_embed_core::rule::RuleDefinition;
use bifrore_embed_core::runtime::RuleEngine;
use criterion::{black_box, criterion_group, criterion_main, Criterion};

fn build_appheap_engine(detailed_metrics: bool) -> RuleEngine {
    let mut engine = RuleEngine::default();
    engine
        .add_rule(RuleDefinition {
            expression: "select hum + 10 from data where temp > 25".to_string(),
            destinations: vec!["log".to_string()],
            schema: None,
        })
        .expect("add AppHeap-style rule");
    engine.set_detailed_latency_metrics(detailed_metrics);
    engine
}

fn appheap_message() -> Message {
    Message::new("data", br#"{"temp":30,"hum":40}"#.to_vec())
}

fn bench_pipeline_single_rule(c: &mut Criterion) {
    let message = appheap_message();

    let mut engine = build_appheap_engine(false);
    assert_eq!(engine.evaluate(&message).len(), 1);

    c.bench_function("pipeline_single_rule_appheap_json", |b| {
        b.iter(|| {
            let results = engine.evaluate(black_box(&message));
            assert_eq!(results.len(), 1);
        })
    });

    let mut detailed_engine = build_appheap_engine(true);
    assert_eq!(detailed_engine.evaluate(&message).len(), 1);

    c.bench_function("pipeline_single_rule_appheap_json_detailed_metrics", |b| {
        b.iter(|| {
            let results = detailed_engine.evaluate(black_box(&message));
            assert_eq!(results.len(), 1);
        })
    });
}

criterion_group!(benches, bench_pipeline_single_rule);
criterion_main!(benches);
