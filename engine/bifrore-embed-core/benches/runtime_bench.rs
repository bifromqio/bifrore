use bifrore_embed_core::message::Message;
use bifrore_embed_core::runtime::RuleEngine;
use bifrore_embed_core::rule::RuleDefinition;
use criterion::{criterion_group, criterion_main, Criterion};

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

fn bench_evaluate(c: &mut Criterion) {
    let engine = build_engine(100);
    let payload = serde_json::json!({"temp": 30, "hum": 40});
    let message = Message::new("sensors/room1/temp", serde_json::to_vec(&payload).unwrap());

    c.bench_function("rule_eval_100", |b| {
        b.iter(|| {
            let results = engine.evaluate(&message);
            assert_eq!(results.len(), 100);
        })
    });
}

criterion_group!(benches, bench_evaluate);
criterion_main!(benches);
