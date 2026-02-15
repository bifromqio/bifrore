use crate::message::Message;
use crate::metrics::EvalMetrics;
use crate::payload::{decode_payload_object, PayloadFormat};
use crate::rule::{
    compile_rule, evaluate_rule_with_payload_and_topic_parts, CompiledRule, RuleDefinition,
    RuleError,
};
use serde::Deserialize;
use std::fs;
use std::collections::{HashMap, HashSet};
use std::path::Path;
use std::time::Instant;

#[derive(Debug, Default)]
pub struct RuleEngine {
    rules: Vec<Option<CompiledRule>>,
    rule_index_by_id: HashMap<String, usize>,
    matcher: TopicTrie,
    metrics: EvalMetrics,
    payload_format: PayloadFormat,
}

impl RuleEngine {
    pub fn new() -> Self {
        Self::with_payload_format(PayloadFormat::Json)
    }

    pub fn with_payload_format(payload_format: PayloadFormat) -> Self {
        Self {
            rules: Vec::new(),
            rule_index_by_id: HashMap::new(),
            matcher: TopicTrie::default(),
            metrics: EvalMetrics::default(),
            payload_format,
        }
    }

    pub fn metrics(&self) -> &EvalMetrics {
        &self.metrics
    }

    pub fn add_rule(&mut self, rule: RuleDefinition) -> Result<String, RuleError> {
        let compiled = compile_rule(rule)?;
        let id = compiled.id.clone();
        let index = self.rules.len();
        self.rules.push(Some(compiled));
        self.rule_index_by_id.insert(id.clone(), index);
        if let Some(rule_ref) = self.rules[index].as_ref() {
            self.matcher.insert(&rule_ref.topic_filter, index);
        }
        Ok(id)
    }

    pub fn remove_rule(&mut self, rule_id: &str) -> bool {
        let Some(index) = self.rule_index_by_id.remove(rule_id) else {
            return false;
        };
        if let Some(rule) = self.rules.get(index).and_then(|slot| slot.as_ref()) {
            self.matcher.remove(&rule.topic_filter, index);
        }
        if let Some(slot) = self.rules.get_mut(index) {
            *slot = None;
        }
        true
    }

    pub fn evaluate(&self, message: &Message) -> Vec<RuleEvaluation> {
        let mut results = Vec::new();
        let matched_rules = self.matcher.match_topic(&message.topic, &self.rules);
        if matched_rules.is_empty() {
            return results;
        }

        let payload_obj = match decode_payload_object(&message.payload, self.payload_format) {
            Ok(value) => value,
            Err(err) => {
                log::warn!(
                    "dropping message with invalid payload topic={} format={:?} error={}",
                    message.topic,
                    self.payload_format,
                    err
                );
                return results;
            }
        };
        let topic_parts: Vec<&str> = message.topic.split('/').collect();

        for rule in matched_rules {
            let start = Instant::now();
            let evaluated = evaluate_rule_with_payload_and_topic_parts(
                rule,
                message,
                &payload_obj,
                &topic_parts,
            );
            let duration = start.elapsed().as_nanos() as u64;
            let success = evaluated.is_some();
            self.metrics.record(duration, success);
            if let Some(evaluated_message) = evaluated {
                results.push(RuleEvaluation {
                    message: evaluated_message,
                    destinations: rule.destinations.clone(),
                    rule_id: rule.id.clone(),
                });
            }
        }
        results
    }

    pub fn topic_filters(&self) -> Vec<String> {
        self.rules
            .iter()
            .filter_map(|rule| rule.as_ref().map(|r| r.topic_filter.clone()))
            .collect()
    }

    pub fn load_rules_from_json<P: AsRef<Path>>(&mut self, path: P) -> Result<usize, RuleError> {
        let content = fs::read_to_string(path).map_err(|e| RuleError::SqlParse(e.to_string()))?;
        let parsed: RuleFile = serde_json::from_str(&content)
            .map_err(|e| RuleError::SqlParse(e.to_string()))?;
        let mut count = 0;
        for rule in parsed.rules {
            self.add_rule(rule)?;
            count += 1;
        }
        Ok(count)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RuleEvaluation {
    pub rule_id: String,
    pub message: Message,
    pub destinations: Vec<String>,
}

#[derive(Debug, Deserialize)]
struct RuleFile {
    rules: Vec<RuleDefinition>,
}

#[derive(Debug, Default)]
struct TopicTrie {
    root: TrieNode,
}

#[derive(Debug, Default)]
struct TrieNode {
    children: HashMap<String, TrieNode>,
    plus_child: Option<Box<TrieNode>>,
    hash_child: Option<Box<TrieNode>>,
    rule_indexes: HashSet<usize>,
}

impl TrieNode {
    fn is_empty(&self) -> bool {
        self.children.is_empty()
            && self.plus_child.is_none()
            && self.hash_child.is_none()
            && self.rule_indexes.is_empty()
    }
}

impl TopicTrie {

    fn insert(&mut self, filter: &str, rule_index: usize) {
        let levels: Vec<&str> = filter.split('/').collect();
        let mut node = &mut self.root;
        for level in levels {
            match level {
                "+" => {
                    node = node.plus_child.get_or_insert_with(|| Box::new(TrieNode::default()));
                }
                "#" => {
                    node = node.hash_child.get_or_insert_with(|| Box::new(TrieNode::default()));
                    break;
                }
                _ => {
                    node = node.children.entry(level.to_string()).or_default();
                }
            }
        }
        node.rule_indexes.insert(rule_index);
    }

    fn remove(&mut self, filter: &str, rule_index: usize) {
        let levels: Vec<&str> = filter.split('/').collect();
        Self::remove_from_node(&mut self.root, &levels, 0, rule_index);
    }

    fn match_topic<'a>(
        &'a self,
        topic: &str,
        rules: &'a [Option<CompiledRule>],
    ) -> Vec<&'a CompiledRule> {
        let levels: Vec<&str> = topic.split('/').collect();
        let mut matched = HashSet::new();
        self.match_node(&self.root, &levels, 0, rules, &mut matched);
        matched
            .into_iter()
            .filter_map(|idx| rules.get(idx).and_then(|slot| slot.as_ref()))
            .collect()
    }

    fn match_node<'a>(
        &'a self,
        node: &'a TrieNode,
        levels: &[&str],
        index: usize,
        rules: &'a [Option<CompiledRule>],
        out: &mut HashSet<usize>,
    ) {
        if let Some(hash_node) = &node.hash_child {
            out.extend(hash_node.rule_indexes.iter().copied());
        }

        if index == levels.len() {
            out.extend(node.rule_indexes.iter().copied());
            return;
        }

        let level = levels[index];
        if let Some(child) = node.children.get(level) {
            self.match_node(child, levels, index + 1, rules, out);
        }
        if let Some(plus_child) = &node.plus_child {
            self.match_node(plus_child, levels, index + 1, rules, out);
        }
    }

    fn remove_from_node(
        node: &mut TrieNode,
        levels: &[&str],
        index: usize,
        rule_index: usize,
    ) -> bool {
        if index == levels.len() {
            node.rule_indexes.remove(&rule_index);
            return node.is_empty();
        }

        let level = levels[index];
        let child_empty = match level {
            "+" => node
                .plus_child
                .as_mut()
                .map(|child| Self::remove_from_node(child, levels, index + 1, rule_index))
                .unwrap_or(false),
            "#" => node
                .hash_child
                .as_mut()
                .map(|child| Self::remove_from_node(child, levels, index + 1, rule_index))
                .unwrap_or(false),
            _ => node
                .children
                .get_mut(level)
                .map(|child| Self::remove_from_node(child, levels, index + 1, rule_index))
                .unwrap_or(false),
        };

        if child_empty {
            match level {
                "+" => {
                    node.plus_child = None;
                }
                "#" => {
                    node.hash_child = None;
                }
                _ => {
                    node.children.remove(level);
                }
            }
        }

        node.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::payload::PayloadFormat;
    use prost::Message as _;
    use prost_types::{Struct, Value as ProstValue};

    #[test]
    fn load_rules_from_json() {
        let json = r#"{
            "rules": [
                { "expression": "select * from data", "destinations": ["dest1"] }
            ]
        }"#;
        let temp = tempfile::NamedTempFile::new().expect("tempfile");
        fs::write(temp.path(), json).expect("write");

        let mut engine = RuleEngine::new();
        let count = engine.load_rules_from_json(temp.path()).expect("load rules");
        assert_eq!(count, 1);
        assert_eq!(engine.rules.len(), 1);
    }

    #[test]
    fn evaluate_records_metrics() {
        let mut engine = RuleEngine::new();
        engine
            .add_rule(RuleDefinition {
                expression: "select * from data".to_string(),
                destinations: vec!["dest".to_string()],
            })
            .expect("add rule");

        let payload = serde_json::json!({"temp": 10});
        let message = Message::new("data", serde_json::to_vec(&payload).unwrap());
        let results = engine.evaluate(&message);
        assert_eq!(results.len(), 1);

        let snapshot = engine.metrics().snapshot();
        assert_eq!(snapshot.eval_count, 1);
        assert_eq!(snapshot.eval_error_count, 0);
        assert!(snapshot.eval_total_nanos > 0);
    }

    #[test]
    fn trie_matches_wildcards() {
        let mut engine = RuleEngine::new();
        engine
            .add_rule(RuleDefinition {
                expression: "select * from sensors/+/temp".to_string(),
                destinations: vec!["dest".to_string()],
            })
            .expect("add rule");
        engine
            .add_rule(RuleDefinition {
                expression: "select * from sensors/#".to_string(),
                destinations: vec!["dest".to_string()],
            })
            .expect("add rule");

        let payload = serde_json::json!({"temp": 10});
        let message = Message::new("sensors/room1/temp", serde_json::to_vec(&payload).unwrap());
        let results = engine.evaluate(&message);
        assert_eq!(results.len(), 2);
    }

    #[test]
    fn trie_removes_rules_incrementally() {
        let mut engine = RuleEngine::new();
        let rule_id = engine
            .add_rule(RuleDefinition {
                expression: "select * from sensors/+/temp".to_string(),
                destinations: vec!["dest".to_string()],
            })
            .expect("add rule");

        let payload = serde_json::json!({"temp": 10});
        let message = Message::new("sensors/room1/temp", serde_json::to_vec(&payload).unwrap());
        let results = engine.evaluate(&message);
        assert_eq!(results.len(), 1);

        assert!(engine.remove_rule(&rule_id));
        let results = engine.evaluate(&message);
        assert_eq!(results.len(), 0);
    }

    #[test]
    fn evaluate_with_protobuf_struct_payload() {
        let mut engine = RuleEngine::with_payload_format(PayloadFormat::Protobuf);
        engine
            .add_rule(RuleDefinition {
                expression: "select * from data where temp > 20".to_string(),
                destinations: vec!["dest".to_string()],
            })
            .expect("add rule");

        let mut fields = std::collections::BTreeMap::new();
        fields.insert(
            "temp".to_string(),
            ProstValue {
                kind: Some(prost_types::value::Kind::NumberValue(25.0)),
            },
        );
        let payload = Struct { fields };
        let message = Message::new("data", payload.encode_to_vec());
        let results = engine.evaluate(&message);
        assert_eq!(results.len(), 1);
    }

    #[test]
    fn evaluate_with_protobuf_typed_predicates_and_projection() {
        let mut engine = RuleEngine::with_payload_format(PayloadFormat::Protobuf);
        engine
            .add_rule(RuleDefinition {
                expression: "select device as d from data where temp >= 20 and online = true"
                    .to_string(),
                destinations: vec!["dest".to_string()],
            })
            .expect("add rule");

        let mut fields = std::collections::BTreeMap::new();
        fields.insert(
            "temp".to_string(),
            ProstValue {
                kind: Some(prost_types::value::Kind::NumberValue(21.0)),
            },
        );
        fields.insert(
            "online".to_string(),
            ProstValue {
                kind: Some(prost_types::value::Kind::BoolValue(true)),
            },
        );
        fields.insert(
            "device".to_string(),
            ProstValue {
                kind: Some(prost_types::value::Kind::StringValue("sensor-1".to_string())),
            },
        );
        let payload = Struct { fields };

        let message = Message::new("data", payload.encode_to_vec());
        let results = engine.evaluate(&message);
        assert_eq!(results.len(), 1);

        let output: serde_json::Value =
            serde_json::from_slice(&results[0].message.payload).expect("output json");
        assert_eq!(output["d"], serde_json::Value::from("sensor-1"));
    }

    #[test]
    fn evaluate_with_protobuf_invalid_payload_is_dropped() {
        let mut engine = RuleEngine::with_payload_format(PayloadFormat::Protobuf);
        engine
            .add_rule(RuleDefinition {
                expression: "select * from data where temp > 10".to_string(),
                destinations: vec!["dest".to_string()],
            })
            .expect("add rule");

        let message = Message::new("data", vec![1, 2, 3, 4, 5]);
        let results = engine.evaluate(&message);
        assert!(results.is_empty());
    }
}
