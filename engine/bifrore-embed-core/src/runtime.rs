use crate::message::Message;
use crate::metrics::EvalMetrics;
use crate::payload::{decode_payload_object_with_decoder, PayloadDecoder, PayloadFormat};
use crate::rule::{
    compile_rule, evaluate_rule_with_payload_and_topic_parts, CompiledRule, RuleDefinition,
    RuleError,
};
use rayon::prelude::*;
use serde::Deserialize;
use std::collections::{HashMap, HashSet};
use std::fs;
use std::path::Path;
use std::time::Instant;

const DEFAULT_TOPIC_CACHE_CAPACITY: usize = 4096;
const DEFAULT_EVAL_PARALLEL_THRESHOLD: usize = 64;
const DEFAULT_EVAL_PARALLEL_MAX_WORKERS: usize = 8;

#[derive(Debug)]
pub struct RuleEngine {
    rules: Vec<Option<CompiledRule>>,
    rule_index_by_id: HashMap<String, usize>,
    matcher: TopicTrie,
    metrics: EvalMetrics,
    payload_decoder: PayloadDecoder,
    topic_match_cache: TopicMatchCache,
    eval_parallel_threshold: usize,
    eval_pool: Option<rayon::ThreadPool>,
}

impl Default for RuleEngine {
    fn default() -> Self {
        Self::new()
    }
}

impl RuleEngine {
    pub fn new() -> Self {
        Self::with_payload_format(PayloadFormat::Json)
    }

    pub fn with_payload_format(payload_format: PayloadFormat) -> Self {
        Self::with_payload_decoder(PayloadDecoder::from_format(payload_format))
    }

    pub fn with_payload_decoder(payload_decoder: PayloadDecoder) -> Self {
        Self::with_payload_decoder_and_cache_capacity(payload_decoder, DEFAULT_TOPIC_CACHE_CAPACITY)
    }

    pub fn with_payload_decoder_and_cache_capacity(
        payload_decoder: PayloadDecoder,
        topic_cache_capacity: usize,
    ) -> Self {
        Self {
            rules: Vec::new(),
            rule_index_by_id: HashMap::new(),
            matcher: TopicTrie::default(),
            metrics: EvalMetrics::default(),
            payload_decoder,
            topic_match_cache: TopicMatchCache::new(topic_cache_capacity),
            eval_parallel_threshold: DEFAULT_EVAL_PARALLEL_THRESHOLD,
            eval_pool: build_eval_pool(),
        }
    }

    pub fn metrics(&self) -> &EvalMetrics {
        &self.metrics
    }

    pub fn set_eval_parallel_threshold(&mut self, threshold: Option<usize>) {
        self.eval_parallel_threshold = threshold.unwrap_or(DEFAULT_EVAL_PARALLEL_THRESHOLD).max(1);
    }

    pub fn eval_parallel_threshold(&self) -> usize {
        self.eval_parallel_threshold
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
        self.clear_topic_cache();
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
        self.clear_topic_cache();
        true
    }

    pub fn evaluate(&mut self, message: &Message) -> Vec<RuleEvaluation> {
        let mut results = Vec::new();
        let matched_rule_indexes = self.match_rule_indexes_with_cache(&message.topic);
        if matched_rule_indexes.is_empty() {
            return results;
        }

        let payload_obj =
            match decode_payload_object_with_decoder(&message.payload, &self.payload_decoder) {
                Ok(value) => value,
                Err(err) => {
                    log::warn!(
                        "dropping message with invalid payload topic={} decoder={:?} error={}",
                        message.topic,
                        self.payload_decoder,
                        err
                    );
                    return results;
                }
            };
        let topic_parts: Vec<&str> = message.topic.split('/').collect();

        let use_parallel = matched_rule_indexes.len() >= self.eval_parallel_threshold
            && self
                .eval_pool
                .as_ref()
                .map(|pool| pool.current_num_threads() > 1)
                .unwrap_or(false);
        let attempts = if use_parallel {
            let rules = &self.rules;
            self.eval_pool
                .as_ref()
                .expect("eval pool checked")
                .install(|| {
                    matched_rule_indexes
                        .par_iter()
                        .filter_map(|rule_index| {
                            evaluate_single_rule(
                                *rule_index,
                                rules,
                                message,
                                &payload_obj,
                                &topic_parts,
                            )
                        })
                        .collect::<Vec<_>>()
                })
        } else {
            matched_rule_indexes
                .iter()
                .filter_map(|rule_index| {
                    evaluate_single_rule(
                        *rule_index,
                        &self.rules,
                        message,
                        &payload_obj,
                        &topic_parts,
                    )
                })
                .collect::<Vec<_>>()
        };
        for attempt in attempts {
            self.metrics.record(attempt.duration_nanos, attempt.success);
            if let Some(evaluation) = attempt.evaluation {
                log::info!("evaluation={:?}", evaluation);
                results.push(evaluation);
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
        let parsed: RuleFile =
            serde_json::from_str(&content).map_err(|e| RuleError::SqlParse(e.to_string()))?;
        let mut count = 0;
        for rule in parsed.rules {
            self.add_rule(rule)?;
            count += 1;
        }
        Ok(count)
    }

    fn clear_topic_cache(&mut self) {
        self.topic_match_cache.clear();
    }

    fn match_rule_indexes_with_cache(&mut self, topic: &str) -> Vec<usize> {
        if let Some(cached) = self.topic_match_cache.get(topic) {
            return cached;
        }

        let matched = self.matcher.match_topic_indexes(topic);
        self.topic_match_cache
            .insert(topic.to_string(), matched.clone());
        matched
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

struct EvalAttempt {
    duration_nanos: u64,
    success: bool,
    evaluation: Option<RuleEvaluation>,
}

fn evaluate_single_rule(
    rule_index: usize,
    rules: &[Option<CompiledRule>],
    message: &Message,
    payload_obj: &serde_json::Map<String, serde_json::Value>,
    topic_parts: &[&str],
) -> Option<EvalAttempt> {
    let rule = rules.get(rule_index).and_then(|slot| slot.as_ref())?;
    let start = Instant::now();
    let evaluated = evaluate_rule_with_payload_and_topic_parts(rule, message, payload_obj, topic_parts);
    let duration_nanos = start.elapsed().as_nanos() as u64;
    let success = evaluated.is_some();
    let evaluation = evaluated.map(|evaluated_message| RuleEvaluation {
        message: evaluated_message,
        destinations: rule.destinations.clone(),
        rule_id: rule.id.clone(),
    });
    Some(EvalAttempt {
        duration_nanos,
        success,
        evaluation,
    })
}

fn build_eval_pool() -> Option<rayon::ThreadPool> {
    let workers = std::thread::available_parallelism()
        .map(|count| count.get().clamp(1, DEFAULT_EVAL_PARALLEL_MAX_WORKERS))
        .unwrap_or(1);
    rayon::ThreadPoolBuilder::new()
        .num_threads(workers)
        .thread_name(|idx| format!("bifrore-eval-{idx}"))
        .build()
        .ok()
}

#[derive(Debug)]
struct TopicMatchCache {
    capacity: usize,
    access_seq: u64,
    entries: HashMap<String, TopicMatchCacheEntry>,
}

#[derive(Debug, Clone)]
struct TopicMatchCacheEntry {
    matched_rule_indexes: Vec<usize>,
    use_count: u64,
    last_access_seq: u64,
}

impl TopicMatchCache {
    fn new(capacity: usize) -> Self {
        Self {
            capacity,
            access_seq: 0,
            entries: HashMap::new(),
        }
    }

    fn clear(&mut self) {
        self.entries.clear();
        self.access_seq = 0;
    }

    fn get(&mut self, topic: &str) -> Option<Vec<usize>> {
        self.access_seq = self.access_seq.saturating_add(1);
        let entry = self.entries.get_mut(topic)?;
        entry.use_count = entry.use_count.saturating_add(1);
        entry.last_access_seq = self.access_seq;
        Some(entry.matched_rule_indexes.clone())
    }

    fn insert(&mut self, topic: String, matched_rule_indexes: Vec<usize>) {
        if self.capacity == 0 {
            return;
        }

        self.access_seq = self.access_seq.saturating_add(1);
        if let Some(entry) = self.entries.get_mut(&topic) {
            entry.matched_rule_indexes = matched_rule_indexes;
            entry.use_count = entry.use_count.saturating_add(1);
            entry.last_access_seq = self.access_seq;
            return;
        }

        if self.entries.len() >= self.capacity {
            self.evict_lfu();
        }

        self.entries.insert(
            topic,
            TopicMatchCacheEntry {
                matched_rule_indexes,
                use_count: 1,
                last_access_seq: self.access_seq,
            },
        );
    }

    fn evict_lfu(&mut self) {
        let Some(evict_key) = self
            .entries
            .iter()
            .min_by_key(|(_, entry)| (entry.use_count, entry.last_access_seq))
            .map(|(key, _)| key.clone()) else {
            return;
        };
        self.entries.remove(&evict_key);
    }
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

    fn match_topic_indexes(&self, topic: &str) -> Vec<usize> {
        let levels: Vec<&str> = topic.split('/').collect();
        let mut matched = HashSet::new();
        self.match_node(&self.root, &levels, 0, &mut matched);
        matched.into_iter().collect()
    }

    fn match_node(&self, node: &TrieNode, levels: &[&str], index: usize, out: &mut HashSet<usize>) {
        if let Some(hash_node) = &node.hash_child {
            out.extend(hash_node.rule_indexes.iter().copied());
        }

        if index == levels.len() {
            out.extend(node.rule_indexes.iter().copied());
            return;
        }

        let level = levels[index];
        if let Some(child) = node.children.get(level) {
            self.match_node(child, levels, index + 1, out);
        }
        if let Some(plus_child) = &node.plus_child {
            self.match_node(plus_child, levels, index + 1, out);
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
    use crate::payload::typed_protobuf_decoder;
    use prost::Message as _;

    #[derive(Clone, PartialEq, ::prost::Message)]
    struct TypedRuntimePayload {
        #[prost(double, tag = "1")]
        temp: f64,
        #[prost(string, tag = "2")]
        device: String,
        #[prost(bool, tag = "3")]
        online: bool,
    }

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
    fn evaluate_with_typed_protobuf_typed_predicates_and_projection() {
        let decoder = typed_protobuf_decoder::<TypedRuntimePayload, _>(|message| {
            let mut output = serde_json::Map::new();
            output.insert("temp".to_string(), serde_json::Value::from(message.temp));
            output.insert("device".to_string(), serde_json::Value::from(message.device));
            output.insert("online".to_string(), serde_json::Value::from(message.online));
            Ok(output)
        });
        let mut engine = RuleEngine::with_payload_decoder(decoder);
        engine
            .add_rule(RuleDefinition {
                expression: "select device as d from data where temp >= 20 and online = true"
                    .to_string(),
                destinations: vec!["dest".to_string()],
            })
            .expect("add rule");

        let payload = TypedRuntimePayload {
            temp: 21.0,
            device: "sensor-1".to_string(),
            online: true,
        };
        let message = Message::new("data", payload.encode_to_vec());
        let results = engine.evaluate(&message);
        assert_eq!(results.len(), 1);

        let output: serde_json::Value =
            serde_json::from_slice(&results[0].message.payload).expect("output json");
        assert_eq!(output["d"], serde_json::Value::from("sensor-1"));
    }

    #[test]
    fn evaluate_with_typed_protobuf_invalid_payload_is_dropped() {
        let decoder = typed_protobuf_decoder::<TypedRuntimePayload, _>(|message| {
            let mut output = serde_json::Map::new();
            output.insert("temp".to_string(), serde_json::Value::from(message.temp));
            output.insert("device".to_string(), serde_json::Value::from(message.device));
            output.insert("online".to_string(), serde_json::Value::from(message.online));
            Ok(output)
        });
        let mut engine = RuleEngine::with_payload_decoder(decoder);
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

    #[test]
    fn evaluate_with_typed_protobuf_decoder() {
        let decoder = typed_protobuf_decoder::<TypedRuntimePayload, _>(|message| {
            let mut output = serde_json::Map::new();
            output.insert("temp".to_string(), serde_json::Value::from(message.temp));
            output.insert("device".to_string(), serde_json::Value::from(message.device));
            Ok(output)
        });
        let mut engine = RuleEngine::with_payload_decoder(decoder);
        engine
            .add_rule(RuleDefinition {
                expression: "select device as d from data where temp > 20".to_string(),
                destinations: vec!["dest".to_string()],
            })
            .expect("add rule");

        let payload = TypedRuntimePayload {
            temp: 30.0,
            device: "typed-dev".to_string(),
            online: true,
        };
        let message = Message::new("data", payload.encode_to_vec());
        let results = engine.evaluate(&message);
        assert_eq!(results.len(), 1);

        let output: serde_json::Value =
            serde_json::from_slice(&results[0].message.payload).expect("output json");
        assert_eq!(output["d"], serde_json::Value::from("typed-dev"));
    }

    #[test]
    fn topic_match_cache_reuses_entries() {
        let mut engine = RuleEngine::with_payload_decoder_and_cache_capacity(PayloadDecoder::Json, 8);
        engine
            .add_rule(RuleDefinition {
                expression: "select * from sensors/+/temp".to_string(),
                destinations: vec!["dest".to_string()],
            })
            .expect("add rule");
        let payload = serde_json::json!({"temp": 10});
        let message = Message::new("sensors/room1/temp", serde_json::to_vec(&payload).unwrap());

        assert_eq!(engine.evaluate(&message).len(), 1);
        assert_eq!(engine.evaluate(&message).len(), 1);

        assert_eq!(engine.topic_match_cache.entries.len(), 1);
        let entry = engine
            .topic_match_cache
            .entries
            .get("sensors/room1/temp")
            .expect("cache entry");
        assert_eq!(entry.use_count, 2);
    }

    #[test]
    fn topic_match_cache_evicts_least_used_entry() {
        let mut engine = RuleEngine::with_payload_decoder_and_cache_capacity(PayloadDecoder::Json, 2);
        engine
            .add_rule(RuleDefinition {
                expression: "select * from sensors/#".to_string(),
                destinations: vec!["dest".to_string()],
            })
            .expect("add rule");
        let payload = serde_json::json!({"temp": 10});

        let message_a = Message::new("sensors/a/temp", serde_json::to_vec(&payload).unwrap());
        let message_b = Message::new("sensors/b/temp", serde_json::to_vec(&payload).unwrap());
        let message_c = Message::new("sensors/c/temp", serde_json::to_vec(&payload).unwrap());

        assert_eq!(engine.evaluate(&message_a).len(), 1);
        assert_eq!(engine.evaluate(&message_a).len(), 1);
        assert_eq!(engine.evaluate(&message_b).len(), 1);
        assert_eq!(engine.evaluate(&message_c).len(), 1);

        assert!(engine.topic_match_cache.entries.get("sensors/a/temp").is_some());
        assert!(engine.topic_match_cache.entries.get("sensors/c/temp").is_some());
        assert!(engine.topic_match_cache.entries.get("sensors/b/temp").is_none());
    }
}
