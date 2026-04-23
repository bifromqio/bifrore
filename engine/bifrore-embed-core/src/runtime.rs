use crate::message::Message;
use crate::metrics::{EvalMetrics, LatencyStage};
use crate::msg_ir::MsgIr;
#[cfg(test)]
use crate::payload::dynamic_protobuf_registry_from_descriptor_set_bytes;
use crate::payload::{
    decode_payload_ir_with_decoder_and_plan_and_metrics,
    decode_payload_ir_with_decoder_and_plan_and_metrics_and_schema, PayloadDecodePlan,
    PayloadDecoder, PayloadFormat,
};
use crate::rule::{
    compile_rule, evaluate_rule_with_payload_and_topic_parts, CompiledRule, EvalError,
    RuleDefinition, RuleError, RulePayloadBinding,
};
use rayon::prelude::*;
use serde::Deserialize;
use std::collections::{HashMap, HashSet};
use std::fs;
use std::path::Path;
use std::sync::Arc;

const DEFAULT_TOPIC_CACHE_CAPACITY: usize = 4096;
const DEFAULT_EVAL_PARALLEL_THRESHOLD: usize = 64;
const DEFAULT_EVAL_PARALLEL_MAX_WORKERS: usize = 8;

#[derive(Debug)]
pub struct RuleEngine {
    rules: Vec<Option<CompiledRule>>,
    rule_index_by_id: HashMap<String, usize>,
    matcher: TopicTrie,
    metrics: Arc<EvalMetrics>,
    payload_decoder: PayloadDecoder,
    topic_match_cache: TopicMatchCache,
    eval_parallel_threshold: usize,
    eval_pool: Option<rayon::ThreadPool>,
}

impl Default for RuleEngine {
    fn default() -> Self {
        Self::new(PayloadDecoder::from_format(PayloadFormat::Json))
    }
}

impl RuleEngine {
    pub fn new(payload_decoder: PayloadDecoder) -> Self {
        Self::new_with_cache_capacity(payload_decoder, DEFAULT_TOPIC_CACHE_CAPACITY)
    }

    fn new_with_cache_capacity(
        payload_decoder: PayloadDecoder,
        topic_cache_capacity: usize,
    ) -> Self {
        Self {
            rules: Vec::new(),
            rule_index_by_id: HashMap::new(),
            matcher: TopicTrie::default(),
            metrics: Arc::new(EvalMetrics::new(false)),
            payload_decoder,
            topic_match_cache: TopicMatchCache::new(topic_cache_capacity),
            eval_parallel_threshold: DEFAULT_EVAL_PARALLEL_THRESHOLD,
            eval_pool: build_eval_pool(),
        }
    }

    pub fn metrics(&self) -> &EvalMetrics {
        self.metrics.as_ref()
    }

    pub fn metrics_handle(&self) -> Arc<EvalMetrics> {
        Arc::clone(&self.metrics)
    }

    pub fn set_detailed_latency_metrics(&mut self, enabled: bool) {
        if self.metrics.detailed_latency_enabled() == enabled {
            return;
        }
        self.metrics = Arc::new(EvalMetrics::new(enabled));
    }

    pub fn set_eval_parallel_threshold(&mut self, threshold: Option<usize>) {
        self.eval_parallel_threshold = threshold.unwrap_or(DEFAULT_EVAL_PARALLEL_THRESHOLD).max(1);
    }

    pub fn eval_parallel_threshold(&self) -> usize {
        self.eval_parallel_threshold
    }

    pub fn add_rule(&mut self, rule: RuleDefinition) -> Result<String, RuleError> {
        match (&self.payload_decoder, rule.schema.as_ref()) {
            (PayloadDecoder::Json, Some(_)) => return Err(RuleError::UnexpectedSchemaName),
            (PayloadDecoder::Protobuf(_), None) => return Err(RuleError::MissingSchemaName),
            _ => {}
        }
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
        let topic_match_timer = self.metrics.start_stage();
        let matched = self.match_rule_indexes_with_cache(&message.topic);
        self.metrics
            .finish_stage(LatencyStage::TopicMatch, topic_match_timer);
        if matched.is_empty() {
            return Vec::new();
        }

        if matches!(self.payload_decoder, PayloadDecoder::Protobuf(_)) {
            return self.evaluate_protobuf_message(message, matched.protobuf_groups());
        }

        let matched_rule_indexes = matched.rule_indexes();
        let required_fields = collect_required_fields(&self.rules, &matched_rule_indexes);
        let decode_plan = if required_fields.is_empty() {
            PayloadDecodePlan::None
        } else {
            PayloadDecodePlan::Sparse(&required_fields)
        };
        let payload_obj = match decode_payload_ir_with_decoder_and_plan_and_metrics(
            &message.payload,
            &self.payload_decoder,
            decode_plan,
            Some(&self.metrics),
        ) {
            Ok(value) => value,
            Err(err) => {
                self.metrics.record_payload_error(err.kind());
                log::warn!(
                    "dropping message with invalid payload topic={} decoder={:?} error={}",
                    message.topic,
                    self.payload_decoder,
                    err
                );
                return Vec::new();
            }
        };
        self.evaluate_matched_rules_with_decoded_payload(
            message,
            &payload_obj,
            &matched_rule_indexes,
        )
    }

    fn evaluate_protobuf_message(
        &self,
        message: &Message,
        schema_groups: &[(String, Vec<usize>)],
    ) -> Vec<RuleEvaluation> {
        let mut results = Vec::new();
        for (schema_name, rule_indexes) in schema_groups {
            let required_fields = collect_required_fields(&self.rules, &rule_indexes);
            let decode_plan = if required_fields.is_empty() {
                PayloadDecodePlan::None
            } else {
                PayloadDecodePlan::Sparse(&required_fields)
            };
            let payload_obj = match decode_payload_ir_with_decoder_and_plan_and_metrics_and_schema(
                &message.payload,
                &self.payload_decoder,
                decode_plan,
                Some(&self.metrics),
                schema_name.as_str(),
            ) {
                Ok(value) => value,
                Err(err) => {
                    self.metrics.record_payload_error(err.kind());
                    log::warn!(
                        "dropping protobuf message with invalid payload topic={} schema={} error={}",
                        message.topic,
                        schema_name,
                        err
                    );
                    continue;
                }
            };
            results.extend(self.evaluate_matched_rules_with_decoded_payload(
                message,
                &payload_obj,
                &rule_indexes,
            ));
        }
        results
    }

    #[doc(hidden)]
    pub fn evaluate_with_decoded_payload(
        &mut self,
        message: &Message,
        payload_obj: &MsgIr,
    ) -> Vec<RuleEvaluation> {
        let topic_match_timer = self.metrics.start_stage();
        let matched_rule_indexes = self.match_rule_indexes_with_cache(&message.topic);
        self.metrics
            .finish_stage(LatencyStage::TopicMatch, topic_match_timer);
        if matched_rule_indexes.is_empty() {
            return Vec::new();
        }
        let matched_rule_indexes = matched_rule_indexes.rule_indexes();
        self.evaluate_matched_rules_with_decoded_payload(
            message,
            payload_obj,
            matched_rule_indexes,
        )
    }

    #[doc(hidden)]
    pub fn evaluate_matched_rules_with_decoded_payload(
        &self,
        message: &Message,
        payload_obj: &MsgIr,
        matched_rule_indexes: &[usize],
    ) -> Vec<RuleEvaluation> {
        let mut results = Vec::new();
        let topic_parts_storage =
            if matched_rules_require_topic_parts(&self.rules, &matched_rule_indexes) {
                Some(message.topic.split('/').collect::<Vec<_>>())
            } else {
                None
            };
        let topic_parts = topic_parts_storage.as_deref().unwrap_or(&[]);

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
                                topic_parts,
                                &self.metrics,
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
                        topic_parts,
                        &self.metrics,
                    )
                })
                .collect::<Vec<_>>()
        };
        for attempt in attempts {
            match attempt.error.as_ref() {
                Some(error) => self.metrics.record_eval_error(error.is_type_error()),
                None => self.metrics.record_eval_success(),
            }
            if let Some(evaluation) = attempt.evaluation {
                log::trace!("evaluation={:?}", evaluation);
                results.push(evaluation);
            }
        }
        results
    }

    #[doc(hidden)]
    pub fn match_rule_indexes_for_bench(&mut self, topic: &str) -> Vec<usize> {
        self.match_rule_indexes_with_cache(topic).into_rule_indexes()
    }

    pub fn topic_filters(&self) -> Vec<String> {
        self.rules
            .iter()
            .filter_map(|rule| rule.as_ref().map(|r| r.topic_filter.clone()))
            .collect()
    }

    pub fn rule_metadata(&self) -> Vec<RuleMetadata> {
        self.rules
            .iter()
            .enumerate()
            .filter_map(|(rule_index, rule)| {
                rule.as_ref().map(|rule| RuleMetadata {
                    rule_index,
                    destinations: rule.destinations.clone(),
                })
            })
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

    fn match_rule_indexes_with_cache(&mut self, topic: &str) -> CachedTopicMatch {
        if let Some(cached) = self.topic_match_cache.get(topic) {
            return cached;
        }

        let matched = self.matcher.match_topic_indexes(topic);
        let cached = build_cached_topic_match(&self.rules, matched);
        self.topic_match_cache.insert(topic.to_string(), cached.clone());
        cached
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RuleEvaluation {
    pub rule_index: usize,
    pub message: Message,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RuleMetadata {
    pub rule_index: usize,
    pub destinations: Vec<String>,
}

#[derive(Debug, Deserialize)]
struct RuleFile {
    rules: Vec<RuleDefinition>,
}

struct EvalAttempt {
    evaluation: Option<RuleEvaluation>,
    error: Option<EvalError>,
}

fn evaluate_single_rule(
    rule_index: usize,
    rules: &[Option<CompiledRule>],
    message: &Message,
    payload_obj: &MsgIr,
    topic_parts: &[&str],
    metrics: &EvalMetrics,
) -> Option<EvalAttempt> {
    let rule = rules.get(rule_index).and_then(|slot| slot.as_ref())?;
    let exec_timer = metrics.start_stage();
    let evaluated = evaluate_rule_with_payload_and_topic_parts(
        rule,
        message,
        payload_obj,
        topic_parts,
        metrics,
    );
    metrics.finish_stage(LatencyStage::Exec, exec_timer);
    Some(match evaluated {
        Ok(Some(evaluated_message)) => EvalAttempt {
            evaluation: Some(RuleEvaluation {
                rule_index,
                message: evaluated_message,
            }),
            error: None,
        },
        Ok(None) => EvalAttempt {
            evaluation: None,
            error: None,
        },
        Err(error) => {
            log::warn!(
                "dropping rule evaluation topic={} rule_id={} error={}",
                message.topic,
                rule.id,
                error
            );
            EvalAttempt {
                evaluation: None,
                error: Some(error),
            }
        }
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

fn collect_required_fields<'a>(
    rules: &'a [Option<CompiledRule>],
    matched_rule_indexes: &[usize],
) -> Vec<&'a crate::msg_ir::CompiledPayloadField> {
    let mut seen = HashSet::new();
    let mut fields = Vec::new();
    for rule_index in matched_rule_indexes {
        if let Some(rule) = rules.get(*rule_index).and_then(|slot| slot.as_ref()) {
            for field in &rule.required_payload_fields {
                if seen.insert(field.key()) {
                    fields.push(field);
                }
            }
        }
    }
    fields
}

fn matched_rules_require_topic_parts(
    rules: &[Option<CompiledRule>],
    matched_rule_indexes: &[usize],
) -> bool {
    matched_rule_indexes.iter().any(|rule_index| {
        rules
            .get(*rule_index)
            .and_then(|slot| slot.as_ref())
            .map(|rule| rule.requires_topic_parts)
            .unwrap_or(false)
    })
}

#[derive(Debug, Clone)]
enum CachedTopicMatch {
    Json(Vec<usize>),
    Protobuf(Vec<(String, Vec<usize>)>),
}

impl CachedTopicMatch {
    fn is_empty(&self) -> bool {
        match self {
            Self::Json(rule_indexes) => rule_indexes.is_empty(),
            Self::Protobuf(groups) => groups.is_empty(),
        }
    }

    fn rule_indexes(&self) -> &[usize] {
        match self {
            Self::Json(rule_indexes) => rule_indexes,
            Self::Protobuf(_) => &[],
        }
    }

    fn protobuf_groups(&self) -> &[(String, Vec<usize>)] {
        match self {
            Self::Json(_) => &[],
            Self::Protobuf(groups) => groups,
        }
    }

    fn into_rule_indexes(self) -> Vec<usize> {
        match self {
            Self::Json(rule_indexes) => rule_indexes,
            Self::Protobuf(groups) => groups
                .into_iter()
                .flat_map(|(_, rule_indexes)| rule_indexes)
                .collect(),
        }
    }
}

fn build_cached_topic_match(
    rules: &[Option<CompiledRule>],
    matched_rule_indexes: Vec<usize>,
) -> CachedTopicMatch {
    let mut protobuf_groups: HashMap<String, Vec<usize>> = HashMap::new();
    let mut json_rule_indexes = Vec::new();
    for rule_index in matched_rule_indexes {
        let Some(rule) = rules.get(rule_index).and_then(|slot| slot.as_ref()) else {
            continue;
        };
        match &rule.payload_binding {
            RulePayloadBinding::Json => json_rule_indexes.push(rule_index),
            RulePayloadBinding::Protobuf { schema } => {
                protobuf_groups.entry(schema.clone()).or_default().push(rule_index);
            }
        }
    }

    if protobuf_groups.is_empty() {
        CachedTopicMatch::Json(json_rule_indexes)
    } else {
        CachedTopicMatch::Protobuf(protobuf_groups.into_iter().collect())
    }
}

#[derive(Debug)]
struct TopicMatchCache {
    capacity: usize,
    random_state: u64,
    entries: HashMap<String, CachedTopicMatch>,
    keys: Vec<String>,
}

impl TopicMatchCache {
    fn new(capacity: usize) -> Self {
        Self {
            capacity,
            random_state: 0x9e37_79b9_7f4a_7c15,
            entries: HashMap::new(),
            keys: Vec::with_capacity(capacity.min(64)),
        }
    }

    fn clear(&mut self) {
        self.entries.clear();
        self.keys.clear();
        self.random_state = 0x9e37_79b9_7f4a_7c15;
    }

    fn get(&mut self, topic: &str) -> Option<CachedTopicMatch> {
        self.entries.get(topic).cloned()
    }

    fn insert(&mut self, topic: String, matched: CachedTopicMatch) {
        if self.capacity == 0 {
            return;
        }

        if let Some(entry) = self.entries.get_mut(&topic) {
            *entry = matched;
            return;
        }

        if self.entries.len() >= self.capacity {
            self.evict_random();
        }

        self.keys.push(topic.clone());
        self.entries.insert(topic, matched);
    }

    fn evict_random(&mut self) {
        if self.keys.is_empty() {
            return;
        }

        let index = (self.next_random_u64() as usize) % self.keys.len();
        let evict_key = self.keys.swap_remove(index);
        self.entries.remove(&evict_key);
    }

    fn next_random_u64(&mut self) -> u64 {
        let mut x = self.random_state;
        x ^= x >> 12;
        x ^= x << 25;
        x ^= x >> 27;
        self.random_state = x;
        x.wrapping_mul(0x2545_f491_4f6c_dd1d)
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
                    node = node
                        .plus_child
                        .get_or_insert_with(|| Box::new(TrieNode::default()));
                }
                "#" => {
                    node = node
                        .hash_child
                        .get_or_insert_with(|| Box::new(TrieNode::default()));
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

    #[derive(Clone, PartialEq, ::prost::Message)]
    struct EvalPayload {
        #[prost(double, tag = "1")]
        temp: f64,
        #[prost(double, tag = "2")]
        hum: f64,
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

        let mut engine = RuleEngine::default();
        let count = engine
            .load_rules_from_json(temp.path())
            .expect("load rules");
        assert_eq!(count, 1);
        assert_eq!(engine.rules.len(), 1);
    }

    #[test]
    fn evaluate_records_metrics() {
        let mut engine = RuleEngine::default();
        engine
            .add_rule(RuleDefinition {
                expression: "select * from data".to_string(),
                destinations: vec!["dest".to_string()],
                schema: None,
            })
            .expect("add rule");

        let payload = serde_json::json!({"temp": 10});
        let message = Message::new("data", serde_json::to_vec(&payload).unwrap());
        let results = engine.evaluate(&message);
        assert_eq!(results.len(), 1);

        let snapshot = engine.metrics().snapshot();
        assert_eq!(snapshot.eval_count, 1);
        assert_eq!(snapshot.eval_error_count, 0);
        assert_eq!(snapshot.topic_match.count, 0);

        engine.set_detailed_latency_metrics(true);
        let _ = engine.evaluate(&message);
        let snapshot = engine.metrics().snapshot();
        assert!(snapshot.topic_match.count > 0);
    }

    #[test]
    fn evaluate_type_mismatch_drops_message_and_records_type_error() {
        let mut engine = RuleEngine::default();
        engine
            .add_rule(RuleDefinition {
                expression: "select hum + 10 as h from data".to_string(),
                destinations: vec!["dest".to_string()],
                schema: None,
            })
            .expect("add rule");

        let payload = serde_json::json!({"hum": "10"});
        let message = Message::new("data", serde_json::to_vec(&payload).unwrap());
        let results = engine.evaluate(&message);
        assert!(results.is_empty());

        let snapshot = engine.metrics().snapshot();
        assert_eq!(snapshot.eval_count, 1);
        assert_eq!(snapshot.eval_error_count, 1);
        assert_eq!(snapshot.eval_type_error_count, 1);
    }

    #[test]
    fn evaluate_missing_field_drops_message_and_records_non_type_error() {
        let mut engine = RuleEngine::default();
        engine
            .add_rule(RuleDefinition {
                expression: "select hum from data".to_string(),
                destinations: vec!["dest".to_string()],
                schema: None,
            })
            .expect("add rule");

        let payload = serde_json::json!({"temp": 10});
        let message = Message::new("data", serde_json::to_vec(&payload).unwrap());
        let results = engine.evaluate(&message);
        assert!(results.is_empty());

        let snapshot = engine.metrics().snapshot();
        assert_eq!(snapshot.eval_count, 1);
        assert_eq!(snapshot.eval_error_count, 1);
        assert_eq!(snapshot.eval_type_error_count, 0);
    }

    #[test]
    fn evaluate_non_object_json_records_payload_build_error() {
        let mut engine = RuleEngine::default();
        engine
            .add_rule(RuleDefinition {
                expression: "select temp from data".to_string(),
                destinations: vec!["dest".to_string()],
                schema: None,
            })
            .expect("add rule");

        let message = Message::new("data", br#"42"#.to_vec());
        let results = engine.evaluate(&message);
        assert!(results.is_empty());

        let snapshot = engine.metrics().snapshot();
        assert_eq!(snapshot.payload_schema_error_count, 0);
        assert_eq!(snapshot.payload_decode_error_count, 0);
        assert_eq!(snapshot.payload_build_error_count, 1);
    }

    #[test]
    fn evaluate_invalid_protobuf_records_payload_decode_error() {
        let decoder =
            dynamic_protobuf_registry_from_descriptor_set_bytes(include_bytes!("../testdata/bifrore_test.desc"))
                .expect("protobuf registry");
        let mut engine = RuleEngine::new(decoder);
        engine
            .add_rule(RuleDefinition {
                expression: "select device from data".to_string(),
                destinations: vec!["dest".to_string()],
                schema: Some("bifrore.test.TypedRuntimePayload".to_string()),
            })
            .expect("add rule");

        let message = Message::new("data", vec![0x0A, 0x05, 0x41]);
        let results = engine.evaluate(&message);
        assert!(results.is_empty());

        let snapshot = engine.metrics().snapshot();
        assert_eq!(snapshot.payload_schema_error_count, 0);
        assert_eq!(snapshot.payload_decode_error_count, 1);
        assert_eq!(snapshot.payload_build_error_count, 0);
    }

    #[test]
    fn evaluate_unknown_protobuf_schema_records_payload_schema_error() {
        let decoder =
            dynamic_protobuf_registry_from_descriptor_set_bytes(include_bytes!("../testdata/bifrore_test.desc"))
                .expect("protobuf registry");
        let mut engine = RuleEngine::new(decoder);
        engine
            .add_rule(RuleDefinition {
                expression: "select device from data".to_string(),
                destinations: vec!["dest".to_string()],
                schema: Some("bifrore.test.UnknownPayload".to_string()),
            })
            .expect("add rule");

        let payload = TypedRuntimePayload {
            temp: 21.0,
            device: "sensor-x".to_string(),
            online: true,
        };
        let message = Message::new("data", payload.encode_to_vec());
        let results = engine.evaluate(&message);
        assert!(results.is_empty());

        let snapshot = engine.metrics().snapshot();
        assert_eq!(snapshot.payload_schema_error_count, 1);
        assert_eq!(snapshot.payload_decode_error_count, 0);
        assert_eq!(snapshot.payload_build_error_count, 0);
    }

    #[test]
    fn trie_matches_wildcards() {
        let mut engine = RuleEngine::default();
        engine
            .add_rule(RuleDefinition {
                expression: "select * from sensors/+/temp".to_string(),
                destinations: vec!["dest".to_string()],
                schema: None,
            })
            .expect("add rule");
        engine
            .add_rule(RuleDefinition {
                expression: "select * from sensors/#".to_string(),
                destinations: vec!["dest".to_string()],
                schema: None,
            })
            .expect("add rule");

        let payload = serde_json::json!({"temp": 10});
        let message = Message::new("sensors/room1/temp", serde_json::to_vec(&payload).unwrap());
        let results = engine.evaluate(&message);
        assert_eq!(results.len(), 2);
    }

    #[test]
    fn trie_removes_rules_incrementally() {
        let mut engine = RuleEngine::default();
        let rule_id = engine
            .add_rule(RuleDefinition {
                expression: "select * from sensors/+/temp".to_string(),
                destinations: vec!["dest".to_string()],
                schema: None,
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
    fn evaluate_with_schema_based_protobuf_predicates_and_projection() {
        let decoder =
            dynamic_protobuf_registry_from_descriptor_set_bytes(include_bytes!("../testdata/bifrore_test.desc"))
                .expect("protobuf registry");
        let mut engine = RuleEngine::new(decoder);
        engine
            .add_rule(RuleDefinition {
                expression: "select device as d from data where temp >= 20 and online = true"
                    .to_string(),
                destinations: vec!["dest".to_string()],
                schema: Some("bifrore.test.TypedRuntimePayload".to_string()),
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
    fn evaluate_with_schema_based_protobuf_invalid_payload_is_dropped() {
        let decoder =
            dynamic_protobuf_registry_from_descriptor_set_bytes(include_bytes!("../testdata/bifrore_test.desc"))
                .expect("protobuf registry");
        let mut engine = RuleEngine::new(decoder);
        engine
            .add_rule(RuleDefinition {
                expression: "select * from data where temp > 10".to_string(),
                destinations: vec!["dest".to_string()],
                schema: Some("bifrore.test.TypedRuntimePayload".to_string()),
            })
            .expect("add rule");

        let message = Message::new("data", vec![1, 2, 3, 4, 5]);
        let results = engine.evaluate(&message);
        assert!(results.is_empty());
    }

    #[test]
    fn evaluate_with_schema_based_protobuf_decoder() {
        let decoder =
            dynamic_protobuf_registry_from_descriptor_set_bytes(include_bytes!("../testdata/bifrore_test.desc"))
                .expect("protobuf registry");
        let mut engine = RuleEngine::new(decoder);
        engine
            .add_rule(RuleDefinition {
                expression: "select device as d from data where temp > 20".to_string(),
                destinations: vec!["dest".to_string()],
                schema: Some("bifrore.test.TypedRuntimePayload".to_string()),
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
    fn evaluate_with_multi_schema_protobuf_rules() {
        let decoder =
            dynamic_protobuf_registry_from_descriptor_set_bytes(include_bytes!("../testdata/bifrore_test.desc"))
                .expect("protobuf registry");
        let mut engine = RuleEngine::new(decoder);
        engine
            .add_rule(RuleDefinition {
                expression: "select temp as t from data where hum >= 50".to_string(),
                destinations: vec!["dest-eval".to_string()],
                schema: Some("bifrore.test.EvalPayload".to_string()),
            })
            .expect("add eval rule");
        engine
            .add_rule(RuleDefinition {
                expression: "select device as d from data where temp >= 20 and online = true"
                    .to_string(),
                destinations: vec!["dest-typed".to_string()],
                schema: Some("bifrore.test.TypedRuntimePayload".to_string()),
            })
            .expect("add typed rule");

        let eval_payload = EvalPayload {
            temp: 30.0,
            hum: 61.0,
        };
        let eval_results = engine.evaluate(&Message::new("data", eval_payload.encode_to_vec()));
        assert_eq!(eval_results.len(), 1);
        let eval_output: serde_json::Value =
            serde_json::from_slice(&eval_results[0].message.payload).expect("eval output");
        assert_eq!(eval_output["t"].as_f64(), Some(30.0));

        let typed_payload = TypedRuntimePayload {
            temp: 22.0,
            device: "typed-dev".to_string(),
            online: true,
        };
        let typed_results =
            engine.evaluate(&Message::new("data", typed_payload.encode_to_vec()));
        assert_eq!(typed_results.len(), 1);
        let typed_output: serde_json::Value =
            serde_json::from_slice(&typed_results[0].message.payload).expect("typed output");
        assert_eq!(typed_output["d"], serde_json::Value::from("typed-dev"));
    }

    #[test]
    fn protobuf_rules_require_schema_name() {
        let decoder =
            dynamic_protobuf_registry_from_descriptor_set_bytes(include_bytes!("../testdata/bifrore_test.desc"))
                .expect("protobuf registry");
        let mut engine = RuleEngine::new(decoder);
        let error = engine
            .add_rule(RuleDefinition {
                expression: "select temp from data".to_string(),
                destinations: vec!["dest".to_string()],
                schema: None,
            })
            .expect_err("protobuf rules without schema should fail");
        assert!(matches!(error, RuleError::MissingSchemaName));
    }

    #[test]
    fn json_rules_reject_protobuf_schema_name() {
        let mut engine = RuleEngine::default();
        let error = engine
            .add_rule(RuleDefinition {
                expression: "select temp from data".to_string(),
                destinations: vec!["dest".to_string()],
                schema: Some("bifrore.test.EvalPayload".to_string()),
            })
            .expect_err("json rules with schema should fail");
        assert!(matches!(error, RuleError::UnexpectedSchemaName));
    }

    #[test]
    fn topic_match_cache_reuses_entries() {
        let mut engine = RuleEngine::new_with_cache_capacity(PayloadDecoder::Json, 8);
        engine
            .add_rule(RuleDefinition {
                expression: "select * from sensors/+/temp".to_string(),
                destinations: vec!["dest".to_string()],
                schema: None,
            })
            .expect("add rule");
        let payload = serde_json::json!({"temp": 10});
        let message = Message::new("sensors/room1/temp", serde_json::to_vec(&payload).unwrap());

        assert_eq!(engine.evaluate(&message).len(), 1);
        assert_eq!(engine.evaluate(&message).len(), 1);

        assert_eq!(engine.topic_match_cache.entries.len(), 1);
        assert_eq!(engine.topic_match_cache.keys.len(), 1);
        let entry = engine
            .topic_match_cache
            .entries
            .get("sensors/room1/temp")
            .expect("cache entry");
        assert!(!entry.is_empty());
    }

    #[test]
    fn topic_match_cache_evicts_to_capacity() {
        let mut engine = RuleEngine::new_with_cache_capacity(PayloadDecoder::Json, 2);
        engine
            .add_rule(RuleDefinition {
                expression: "select * from sensors/#".to_string(),
                destinations: vec!["dest".to_string()],
                schema: None,
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

        assert_eq!(engine.topic_match_cache.entries.len(), 2);
        assert_eq!(engine.topic_match_cache.keys.len(), 2);
        assert!(engine.topic_match_cache.entries.contains_key("sensors/c/temp"));
        for key in &engine.topic_match_cache.keys {
            assert!(engine.topic_match_cache.entries.contains_key(key));
        }
    }
}
