use crate::message::Message;
use crate::msg_ir::{MsgIr, PayloadValue};
#[cfg(test)]
use crate::payload::{decode_payload_ir, PayloadFormat};
use regex::Regex;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashSet;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use sqlparser::ast::{
    BinaryOperator, Expr, FunctionArg, FunctionArgExpr, FunctionArguments, Ident, Query, Select,
    SelectItem, SetExpr, Statement, TableFactor,
};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RuleDefinition {
    pub expression: String,
    pub destinations: Vec<String>,
}

#[derive(Debug)]
pub struct CompiledRule {
    pub id: String,
    pub topic_filter: String,
    pub aliased_topic_filter: String,
    pub select: SelectSpec,
    pub select_plan: SelectPlan,
    pub where_expr: Option<Expr>,
    pub where_plan: Option<EvalExprPlan>,
    pub fast_where: Option<FastPredicate>,
    pub fast_path_profile: FastPathProfile,
    pub destinations: Vec<String>,
    pub required_payload_fields: HashSet<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SelectSpec {
    All,
    Columns(Vec<SelectedColumn>),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SelectedColumn {
    pub expr: Expr,
    pub alias: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SelectPlan {
    All,
    Columns(Vec<PlannedColumn>),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlannedColumn {
    pub expr: EvalExprPlan,
    pub alias: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EvalExprPlan {
    Const(Value),
    PayloadField(String),
    MetaQos,
    MetaRetain,
    MetaDup,
    MetaTimestamp,
    MetaClientId,
    MetaUsername,
    Binary {
        left: Box<EvalExprPlan>,
        op: BinaryOperator,
        right: Box<EvalExprPlan>,
    },
    TopicLevel {
        level: Box<EvalExprPlan>,
    },
    PropertiesKey {
        key: Box<EvalExprPlan>,
    },
    Unknown(Expr),
}

#[derive(Debug)]
pub enum FastPredicate {
    Compare {
        field: FastField,
        op: FastCmpOp,
        value: FastValue,
    },
    And {
        left: Box<FastPredicate>,
        right: Box<FastPredicate>,
        profile: BranchProfile,
    },
    Or {
        left: Box<FastPredicate>,
        right: Box<FastPredicate>,
        profile: BranchProfile,
    },
    Const(bool),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FastField {
    Payload(String),
    MetaQos,
    MetaRetain,
    MetaDup,
    MetaTimestamp,
    MetaClientId,
    MetaUsername,
    TopicLevel(u64),
    Property(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FastCmpOp {
    Eq,
    NotEq,
    Gt,
    GtEq,
    Lt,
    LtEq,
}

#[derive(Debug, Clone, PartialEq)]
pub enum FastValue {
    Number(f64),
    String(String),
    Bool(bool),
}

#[derive(Debug, Default)]
pub struct FastPathProfile {
    enabled: AtomicBool,
    attempts: AtomicU64,
    misses: AtomicU64,
    downgrade_logged: AtomicBool,
}

impl FastPathProfile {
    fn new() -> Self {
        Self {
            enabled: AtomicBool::new(true),
            attempts: AtomicU64::new(0),
            misses: AtomicU64::new(0),
            downgrade_logged: AtomicBool::new(false),
        }
    }
}

#[derive(Debug, Default)]
pub struct BranchProfile {
    left_true: AtomicU64,
    left_false: AtomicU64,
    right_true: AtomicU64,
    right_false: AtomicU64,
}

#[derive(Debug, thiserror::Error)]
pub enum RuleError {
    #[error("SQL parse error: {0}")]
    SqlParse(String),
    #[error("unsupported SQL form")]
    UnsupportedSql,
    #[error("missing topic filter in FROM")]
    MissingTopic,
}

pub fn compile_rule(rule: RuleDefinition) -> Result<CompiledRule, RuleError> {
    let dialect = GenericDialect {};
    let normalized_sql = normalize_rule_sql(&rule.expression);
    let mut statements =
        Parser::parse_sql(&dialect, &normalized_sql)
            .map_err(|e| RuleError::SqlParse(e.to_string()))?;
    if statements.len() != 1 {
        return Err(RuleError::UnsupportedSql);
    }
    let statement = statements.pop().expect("statement already checked");
    let query = match statement {
        Statement::Query(query) => query,
        _ => return Err(RuleError::UnsupportedSql),
    };
    let (topic_filter, aliased_topic_filter, select, where_expr) = parse_query(*query)?;
    let select_plan = compile_select_plan(&select);
    let where_plan = where_expr.as_ref().map(compile_expr_plan);
    let fast_where = where_plan.as_ref().and_then(compile_fast_predicate);
    let required_payload_fields = collect_required_payload_fields(&select_plan, where_plan.as_ref());
    let has_unknown_plan = select_plan_has_unknown(&select_plan)
        || where_plan
            .as_ref()
            .map(plan_has_unknown)
            .unwrap_or(false);
    if has_unknown_plan {
        log::warn!(
            "dropping incompatible rule during compile expression={}",
            rule.expression
        );
        return Err(RuleError::UnsupportedSql);
    }
    let rule_id = format!("{:x}", fxhash::hash64(&rule.expression));
    Ok(CompiledRule {
        id: rule_id,
        topic_filter,
        aliased_topic_filter,
        select,
        select_plan,
        where_expr,
        where_plan,
        fast_where,
        fast_path_profile: FastPathProfile::new(),
        destinations: rule.destinations,
        required_payload_fields,
    })
}

fn parse_query(query: Query) -> Result<(String, String, SelectSpec, Option<Expr>), RuleError> {
    let select = match *query.body {
        SetExpr::Select(select) => select,
        _ => return Err(RuleError::UnsupportedSql),
    };
    let (topic_filter, aliased, select_spec) = parse_select(*select)?;
    Ok((topic_filter, aliased, select_spec.0, select_spec.1))
}

fn parse_select(
    select: Select,
) -> Result<(String, String, (SelectSpec, Option<Expr>)), RuleError> {
    let from = select
        .from
        .get(0)
        .ok_or(RuleError::MissingTopic)?;
    let relation = &from.relation;
    let (topic_filter, aliased) = match relation {
        TableFactor::Table { name, alias, .. } => {
            let filter = normalize_topic(&name.to_string());
            let alias_value = alias
                .as_ref()
                .map(|alias| alias.name.value.clone())
                .unwrap_or_else(|| filter.clone());
            (filter, alias_value)
        }
        _ => return Err(RuleError::UnsupportedSql),
    };

    let select_spec = parse_select_items(&select.projection)?;
    Ok((topic_filter, aliased, (select_spec, select.selection)))
}

fn parse_select_items(items: &[SelectItem]) -> Result<SelectSpec, RuleError> {
    if items.len() == 1 {
        if let SelectItem::Wildcard(_) = items[0] {
            return Ok(SelectSpec::All);
        }
    }

    let mut columns = Vec::with_capacity(items.len());
    for item in items {
        match item {
            SelectItem::UnnamedExpr(expr) => {
                let alias = derive_alias(expr);
                columns.push(SelectedColumn {
                    expr: expr.clone(),
                    alias,
                });
            }
            SelectItem::ExprWithAlias { expr, alias } => {
                columns.push(SelectedColumn {
                    expr: expr.clone(),
                    alias: alias.value.clone(),
                });
            }
            SelectItem::Wildcard(_) => return Ok(SelectSpec::All),
            _ => return Err(RuleError::UnsupportedSql),
        }
    }
    Ok(SelectSpec::Columns(columns))
}

fn compile_select_plan(select: &SelectSpec) -> SelectPlan {
    match select {
        SelectSpec::All => SelectPlan::All,
        SelectSpec::Columns(columns) => SelectPlan::Columns(
            columns
                .iter()
                .map(|column| PlannedColumn {
                    expr: compile_expr_plan(&column.expr),
                    alias: column.alias.clone(),
                })
                .collect(),
        ),
    }
}

fn compile_expr_plan(expr: &Expr) -> EvalExprPlan {
    match expr {
        Expr::Identifier(Ident { value, .. }) => match value.as_str() {
            "qos" => EvalExprPlan::MetaQos,
            "retain" => EvalExprPlan::MetaRetain,
            "dup" => EvalExprPlan::MetaDup,
            "timestamp" => EvalExprPlan::MetaTimestamp,
            "clientId" => EvalExprPlan::MetaClientId,
            "username" => EvalExprPlan::MetaUsername,
            _ => EvalExprPlan::PayloadField(value.clone()),
        },
        Expr::CompoundIdentifier(idents) => EvalExprPlan::PayloadField(
            idents.iter().map(|ident| ident.value.as_str()).collect::<Vec<_>>().join("."),
        ),
        Expr::Value(value) => match value {
            sqlparser::ast::Value::Number(num, _) => num
                .parse::<f64>()
                .ok()
                .and_then(serde_json::Number::from_f64)
                .map(Value::Number)
                .map(EvalExprPlan::Const)
                .unwrap_or_else(|| EvalExprPlan::Unknown(expr.clone())),
            sqlparser::ast::Value::SingleQuotedString(s) => {
                EvalExprPlan::Const(Value::String(s.clone()))
            }
            sqlparser::ast::Value::Boolean(b) => EvalExprPlan::Const(Value::Bool(*b)),
            _ => EvalExprPlan::Unknown(expr.clone()),
        },
        Expr::BinaryOp { left, op, right } => {
            let left_plan = compile_expr_plan(left);
            let right_plan = compile_expr_plan(right);
            fold_binary_plan(left_plan, op.clone(), right_plan)
        }
        Expr::Nested(inner) => compile_expr_plan(inner),
        Expr::Function(func) => {
            let name = func.name.to_string().to_lowercase();
            if name == "topic_level" {
                if let FunctionArguments::List(list) = &func.args {
                    if list.args.len() == 2 {
                        if let FunctionArg::Unnamed(FunctionArgExpr::Expr(level_expr)) = &list.args[1] {
                            return EvalExprPlan::TopicLevel {
                                level: Box::new(compile_expr_plan(level_expr)),
                            };
                        }
                    }
                }
            }
            EvalExprPlan::Unknown(expr.clone())
        }
        Expr::MapAccess { column, keys } => {
            if let Expr::Identifier(Ident { value, .. }) = &**column {
                if value == "properties" {
                    if let Some(first_key) = keys.first() {
                        return EvalExprPlan::PropertiesKey {
                            key: Box::new(compile_expr_plan(&first_key.key)),
                        };
                    }
                }
            }
            EvalExprPlan::Unknown(expr.clone())
        }
        Expr::ArrayIndex { obj, indexes } => {
            if let Expr::Identifier(Ident { value, .. }) = &**obj {
                if value == "properties" {
                    if let Some(first_index) = indexes.first() {
                        return EvalExprPlan::PropertiesKey {
                            key: Box::new(compile_expr_plan(first_index)),
                        };
                    }
                }
            }
            EvalExprPlan::Unknown(expr.clone())
        }
        _ => EvalExprPlan::Unknown(expr.clone()),
    }
}

fn fold_binary_plan(left: EvalExprPlan, op: BinaryOperator, right: EvalExprPlan) -> EvalExprPlan {
    if let Some(folded) = fold_const_binary(&left, &op, &right) {
        return EvalExprPlan::Const(folded);
    }

    EvalExprPlan::Binary {
        left: Box::new(left),
        op,
        right: Box::new(right),
    }
}

fn fold_const_binary(left: &EvalExprPlan, op: &BinaryOperator, right: &EvalExprPlan) -> Option<Value> {
    let (EvalExprPlan::Const(left), EvalExprPlan::Const(right)) = (left, right) else {
        return None;
    };

    match op {
        BinaryOperator::Plus
        | BinaryOperator::Minus
        | BinaryOperator::Multiply
        | BinaryOperator::Divide => evaluate_arithmetic(left, right, op),
        BinaryOperator::Gt
        | BinaryOperator::GtEq
        | BinaryOperator::Lt
        | BinaryOperator::LtEq
        | BinaryOperator::Eq
        | BinaryOperator::NotEq => compare_values(left, right, op).map(Value::Bool),
        BinaryOperator::And | BinaryOperator::Or => {
            let l = left.as_bool()?;
            let r = right.as_bool()?;
            Some(Value::Bool(match op {
                BinaryOperator::And => l && r,
                BinaryOperator::Or => l || r,
                _ => return None,
            }))
        }
        _ => None,
    }
}

fn select_plan_has_unknown(plan: &SelectPlan) -> bool {
    match plan {
        SelectPlan::All => false,
        SelectPlan::Columns(columns) => columns.iter().any(|column| plan_has_unknown(&column.expr)),
    }
}

fn plan_has_unknown(plan: &EvalExprPlan) -> bool {
    match plan {
        EvalExprPlan::Unknown(_) => true,
        EvalExprPlan::Binary { left, right, .. } => {
            plan_has_unknown(left) || plan_has_unknown(right)
        }
        EvalExprPlan::TopicLevel { level } => plan_has_unknown(level),
        EvalExprPlan::PropertiesKey { key } => plan_has_unknown(key),
        _ => false,
    }
}

fn collect_required_payload_fields(
    select_plan: &SelectPlan,
    where_plan: Option<&EvalExprPlan>,
) -> HashSet<String> {
    let mut fields = HashSet::new();
    collect_required_payload_fields_from_select(select_plan, &mut fields);
    if let Some(where_plan) = where_plan {
        collect_required_payload_fields_from_plan(where_plan, &mut fields);
    }
    fields
}

fn collect_required_payload_fields_from_select(
    select_plan: &SelectPlan,
    fields: &mut HashSet<String>,
) {
    match select_plan {
        SelectPlan::All => {}
        SelectPlan::Columns(columns) => {
            for column in columns {
                collect_required_payload_fields_from_plan(&column.expr, fields);
            }
        }
    }
}

fn collect_required_payload_fields_from_plan(plan: &EvalExprPlan, fields: &mut HashSet<String>) {
    match plan {
        EvalExprPlan::PayloadField(path) => {
            fields.insert(path.clone());
        }
        EvalExprPlan::Binary { left, right, .. } => {
            collect_required_payload_fields_from_plan(left, fields);
            collect_required_payload_fields_from_plan(right, fields);
        }
        EvalExprPlan::TopicLevel { level } => {
            collect_required_payload_fields_from_plan(level, fields);
        }
        EvalExprPlan::PropertiesKey { key } => {
            collect_required_payload_fields_from_plan(key, fields);
        }
        _ => {}
    }
}

fn compile_fast_predicate(plan: &EvalExprPlan) -> Option<FastPredicate> {
    match plan {
        EvalExprPlan::Const(Value::Bool(value)) => Some(FastPredicate::Const(*value)),
        EvalExprPlan::Binary { left, op, right } if matches!(op, BinaryOperator::And | BinaryOperator::Or) => {
            let left = compile_fast_predicate(left)?;
            let right = compile_fast_predicate(right)?;
            Some(match op {
                BinaryOperator::And => FastPredicate::And {
                    left: Box::new(left),
                    right: Box::new(right),
                    profile: BranchProfile::default(),
                },
                BinaryOperator::Or => FastPredicate::Or {
                    left: Box::new(left),
                    right: Box::new(right),
                    profile: BranchProfile::default(),
                },
                _ => return None,
            })
        }
        EvalExprPlan::Binary { left, op, right } => {
            let cmp = fast_cmp_op(op)?;
            if let (Some(field), Some(value)) = (compile_fast_field(left), compile_fast_const(right)) {
                return Some(FastPredicate::Compare {
                    field,
                    op: cmp,
                    value,
                });
            }
            if let (Some(field), Some(value)) = (compile_fast_field(right), compile_fast_const(left)) {
                return Some(FastPredicate::Compare {
                    field,
                    op: invert_fast_cmp_op(cmp),
                    value,
                });
            }
            None
        }
        _ => None,
    }
}

fn compile_fast_field(plan: &EvalExprPlan) -> Option<FastField> {
    match plan {
        EvalExprPlan::PayloadField(path) => Some(FastField::Payload(path.clone())),
        EvalExprPlan::MetaQos => Some(FastField::MetaQos),
        EvalExprPlan::MetaRetain => Some(FastField::MetaRetain),
        EvalExprPlan::MetaDup => Some(FastField::MetaDup),
        EvalExprPlan::MetaTimestamp => Some(FastField::MetaTimestamp),
        EvalExprPlan::MetaClientId => Some(FastField::MetaClientId),
        EvalExprPlan::MetaUsername => Some(FastField::MetaUsername),
        EvalExprPlan::TopicLevel { level } => {
            let raw = compile_fast_const(level)?;
            let FastValue::Number(number) = raw else {
                return None;
            };
            if number < 0.0 || number.fract() != 0.0 || number > u64::MAX as f64 {
                return None;
            }
            Some(FastField::TopicLevel(number as u64))
        }
        EvalExprPlan::PropertiesKey { key } => {
            let value = compile_fast_const(key)?;
            match value {
                FastValue::String(key) => Some(FastField::Property(key)),
                _ => None,
            }
        }
        _ => None,
    }
}

fn compile_fast_const(plan: &EvalExprPlan) -> Option<FastValue> {
    match plan {
        EvalExprPlan::Const(Value::Number(value)) => value.as_f64().map(FastValue::Number),
        EvalExprPlan::Const(Value::String(value)) => Some(FastValue::String(value.clone())),
        EvalExprPlan::Const(Value::Bool(value)) => Some(FastValue::Bool(*value)),
        _ => None,
    }
}

fn fast_cmp_op(op: &BinaryOperator) -> Option<FastCmpOp> {
    match op {
        BinaryOperator::Eq => Some(FastCmpOp::Eq),
        BinaryOperator::NotEq => Some(FastCmpOp::NotEq),
        BinaryOperator::Gt => Some(FastCmpOp::Gt),
        BinaryOperator::GtEq => Some(FastCmpOp::GtEq),
        BinaryOperator::Lt => Some(FastCmpOp::Lt),
        BinaryOperator::LtEq => Some(FastCmpOp::LtEq),
        _ => None,
    }
}

fn invert_fast_cmp_op(op: FastCmpOp) -> FastCmpOp {
    match op {
        FastCmpOp::Eq => FastCmpOp::Eq,
        FastCmpOp::NotEq => FastCmpOp::NotEq,
        FastCmpOp::Gt => FastCmpOp::Lt,
        FastCmpOp::GtEq => FastCmpOp::LtEq,
        FastCmpOp::Lt => FastCmpOp::Gt,
        FastCmpOp::LtEq => FastCmpOp::GtEq,
    }
}

pub(crate) fn evaluate_rule_with_payload_and_topic_parts(
    rule: &CompiledRule,
    message: &Message,
    payload_obj: &MsgIr,
    topic_parts: &[&str],
) -> Option<Message> {
    let context = EvalContext {
        message,
        payload: payload_obj,
        topic_parts,
    };

    if let Some(fast_where) = &rule.fast_where {
        if rule.fast_path_profile.enabled.load(Ordering::Relaxed) {
            rule.fast_path_profile
                .attempts
                .fetch_add(1, Ordering::Relaxed);
            match evaluate_fast_predicate(fast_where, &context) {
                Some(true) => {}
                Some(false) => return None,
                None => {
                    let misses = rule.fast_path_profile.misses.fetch_add(1, Ordering::Relaxed) + 1;
                    let attempts = rule.fast_path_profile.attempts.load(Ordering::Relaxed);
                    if attempts >= 128 && misses.saturating_mul(100) >= attempts.saturating_mul(90) {
                        rule.fast_path_profile.enabled.store(false, Ordering::Relaxed);
                        if !rule
                            .fast_path_profile
                            .downgrade_logged
                            .swap(true, Ordering::Relaxed)
                        {
                            log::warn!(
                                "downgrading fast predicate for rule_id={} due to high miss ratio {}/{}",
                                rule.id,
                                misses,
                                attempts
                            );
                        }
                    }
                }
            }
        }
    }

    if let Some(plan) = &rule.where_plan {
        if !evaluate_plan_bool(plan, &context).unwrap_or(false) {
            return None;
        }
    }

    match &rule.select_plan {
        SelectPlan::All => Some(message.clone()),
        SelectPlan::Columns(columns) => {
            let mut output = serde_json::Map::with_capacity(columns.len());
            for column in columns {
                let value = evaluate_plan_value(&column.expr, &context).unwrap_or(Value::Null);
                output.insert(column.alias.clone(), value);
            }
            let new_payload = serde_json::to_vec(&Value::Object(output)).ok()?;
            Some(Message::new(&message.topic, new_payload))
        }
    }
}

struct EvalContext<'a> {
    message: &'a Message,
    payload: &'a MsgIr,
    topic_parts: &'a [&'a str],
}

fn evaluate_plan_bool(plan: &EvalExprPlan, ctx: &EvalContext) -> Option<bool> {
    match plan {
        EvalExprPlan::Binary { left, op, right } if matches!(op, BinaryOperator::And | BinaryOperator::Or) => {
            let left_val = evaluate_plan_bool(left, ctx)?;
            let right_val = evaluate_plan_bool(right, ctx)?;
            Some(match op {
                BinaryOperator::And => left_val && right_val,
                BinaryOperator::Or => left_val || right_val,
                _ => false,
            })
        }
        EvalExprPlan::Binary { left, op, right }
            if matches!(
                op,
                BinaryOperator::Gt
                    | BinaryOperator::GtEq
                    | BinaryOperator::Lt
                    | BinaryOperator::LtEq
                    | BinaryOperator::Eq
                    | BinaryOperator::NotEq
            ) =>
        {
            if let Some(value) = evaluate_plan_compare_bool(left, op, right, ctx) {
                return Some(value);
            }
            let left_val = evaluate_plan_value(left, ctx)?;
            let right_val = evaluate_plan_value(right, ctx)?;
            compare_values(&left_val, &right_val, op)
        }
        _ => {
            let value = evaluate_plan_value(plan, ctx)?;
            match value {
                Value::Bool(v) => Some(v),
                _ => None,
            }
        }
    }
}

#[derive(Debug, Clone)]
enum RuntimeValue<'a> {
    Number(f64),
    String(&'a str),
    Bool(bool),
}

fn evaluate_fast_predicate(pred: &FastPredicate, ctx: &EvalContext) -> Option<bool> {
    match pred {
        FastPredicate::Const(value) => Some(*value),
        FastPredicate::And { left, right, profile } => evaluate_fast_and(left, right, profile, ctx),
        FastPredicate::Or { left, right, profile } => evaluate_fast_or(left, right, profile, ctx),
        FastPredicate::Compare { field, op, value } => {
            let runtime_value = evaluate_fast_field(field, ctx)?;
            compare_runtime_value(&runtime_value, value, op)
        }
    }
}

fn evaluate_fast_and(
    left: &FastPredicate,
    right: &FastPredicate,
    profile: &BranchProfile,
    ctx: &EvalContext,
) -> Option<bool> {
    let left_total = profile.left_true.load(Ordering::Relaxed) + profile.left_false.load(Ordering::Relaxed);
    let right_total = profile.right_true.load(Ordering::Relaxed) + profile.right_false.load(Ordering::Relaxed);
    let left_false_rate = if left_total == 0 {
        0.5
    } else {
        profile.left_false.load(Ordering::Relaxed) as f64 / left_total as f64
    };
    let right_false_rate = if right_total == 0 {
        0.5
    } else {
        profile.right_false.load(Ordering::Relaxed) as f64 / right_total as f64
    };
    let right_first = right_false_rate > left_false_rate;

    if right_first {
        let first = evaluate_fast_predicate(right, ctx)?;
        if first {
            profile.right_true.fetch_add(1, Ordering::Relaxed);
        } else {
            profile.right_false.fetch_add(1, Ordering::Relaxed);
            return Some(false);
        }
        let second = evaluate_fast_predicate(left, ctx)?;
        if second {
            profile.left_true.fetch_add(1, Ordering::Relaxed);
        } else {
            profile.left_false.fetch_add(1, Ordering::Relaxed);
        }
        Some(second)
    } else {
        let first = evaluate_fast_predicate(left, ctx)?;
        if first {
            profile.left_true.fetch_add(1, Ordering::Relaxed);
        } else {
            profile.left_false.fetch_add(1, Ordering::Relaxed);
            return Some(false);
        }
        let second = evaluate_fast_predicate(right, ctx)?;
        if second {
            profile.right_true.fetch_add(1, Ordering::Relaxed);
        } else {
            profile.right_false.fetch_add(1, Ordering::Relaxed);
        }
        Some(second)
    }
}

fn evaluate_fast_or(
    left: &FastPredicate,
    right: &FastPredicate,
    profile: &BranchProfile,
    ctx: &EvalContext,
) -> Option<bool> {
    let left_total = profile.left_true.load(Ordering::Relaxed) + profile.left_false.load(Ordering::Relaxed);
    let right_total = profile.right_true.load(Ordering::Relaxed) + profile.right_false.load(Ordering::Relaxed);
    let left_true_rate = if left_total == 0 {
        0.5
    } else {
        profile.left_true.load(Ordering::Relaxed) as f64 / left_total as f64
    };
    let right_true_rate = if right_total == 0 {
        0.5
    } else {
        profile.right_true.load(Ordering::Relaxed) as f64 / right_total as f64
    };
    let right_first = right_true_rate > left_true_rate;

    if right_first {
        let first = evaluate_fast_predicate(right, ctx)?;
        if first {
            profile.right_true.fetch_add(1, Ordering::Relaxed);
            return Some(true);
        }
        profile.right_false.fetch_add(1, Ordering::Relaxed);
        let second = evaluate_fast_predicate(left, ctx)?;
        if second {
            profile.left_true.fetch_add(1, Ordering::Relaxed);
        } else {
            profile.left_false.fetch_add(1, Ordering::Relaxed);
        }
        Some(second)
    } else {
        let first = evaluate_fast_predicate(left, ctx)?;
        if first {
            profile.left_true.fetch_add(1, Ordering::Relaxed);
            return Some(true);
        }
        profile.left_false.fetch_add(1, Ordering::Relaxed);
        let second = evaluate_fast_predicate(right, ctx)?;
        if second {
            profile.right_true.fetch_add(1, Ordering::Relaxed);
        } else {
            profile.right_false.fetch_add(1, Ordering::Relaxed);
        }
        Some(second)
    }
}

fn evaluate_fast_field<'a>(field: &FastField, ctx: &'a EvalContext) -> Option<RuntimeValue<'a>> {
    match field {
        FastField::Payload(path) => match payload_value_by_path(ctx.payload, path)? {
            PayloadValue::Number(number) => Some(RuntimeValue::Number(*number)),
            PayloadValue::String(value) => Some(RuntimeValue::String(value)),
            PayloadValue::Bool(value) => Some(RuntimeValue::Bool(*value)),
            _ => None,
        },
        FastField::MetaQos => Some(RuntimeValue::Number(ctx.message.qos as f64)),
        FastField::MetaRetain => Some(RuntimeValue::Bool(ctx.message.retain)),
        FastField::MetaDup => Some(RuntimeValue::Bool(ctx.message.dup)),
        FastField::MetaTimestamp => Some(RuntimeValue::Number(ctx.message.timestamp_millis as f64)),
        FastField::MetaClientId => ctx
            .message
            .client_id
            .as_ref()
            .map(|value| RuntimeValue::String(value)),
        FastField::MetaUsername => ctx
            .message
            .username
            .as_ref()
            .map(|value| RuntimeValue::String(value)),
        FastField::TopicLevel(level) => {
            let index = if *level == 0 { 0 } else { (*level - 1) as usize };
            ctx.topic_parts
                .get(index)
                .map(|value| RuntimeValue::String(value))
        }
        FastField::Property(key) => ctx
            .message
            .properties
            .get(key)
            .map(|value| RuntimeValue::String(value)),
    }
}

fn compare_runtime_value(value: &RuntimeValue, expected: &FastValue, op: &FastCmpOp) -> Option<bool> {
    match (value, expected) {
        (RuntimeValue::Number(left), FastValue::Number(right)) => Some(match op {
            FastCmpOp::Eq => (left - right).abs() < f64::EPSILON,
            FastCmpOp::NotEq => (left - right).abs() >= f64::EPSILON,
            FastCmpOp::Gt => left > right,
            FastCmpOp::GtEq => left >= right,
            FastCmpOp::Lt => left < right,
            FastCmpOp::LtEq => left <= right,
        }),
        (RuntimeValue::String(left), FastValue::String(right)) => Some(match op {
            FastCmpOp::Eq => *left == right,
            FastCmpOp::NotEq => *left != right,
            _ => return None,
        }),
        (RuntimeValue::Bool(left), FastValue::Bool(right)) => Some(match op {
            FastCmpOp::Eq => left == right,
            FastCmpOp::NotEq => left != right,
            _ => return None,
        }),
        _ => None,
    }
}

fn evaluate_plan_compare_bool(
    left: &EvalExprPlan,
    op: &BinaryOperator,
    right: &EvalExprPlan,
    ctx: &EvalContext,
) -> Option<bool> {
    let left_val = evaluate_plan_runtime_value(left, ctx)?;
    let right_val = evaluate_plan_runtime_value(right, ctx)?;
    compare_runtime_runtime(&left_val, &right_val, op)
}

fn evaluate_plan_runtime_value<'a>(
    plan: &'a EvalExprPlan,
    ctx: &'a EvalContext,
) -> Option<RuntimeValue<'a>> {
    match plan {
        EvalExprPlan::Const(value) => match value {
            Value::Number(number) => number.as_f64().map(RuntimeValue::Number),
            Value::String(text) => Some(RuntimeValue::String(text)),
            Value::Bool(flag) => Some(RuntimeValue::Bool(*flag)),
            _ => None,
        },
        EvalExprPlan::PayloadField(path) => match payload_value_by_path(ctx.payload, path)? {
            PayloadValue::Number(number) => Some(RuntimeValue::Number(*number)),
            PayloadValue::String(text) => Some(RuntimeValue::String(text)),
            PayloadValue::Bool(flag) => Some(RuntimeValue::Bool(*flag)),
            _ => None,
        },
        EvalExprPlan::MetaQos => Some(RuntimeValue::Number(ctx.message.qos as f64)),
        EvalExprPlan::MetaRetain => Some(RuntimeValue::Bool(ctx.message.retain)),
        EvalExprPlan::MetaDup => Some(RuntimeValue::Bool(ctx.message.dup)),
        EvalExprPlan::MetaTimestamp => Some(RuntimeValue::Number(ctx.message.timestamp_millis as f64)),
        EvalExprPlan::MetaClientId => ctx
            .message
            .client_id
            .as_ref()
            .map(|value| RuntimeValue::String(value)),
        EvalExprPlan::MetaUsername => ctx
            .message
            .username
            .as_ref()
            .map(|value| RuntimeValue::String(value)),
        EvalExprPlan::TopicLevel { level } => {
            let level_value = value_to_non_negative_u64(&evaluate_plan_value(level, ctx)?)?;
            let index = if level_value == 0 {
                0
            } else {
                (level_value - 1) as usize
            };
            ctx.topic_parts
                .get(index)
                .copied()
                .map(RuntimeValue::String)
        }
        EvalExprPlan::PropertiesKey { key } => {
            let key_value = evaluate_plan_value(key, ctx)?;
            let key = key_value.as_str()?;
            ctx.message
                .properties
                .get(key)
                .map(|value| RuntimeValue::String(value))
        }
        _ => None,
    }
}

fn compare_runtime_runtime(
    left: &RuntimeValue,
    right: &RuntimeValue,
    op: &BinaryOperator,
) -> Option<bool> {
    match (left, right) {
        (RuntimeValue::Number(l), RuntimeValue::Number(r)) => Some(match op {
            BinaryOperator::Gt => l > r,
            BinaryOperator::GtEq => l >= r,
            BinaryOperator::Lt => l < r,
            BinaryOperator::LtEq => l <= r,
            BinaryOperator::Eq => (l - r).abs() < f64::EPSILON,
            BinaryOperator::NotEq => (l - r).abs() >= f64::EPSILON,
            _ => return None,
        }),
        (RuntimeValue::String(l), RuntimeValue::String(r)) => Some(match op {
            BinaryOperator::Eq => l == r,
            BinaryOperator::NotEq => l != r,
            _ => return None,
        }),
        (RuntimeValue::Bool(l), RuntimeValue::Bool(r)) => Some(match op {
            BinaryOperator::Eq => l == r,
            BinaryOperator::NotEq => l != r,
            _ => return None,
        }),
        _ => None,
    }
}

fn evaluate_plan_value(plan: &EvalExprPlan, ctx: &EvalContext) -> Option<Value> {
    match plan {
        EvalExprPlan::Const(value) => Some(value.clone()),
        EvalExprPlan::PayloadField(path) => payload_value_by_path(ctx.payload, path).map(PayloadValue::to_json_value),
        EvalExprPlan::MetaQos => Some(Value::Number(serde_json::Number::from(ctx.message.qos))),
        EvalExprPlan::MetaRetain => Some(Value::Bool(ctx.message.retain)),
        EvalExprPlan::MetaDup => Some(Value::Bool(ctx.message.dup)),
        EvalExprPlan::MetaTimestamp => Some(Value::Number(serde_json::Number::from(
            ctx.message.timestamp_millis,
        ))),
        EvalExprPlan::MetaClientId => ctx
            .message
            .client_id
            .as_ref()
            .map(|value| Value::String(value.clone())),
        EvalExprPlan::MetaUsername => ctx
            .message
            .username
            .as_ref()
            .map(|value| Value::String(value.clone())),
        EvalExprPlan::Binary { left, op, right } => {
            if matches!(op, BinaryOperator::And | BinaryOperator::Or) {
                return evaluate_plan_bool(plan, ctx).map(Value::Bool);
            }

            let left_val = evaluate_plan_value(left, ctx)?;
            let right_val = evaluate_plan_value(right, ctx)?;
            match op {
                BinaryOperator::Plus
                | BinaryOperator::Minus
                | BinaryOperator::Multiply
                | BinaryOperator::Divide => evaluate_arithmetic(&left_val, &right_val, op),
                BinaryOperator::Gt
                | BinaryOperator::GtEq
                | BinaryOperator::Lt
                | BinaryOperator::LtEq
                | BinaryOperator::Eq
                | BinaryOperator::NotEq => compare_values(&left_val, &right_val, op).map(Value::Bool),
                _ => None,
            }
        }
        EvalExprPlan::TopicLevel { level } => {
            let level_value = value_to_non_negative_u64(&evaluate_plan_value(level, ctx)?)?;
            let index = if level_value == 0 {
                0
            } else {
                (level_value - 1) as usize
            };
            ctx.topic_parts
                .get(index)
                .map(|value| Value::String((*value).to_string()))
        }
        EvalExprPlan::PropertiesKey { key } => {
            let key_value = evaluate_plan_value(key, ctx)?;
            let key = key_value.as_str()?;
            ctx.message
                .properties
                .get(key)
                .map(|value| Value::String(value.clone()))
        }
        EvalExprPlan::Unknown(_) => None,
    }
}

fn evaluate_arithmetic(left: &Value, right: &Value, op: &BinaryOperator) -> Option<Value> {
    let left = left.as_f64()?;
    let right = right.as_f64()?;
    let result = match op {
        BinaryOperator::Plus => left + right,
        BinaryOperator::Minus => left - right,
        BinaryOperator::Multiply => left * right,
        BinaryOperator::Divide => left / right,
        _ => return None,
    };
    serde_json::Number::from_f64(result).map(Value::Number)
}

fn compare_values(left: &Value, right: &Value, op: &BinaryOperator) -> Option<bool> {
    match (left, right) {
        (Value::Number(l), Value::Number(r)) => {
            let l = l.as_f64()?;
            let r = r.as_f64()?;
            Some(match op {
                BinaryOperator::Gt => l > r,
                BinaryOperator::GtEq => l >= r,
                BinaryOperator::Lt => l < r,
                BinaryOperator::LtEq => l <= r,
                BinaryOperator::Eq => (l - r).abs() < f64::EPSILON,
                BinaryOperator::NotEq => (l - r).abs() >= f64::EPSILON,
                _ => return None,
            })
        }
        (Value::String(l), Value::String(r)) => Some(match op {
            BinaryOperator::Eq => l == r,
            BinaryOperator::NotEq => l != r,
            _ => return None,
        }),
        (Value::Bool(l), Value::Bool(r)) => Some(match op {
            BinaryOperator::Eq => l == r,
            BinaryOperator::NotEq => l != r,
            _ => return None,
        }),
        _ => None,
    }
}

fn value_to_non_negative_u64(value: &Value) -> Option<u64> {
    if let Some(raw) = value.as_u64() {
        return Some(raw);
    }
    let number = value.as_f64()?;
    if number < 0.0 || number.fract() != 0.0 || number > u64::MAX as f64 {
        return None;
    }
    Some(number as u64)
}

fn derive_alias(expr: &Expr) -> String {
    match expr {
        Expr::Identifier(Ident { value, .. }) => value.clone(),
        Expr::CompoundIdentifier(idents) => idents
            .last()
            .map(|ident| ident.value.clone())
            .unwrap_or_else(|| expr.to_string()),
        _ => expr.to_string(),
    }
}

fn payload_value_by_path<'a>(payload: &'a MsgIr, path: &str) -> Option<&'a PayloadValue> {
    payload.get_key(path)
}

fn normalize_topic(topic: &str) -> String {
    let trimmed = topic.trim();
    if trimmed.starts_with('\"') && trimmed.ends_with('\"') && trimmed.len() >= 2 {
        trimmed[1..trimmed.len() - 1].to_string()
    } else {
        trimmed.to_string()
    }
}

fn normalize_rule_sql(sql: &str) -> String {
    let quoted = Regex::new(r"(?i)\bfrom\s+'([^']+)'").expect("regex");
    let normalized = quoted.replace(sql, r#"from "$1""#).to_string();

    let bare = Regex::new(r#"(?i)\bfrom\s+([^\s,()]+)"#).expect("regex");
    bare.replace(&normalized, |caps: &regex::Captures<'_>| {
        let token = caps.get(1).map(|m| m.as_str()).unwrap_or_default();
        if token.starts_with('"')
            || token.starts_with('\'')
            || !(token.contains('/') || token.contains('+') || token.contains('#'))
        {
            caps.get(0)
                .map(|m| m.as_str().to_string())
                .unwrap_or_default()
        } else {
            format!(r#"from "{}""#, token)
        }
    })
    .to_string()
}

pub fn match_topic(filter: &str, topic: &str) -> bool {
    if filter == "#" {
        return true;
    }
    let filter_levels: Vec<&str> = filter.split('/').collect();
    let topic_levels: Vec<&str> = topic.split('/').collect();

    let mut i = 0;
    while i < filter_levels.len() {
        let f = filter_levels[i];
        if f == "#" {
            return true;
        }
        if i >= topic_levels.len() {
            return false;
        }
        let t = topic_levels[i];
        if f != "+" && f != t {
            return false;
        }
        i += 1;
    }
    i == topic_levels.len()
}

#[cfg(test)]
mod tests {
    use super::*;
    use pretty_assertions::assert_eq;
    
    pub fn evaluate_rule(rule: &CompiledRule, message: &Message) -> Option<Message> {
        let obj = decode_payload_ir(&message.payload, PayloadFormat::Json).ok()?;
        let topic_parts: Vec<&str> = message.topic.split('/').collect();
        evaluate_rule_with_payload_and_topic_parts(rule, message, &obj, &topic_parts)
    }

    #[test]
    fn compile_basic_rule() {
        let rule = RuleDefinition {
            expression: "select * from a/b/c".to_string(),
            destinations: vec!["dest1".to_string()],
        };
        let compiled = compile_rule(rule).expect("compile rule");
        assert_eq!(compiled.topic_filter, "a/b/c");
        assert_eq!(compiled.aliased_topic_filter, "a/b/c");
        assert_eq!(compiled.select, SelectSpec::All);
        assert!(compiled.where_expr.is_none());
    }

    #[test]
    fn evaluate_where_true() {
        let rule = RuleDefinition {
            expression: "select * from data where temp > 25".to_string(),
            destinations: vec!["dest".to_string()],
        };
        let compiled = compile_rule(rule).expect("compile");
        let payload = serde_json::json!({"temp": 30, "hum": 10});
        let message = Message::new("data", serde_json::to_vec(&payload).unwrap());
        let evaluated = evaluate_rule(&compiled, &message);
        assert!(evaluated.is_some());
    }

    #[test]
    fn evaluate_where_false() {
        let rule = RuleDefinition {
            expression: "select * from data where temp > 25".to_string(),
            destinations: vec!["dest".to_string()],
        };
        let compiled = compile_rule(rule).expect("compile");
        let payload = serde_json::json!({"temp": 10});
        let message = Message::new("data", serde_json::to_vec(&payload).unwrap());
        let evaluated = evaluate_rule(&compiled, &message);
        assert!(evaluated.is_none());
    }

    #[test]
    fn evaluate_select_alias() {
        let rule = RuleDefinition {
            expression: "select height as h from data where temp > 20".to_string(),
            destinations: vec!["dest".to_string()],
        };
        let compiled = compile_rule(rule).expect("compile");
        let payload = serde_json::json!({"temp": 22, "height": 9});
        let message = Message::new("data", serde_json::to_vec(&payload).unwrap());
        let evaluated = evaluate_rule(&compiled, &message).expect("evaluated");
        let output: Value = serde_json::from_slice(&evaluated.payload).unwrap();
        assert_eq!(output["h"], Value::from(9));
    }

    #[test]
    fn evaluate_where_with_nested_payload_field() {
        let rule = RuleDefinition {
            expression: "select * from data where a.b.temp >= 20".to_string(),
            destinations: vec!["dest".to_string()],
        };
        let compiled = compile_rule(rule).expect("compile");

        let payload = serde_json::json!({"a": {"b": {"temp": 20}}, "c": "d"});
        let message = Message::new("data", serde_json::to_vec(&payload).unwrap());
        assert!(evaluate_rule(&compiled, &message).is_some());

        let payload = serde_json::json!({"a": {"b": {"temp": 19}}, "c": "d"});
        let message = Message::new("data", serde_json::to_vec(&payload).unwrap());
        assert!(evaluate_rule(&compiled, &message).is_none());
    }

    #[test]
    fn evaluate_select_nested_payload_field() {
        let rule = RuleDefinition {
            expression: "select a.b.temp as temperature, c from data where a.b.temp >= 20"
                .to_string(),
            destinations: vec!["dest".to_string()],
        };
        let compiled = compile_rule(rule).expect("compile");

        let payload = serde_json::json!({"a": {"b": {"temp": 20}}, "c": "d"});
        let message = Message::new("data", serde_json::to_vec(&payload).unwrap());
        let evaluated = evaluate_rule(&compiled, &message).expect("evaluated");
        let output: Value = serde_json::from_slice(&evaluated.payload).unwrap();
        assert_eq!(output["temperature"], Value::from(20));
        assert_eq!(output["c"], Value::from("d"));
    }

    #[test]
    fn derive_alias_from_nested_payload_field() {
        let rule = RuleDefinition {
            expression: "select a.b.temp from data where a.b.temp >= 20".to_string(),
            destinations: vec!["dest".to_string()],
        };
        let compiled = compile_rule(rule).expect("compile");

        let payload = serde_json::json!({"a": {"b": {"temp": 20}}});
        let message = Message::new("data", serde_json::to_vec(&payload).unwrap());
        let evaluated = evaluate_rule(&compiled, &message).expect("evaluated");
        let output: Value = serde_json::from_slice(&evaluated.payload).unwrap();
        assert_eq!(output["temp"], Value::from(20));
    }

    #[test]
    fn evaluate_arithmetic_expression() {
        let rule = RuleDefinition {
            expression: "select (height + 2) * 2 as h from data where temp >= 20"
                .to_string(),
            destinations: vec!["dest".to_string()],
        };
        let compiled = compile_rule(rule).expect("compile");
        let payload = serde_json::json!({"temp": 22, "height": 9});
        let message = Message::new("data", serde_json::to_vec(&payload).unwrap());
        let evaluated = evaluate_rule(&compiled, &message).expect("evaluated");
        let output: Value = serde_json::from_slice(&evaluated.payload).unwrap();
        assert_eq!(output["h"], Value::from(22.0));
    }

    #[test]
    fn match_topic_filter() {
        assert!(match_topic("a/+/c", "a/b/c"));
        assert!(match_topic("a/#", "a/b/c/d"));
        assert!(!match_topic("a/b", "a/b/c"));
    }

    #[test]
    fn where_with_and_or() {
        let rule = RuleDefinition {
            expression: "select * from data where temp > 20 and hum < 30".to_string(),
            destinations: vec!["dest".to_string()],
        };
        let compiled = compile_rule(rule).expect("compile");
        let payload = serde_json::json!({"temp": 25, "hum": 10});
        let message = Message::new("data", serde_json::to_vec(&payload).unwrap());
        assert!(evaluate_rule(&compiled, &message).is_some());

        let payload = serde_json::json!({"temp": 25, "hum": 50});
        let message = Message::new("data", serde_json::to_vec(&payload).unwrap());
        assert!(evaluate_rule(&compiled, &message).is_none());
    }

    #[test]
    fn parse_aliased_topic() {
        let rule = RuleDefinition {
            expression: "select * from data as source_data".to_string(),
            destinations: vec!["dest".to_string()],
        };
        let compiled = compile_rule(rule).expect("compile");
        assert_eq!(compiled.topic_filter, "data");
        assert_eq!(compiled.aliased_topic_filter, "source_data");
    }

    #[test]
    fn where_with_topic_level_function() {
        let rule = RuleDefinition {
            expression: "select * from 'sensors/+/temp' as t where topic_level(t, 2) = 'room1'"
                .to_string(),
            destinations: vec!["dest".to_string()],
        };
        let compiled = compile_rule(rule).expect("compile");
        let payload = serde_json::json!({"temp": 25});
        let message = Message::new("sensors/room1/temp", serde_json::to_vec(&payload).unwrap());
        assert!(evaluate_rule(&compiled, &message).is_some());

        let message = Message::new("sensors/room2/temp", serde_json::to_vec(&payload).unwrap());
        assert!(evaluate_rule(&compiled, &message).is_none());
    }

    #[test]
    fn where_with_metadata_and_properties() {
        let rule = RuleDefinition {
            expression: "select * from data where qos >= 1 and properties['content-type'] = 'application/json'"
                .to_string(),
            destinations: vec!["dest".to_string()],
        };
        let compiled = compile_rule(rule).expect("compile");
        let payload = serde_json::json!({"temp": 25});
        let mut message = Message::new("data", serde_json::to_vec(&payload).unwrap());
        message.qos = 1;
        message
            .properties
            .insert("content-type".to_string(), "application/json".to_string());
        assert!(evaluate_rule(&compiled, &message).is_some());

        message.qos = 0;
        assert!(evaluate_rule(&compiled, &message).is_none());
    }

    #[test]
    fn constant_folding_in_where_plan() {
        let rule = RuleDefinition {
            expression: "select * from data where temp > 25 + 3".to_string(),
            destinations: vec!["dest".to_string()],
        };
        let compiled = compile_rule(rule).expect("compile");

        let where_plan = compiled.where_plan.expect("where plan");
        match where_plan {
            EvalExprPlan::Binary { left: _, op: _, right } => {
                assert_eq!(*right, EvalExprPlan::Const(Value::from(28.0)));
            }
            _ => panic!("expected binary where plan"),
        }
    }

    #[test]
    fn constant_folding_full_boolean_expression() {
        let rule = RuleDefinition {
            expression: "select * from data where (25 + 3) = 28".to_string(),
            destinations: vec!["dest".to_string()],
        };
        let compiled = compile_rule(rule).expect("compile");
        assert_eq!(compiled.where_plan, Some(EvalExprPlan::Const(Value::Bool(true))));

        let payload = serde_json::json!({"temp": 10});
        let message = Message::new("data", serde_json::to_vec(&payload).unwrap());
        assert!(evaluate_rule(&compiled, &message).is_some());
    }

    #[test]
    fn compiles_fast_predicate_for_simple_compare() {
        let rule = RuleDefinition {
            expression: "select * from data where temp > 10".to_string(),
            destinations: vec!["dest".to_string()],
        };
        let compiled = compile_rule(rule).expect("compile");
        assert!(matches!(
            compiled.fast_where,
            Some(FastPredicate::Compare {
                field: FastField::Payload(_),
                op: FastCmpOp::Gt,
                value: FastValue::Number(_)
            })
        ));
    }

    #[test]
    fn fast_and_reorders_to_high_reject_side() {
        let rule = RuleDefinition {
            expression: "select * from data where a > 0 and b > 0".to_string(),
            destinations: vec!["dest".to_string()],
        };
        let compiled = compile_rule(rule).expect("compile");
        let payload = serde_json::json!({"a": 1, "b": 0});

        for _ in 0..64 {
            let message = Message::new("data", serde_json::to_vec(&payload).unwrap());
            assert!(evaluate_rule(&compiled, &message).is_none());
        }

        let profile = match compiled.fast_where.as_ref().expect("fast where") {
            FastPredicate::And { profile, .. } => profile,
            _ => panic!("expected fast AND predicate"),
        };
        let left_total = profile.left_true.load(Ordering::Relaxed)
            + profile.left_false.load(Ordering::Relaxed);
        let right_total = profile.right_true.load(Ordering::Relaxed)
            + profile.right_false.load(Ordering::Relaxed);

        assert!(right_total > left_total);
        assert!(profile.right_false.load(Ordering::Relaxed) > 0);
    }

    #[test]
    fn fast_path_downgrades_when_miss_ratio_is_high() {
        let rule = RuleDefinition {
            expression: "select * from data where temp > 10".to_string(),
            destinations: vec!["dest".to_string()],
        };
        let compiled = compile_rule(rule).expect("compile");
        let payload = serde_json::json!({"temp": "not-a-number"});

        for _ in 0..160 {
            let message = Message::new("data", serde_json::to_vec(&payload).unwrap());
            assert!(evaluate_rule(&compiled, &message).is_none());
        }

        assert!(!compiled.fast_path_profile.enabled.load(Ordering::Relaxed));
        assert!(compiled.fast_path_profile.downgrade_logged.load(Ordering::Relaxed));
    }
}
