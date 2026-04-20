use crate::message::Message;
use crate::metrics::{EvalMetrics, LatencyStage};
use crate::msg_ir::{CompiledPayloadField, MsgIr, PayloadValue};
#[cfg(test)]
use crate::payload::{decode_payload_ir, PayloadFormat};
use regex::Regex;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sqlparser::ast::{
    BinaryOperator, Expr, FunctionArg, FunctionArgExpr, FunctionArguments, Ident, Query, Select,
    SelectItem, SetExpr, Statement, TableFactor,
};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;
use std::collections::HashSet;

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
    pub destinations: Vec<String>,
    pub required_payload_fields: Vec<CompiledPayloadField>,
    pub requires_topic_parts: bool,
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
    PayloadField(CompiledPayloadField),
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

#[derive(Debug, thiserror::Error)]
pub enum RuleError {
    #[error("SQL parse error: {0}")]
    SqlParse(String),
    #[error("unsupported SQL form")]
    UnsupportedSql,
    #[error("missing topic filter in FROM")]
    MissingTopic,
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum EvalError {
    #[error("missing payload field `{field}`")]
    MissingPayloadField { field: String },
    #[error("missing message property `{key}`")]
    MissingProperty { key: String },
    #[error("missing message metadata `{name}`")]
    MissingMetadata { name: &'static str },
    #[error("type mismatch in {context}: expected {expected}, got {actual}")]
    TypeMismatch {
        context: &'static str,
        expected: &'static str,
        actual: &'static str,
    },
    #[error("type mismatch in {context}: operator {op} cannot apply to {left} and {right}")]
    BinaryTypeMismatch {
        context: &'static str,
        op: &'static str,
        left: &'static str,
        right: &'static str,
    },
    #[error("invalid topic level value in {context}: expected non-negative integer, got {actual}")]
    InvalidTopicLevelValue {
        context: &'static str,
        actual: &'static str,
    },
    #[error("unsupported expression in {context}")]
    UnsupportedExpression { context: &'static str },
    #[error("failed to serialize projected payload")]
    ProjectionSerialization,
}

impl EvalError {
    pub fn is_type_error(&self) -> bool {
        matches!(
            self,
            EvalError::TypeMismatch { .. }
                | EvalError::BinaryTypeMismatch { .. }
                | EvalError::InvalidTopicLevelValue { .. }
        )
    }
}

pub fn compile_rule(rule: RuleDefinition) -> Result<CompiledRule, RuleError> {
    let dialect = GenericDialect {};
    let normalized_sql = normalize_rule_sql(&rule.expression);
    let mut statements = Parser::parse_sql(&dialect, &normalized_sql)
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
    let required_payload_fields =
        collect_required_payload_fields(&select_plan, where_plan.as_ref());
    let requires_topic_parts = select_plan_requires_topic_parts(&select_plan)
        || where_plan
            .as_ref()
            .map(plan_requires_topic_parts)
            .unwrap_or(false);
    let has_unknown_plan = select_plan_has_unknown(&select_plan)
        || where_plan.as_ref().map(plan_has_unknown).unwrap_or(false);
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
        destinations: rule.destinations,
        required_payload_fields,
        requires_topic_parts,
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

fn parse_select(select: Select) -> Result<(String, String, (SelectSpec, Option<Expr>)), RuleError> {
    let from = select.from.get(0).ok_or(RuleError::MissingTopic)?;
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
            _ => EvalExprPlan::PayloadField(CompiledPayloadField::from_key(value.clone())),
        },
        Expr::CompoundIdentifier(idents) => {
            EvalExprPlan::PayloadField(CompiledPayloadField::from_key(
                idents
                    .iter()
                    .map(|ident| ident.value.as_str())
                    .collect::<Vec<_>>()
                    .join("."),
            ))
        }
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
                        if let FunctionArg::Unnamed(FunctionArgExpr::Expr(level_expr)) =
                            &list.args[1]
                        {
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

fn fold_const_binary(
    left: &EvalExprPlan,
    op: &BinaryOperator,
    right: &EvalExprPlan,
) -> Option<Value> {
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
) -> Vec<CompiledPayloadField> {
    let mut fields = Vec::new();
    let mut seen = HashSet::new();
    collect_required_payload_fields_from_select(select_plan, &mut fields, &mut seen);
    if let Some(where_plan) = where_plan {
        collect_required_payload_fields_from_plan(where_plan, &mut fields, &mut seen);
    }
    fields
}

fn select_plan_requires_topic_parts(plan: &SelectPlan) -> bool {
    match plan {
        SelectPlan::All => false,
        SelectPlan::Columns(columns) => columns
            .iter()
            .any(|column| plan_requires_topic_parts(&column.expr)),
    }
}

fn plan_requires_topic_parts(plan: &EvalExprPlan) -> bool {
    match plan {
        EvalExprPlan::TopicLevel { .. } => true,
        EvalExprPlan::Binary { left, right, .. } => {
            plan_requires_topic_parts(left) || plan_requires_topic_parts(right)
        }
        EvalExprPlan::PropertiesKey { key } => plan_requires_topic_parts(key),
        _ => false,
    }
}

fn collect_required_payload_fields_from_select(
    select_plan: &SelectPlan,
    fields: &mut Vec<CompiledPayloadField>,
    seen: &mut HashSet<String>,
) {
    match select_plan {
        SelectPlan::All => {}
        SelectPlan::Columns(columns) => {
            for column in columns {
                collect_required_payload_fields_from_plan(&column.expr, fields, seen);
            }
        }
    }
}

fn collect_required_payload_fields_from_plan(
    plan: &EvalExprPlan,
    fields: &mut Vec<CompiledPayloadField>,
    seen: &mut HashSet<String>,
) {
    match plan {
        EvalExprPlan::PayloadField(path) => {
            if seen.insert(path.key().to_string()) {
                fields.push(path.clone());
            }
        }
        EvalExprPlan::Binary { left, right, .. } => {
            collect_required_payload_fields_from_plan(left, fields, seen);
            collect_required_payload_fields_from_plan(right, fields, seen);
        }
        EvalExprPlan::TopicLevel { level } => {
            collect_required_payload_fields_from_plan(level, fields, seen);
        }
        EvalExprPlan::PropertiesKey { key } => {
            collect_required_payload_fields_from_plan(key, fields, seen);
        }
        _ => {}
    }
}

pub(crate) fn evaluate_rule_with_payload_and_topic_parts(
    rule: &CompiledRule,
    message: &Message,
    payload_obj: &MsgIr,
    topic_parts: &[&str],
    metrics: &EvalMetrics,
) -> Result<Option<Message>, EvalError> {
    let context = EvalContext {
        message,
        payload: payload_obj,
        topic_parts,
    };

    if let Some(plan) = &rule.where_plan {
        let predicate_timer = metrics.start_stage();
        let predicate_result = evaluate_plan_bool(plan, &context)?;
        metrics.finish_stage(LatencyStage::Predicate, predicate_timer);
        if !predicate_result {
            return Ok(None);
        }
    }

    match &rule.select_plan {
        SelectPlan::All => Ok(Some(message.clone())),
        SelectPlan::Columns(columns) => {
            let projection_timer = metrics.start_stage();
            let mut output = serde_json::Map::with_capacity(columns.len());
            for column in columns {
                let value = evaluate_plan_value(&column.expr, &context)?;
                output.insert(column.alias.clone(), value);
            }
            let new_payload = serde_json::to_vec(&Value::Object(output))
                .map_err(|_| EvalError::ProjectionSerialization)?;
            metrics.finish_stage(LatencyStage::Projection, projection_timer);
            Ok(Some(Message::new(&message.topic, new_payload)))
        }
    }
}

struct EvalContext<'a> {
    message: &'a Message,
    payload: &'a MsgIr,
    topic_parts: &'a [&'a str],
}

fn evaluate_plan_bool(plan: &EvalExprPlan, ctx: &EvalContext) -> Result<bool, EvalError> {
    match plan {
        EvalExprPlan::Binary { left, op, right }
            if matches!(op, BinaryOperator::And | BinaryOperator::Or) =>
        {
            let left_val = evaluate_plan_bool(left, ctx)?;
            let right_val = evaluate_plan_bool(right, ctx)?;
            Ok(match op {
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
            if let Some(value) = evaluate_plan_compare_bool(left, op, right, ctx)? {
                return Ok(value);
            }
            let left_val = evaluate_plan_value(left, ctx)?;
            let right_val = evaluate_plan_value(right, ctx)?;
            compare_values_runtime(&left_val, &right_val, op, "predicate")
        }
        _ => {
            let value = evaluate_plan_value(plan, ctx)?;
            match value {
                Value::Bool(v) => Ok(v),
                _ => Err(EvalError::TypeMismatch {
                    context: "predicate",
                    expected: "bool",
                    actual: json_value_type_name(&value),
                }),
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

fn evaluate_plan_compare_bool(
    left: &EvalExprPlan,
    op: &BinaryOperator,
    right: &EvalExprPlan,
    ctx: &EvalContext,
) -> Result<Option<bool>, EvalError> {
    let Some(left_val) = evaluate_plan_runtime_value(left, ctx)? else {
        return Ok(None);
    };
    let Some(right_val) = evaluate_plan_runtime_value(right, ctx)? else {
        return Ok(None);
    };
    compare_runtime_runtime(&left_val, &right_val, op, "predicate").map(Some)
}

fn evaluate_plan_runtime_value<'a>(
    plan: &'a EvalExprPlan,
    ctx: &'a EvalContext,
) -> Result<Option<RuntimeValue<'a>>, EvalError> {
    match plan {
        EvalExprPlan::Const(value) => match value {
            Value::Number(number) => Ok(number.as_f64().map(RuntimeValue::Number)),
            Value::String(text) => Ok(Some(RuntimeValue::String(text))),
            Value::Bool(flag) => Ok(Some(RuntimeValue::Bool(*flag))),
            _ => Ok(None),
        },
        EvalExprPlan::PayloadField(path) => Ok(Some(match payload_value_by_path(ctx.payload, path)? {
            PayloadValue::Number(number) => RuntimeValue::Number(*number),
            PayloadValue::String(text) => RuntimeValue::String(text),
            PayloadValue::Bool(flag) => RuntimeValue::Bool(*flag),
            value => {
                return Err(EvalError::TypeMismatch {
                    context: "predicate",
                    expected: "number|string|bool",
                    actual: payload_value_type_name(value),
                })
            }
        })),
        EvalExprPlan::MetaQos => Ok(Some(RuntimeValue::Number(ctx.message.qos as f64))),
        EvalExprPlan::MetaRetain => Ok(Some(RuntimeValue::Bool(ctx.message.retain))),
        EvalExprPlan::MetaDup => Ok(Some(RuntimeValue::Bool(ctx.message.dup))),
        EvalExprPlan::MetaTimestamp => Ok(Some(RuntimeValue::Number(
            ctx.message.timestamp_millis as f64,
        ))),
        EvalExprPlan::MetaClientId => ctx
            .message
            .client_id
            .as_ref()
            .map(|value| RuntimeValue::String(value))
            .map(Some)
            .ok_or(EvalError::MissingMetadata { name: "clientId" }),
        EvalExprPlan::MetaUsername => ctx
            .message
            .username
            .as_ref()
            .map(|value| RuntimeValue::String(value))
            .map(Some)
            .ok_or(EvalError::MissingMetadata { name: "username" }),
        EvalExprPlan::TopicLevel { level } => {
            let level_value =
                value_to_non_negative_u64(&evaluate_plan_value(level, ctx)?, "topic_level")?;
            let index = if level_value == 0 {
                0
            } else {
                (level_value - 1) as usize
            };
            ctx.topic_parts
                .get(index)
                .copied()
                .map(RuntimeValue::String)
                .map(Some)
                .ok_or(EvalError::MissingMetadata { name: "topic level" })
        }
        EvalExprPlan::PropertiesKey { key } => {
            let key_value = evaluate_plan_value(key, ctx)?;
            let key = key_value.as_str().ok_or(EvalError::TypeMismatch {
                context: "properties key",
                expected: "string",
                actual: json_value_type_name(&key_value),
            })?;
            ctx.message
                .properties
                .get(key)
                .map(|value| RuntimeValue::String(value))
                .map(Some)
                .ok_or_else(|| EvalError::MissingProperty {
                    key: key.to_string(),
                })
        }
        EvalExprPlan::Binary { .. } | EvalExprPlan::Unknown(_) => Ok(None),
    }
}

fn compare_runtime_runtime(
    left: &RuntimeValue,
    right: &RuntimeValue,
    op: &BinaryOperator,
    context: &'static str,
) -> Result<bool, EvalError> {
    match (left, right) {
        (RuntimeValue::Number(l), RuntimeValue::Number(r)) => Ok(match op {
            BinaryOperator::Gt => l > r,
            BinaryOperator::GtEq => l >= r,
            BinaryOperator::Lt => l < r,
            BinaryOperator::LtEq => l <= r,
            BinaryOperator::Eq => (l - r).abs() < f64::EPSILON,
            BinaryOperator::NotEq => (l - r).abs() >= f64::EPSILON,
            _ => return Err(EvalError::UnsupportedExpression { context }),
        }),
        (RuntimeValue::String(l), RuntimeValue::String(r)) => Ok(match op {
            BinaryOperator::Eq => l == r,
            BinaryOperator::NotEq => l != r,
            _ => {
                return Err(EvalError::BinaryTypeMismatch {
                    context,
                    op: binary_operator_name(op),
                    left: left.kind(),
                    right: right.kind(),
                })
            }
        }),
        (RuntimeValue::Bool(l), RuntimeValue::Bool(r)) => Ok(match op {
            BinaryOperator::Eq => l == r,
            BinaryOperator::NotEq => l != r,
            _ => {
                return Err(EvalError::BinaryTypeMismatch {
                    context,
                    op: binary_operator_name(op),
                    left: left.kind(),
                    right: right.kind(),
                })
            }
        }),
        _ => Err(EvalError::BinaryTypeMismatch {
            context,
            op: binary_operator_name(op),
            left: left.kind(),
            right: right.kind(),
        }),
    }
}

fn evaluate_plan_value(plan: &EvalExprPlan, ctx: &EvalContext) -> Result<Value, EvalError> {
    match plan {
        EvalExprPlan::Const(value) => Ok(value.clone()),
        EvalExprPlan::PayloadField(path) => Ok(payload_value_by_path(ctx.payload, path)?.to_json_value()),
        EvalExprPlan::MetaQos => Ok(Value::Number(serde_json::Number::from(ctx.message.qos))),
        EvalExprPlan::MetaRetain => Ok(Value::Bool(ctx.message.retain)),
        EvalExprPlan::MetaDup => Ok(Value::Bool(ctx.message.dup)),
        EvalExprPlan::MetaTimestamp => Ok(Value::Number(serde_json::Number::from(
            ctx.message.timestamp_millis,
        ))),
        EvalExprPlan::MetaClientId => ctx
            .message
            .client_id
            .as_ref()
            .map(|value| Value::String(value.clone()))
            .ok_or(EvalError::MissingMetadata { name: "clientId" }),
        EvalExprPlan::MetaUsername => ctx
            .message
            .username
            .as_ref()
            .map(|value| Value::String(value.clone()))
            .ok_or(EvalError::MissingMetadata { name: "username" }),
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
                | BinaryOperator::Divide => {
                    evaluate_arithmetic_runtime(&left_val, &right_val, op, "projection")
                }
                BinaryOperator::Gt
                | BinaryOperator::GtEq
                | BinaryOperator::Lt
                | BinaryOperator::LtEq
                | BinaryOperator::Eq
                | BinaryOperator::NotEq => {
                    compare_values_runtime(&left_val, &right_val, op, "projection")
                        .map(Value::Bool)
                }
                _ => Err(EvalError::UnsupportedExpression {
                    context: "projection",
                }),
            }
        }
        EvalExprPlan::TopicLevel { level } => {
            let level_value =
                value_to_non_negative_u64(&evaluate_plan_value(level, ctx)?, "topic_level")?;
            let index = if level_value == 0 {
                0
            } else {
                (level_value - 1) as usize
            };
            ctx.topic_parts
                .get(index)
                .map(|value| Value::String((*value).to_string()))
                .ok_or(EvalError::MissingMetadata { name: "topic level" })
        }
        EvalExprPlan::PropertiesKey { key } => {
            let key_value = evaluate_plan_value(key, ctx)?;
            let key = key_value.as_str().ok_or(EvalError::TypeMismatch {
                context: "properties key",
                expected: "string",
                actual: json_value_type_name(&key_value),
            })?;
            ctx.message
                .properties
                .get(key)
                .map(|value| Value::String(value.clone()))
                .ok_or_else(|| EvalError::MissingProperty {
                    key: key.to_string(),
                })
        }
        EvalExprPlan::Unknown(_) => Err(EvalError::UnsupportedExpression {
            context: "rule expression",
        }),
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

fn evaluate_arithmetic_runtime(
    left: &Value,
    right: &Value,
    op: &BinaryOperator,
    context: &'static str,
) -> Result<Value, EvalError> {
    let left_number = left.as_f64().ok_or(EvalError::BinaryTypeMismatch {
        context,
        op: binary_operator_name(op),
        left: json_value_type_name(left),
        right: json_value_type_name(right),
    })?;
    let right_number = right.as_f64().ok_or(EvalError::BinaryTypeMismatch {
        context,
        op: binary_operator_name(op),
        left: json_value_type_name(left),
        right: json_value_type_name(right),
    })?;
    let result = match op {
        BinaryOperator::Plus => left_number + right_number,
        BinaryOperator::Minus => left_number - right_number,
        BinaryOperator::Multiply => left_number * right_number,
        BinaryOperator::Divide => left_number / right_number,
        _ => {
            return Err(EvalError::UnsupportedExpression { context });
        }
    };
    serde_json::Number::from_f64(result)
        .map(Value::Number)
        .ok_or(EvalError::UnsupportedExpression { context })
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

fn compare_values_runtime(
    left: &Value,
    right: &Value,
    op: &BinaryOperator,
    context: &'static str,
) -> Result<bool, EvalError> {
    match (left, right) {
        (Value::Number(l), Value::Number(r)) => {
            let l = l.as_f64().ok_or(EvalError::BinaryTypeMismatch {
                context,
                op: binary_operator_name(op),
                left: json_value_type_name(left),
                right: json_value_type_name(right),
            })?;
            let r = r.as_f64().ok_or(EvalError::BinaryTypeMismatch {
                context,
                op: binary_operator_name(op),
                left: json_value_type_name(left),
                right: json_value_type_name(right),
            })?;
            Ok(match op {
                BinaryOperator::Gt => l > r,
                BinaryOperator::GtEq => l >= r,
                BinaryOperator::Lt => l < r,
                BinaryOperator::LtEq => l <= r,
                BinaryOperator::Eq => (l - r).abs() < f64::EPSILON,
                BinaryOperator::NotEq => (l - r).abs() >= f64::EPSILON,
                _ => return Err(EvalError::UnsupportedExpression { context }),
            })
        }
        (Value::String(l), Value::String(r)) => Ok(match op {
            BinaryOperator::Eq => l == r,
            BinaryOperator::NotEq => l != r,
            _ => {
                return Err(EvalError::BinaryTypeMismatch {
                    context,
                    op: binary_operator_name(op),
                    left: json_value_type_name(left),
                    right: json_value_type_name(right),
                })
            }
        }),
        (Value::Bool(l), Value::Bool(r)) => Ok(match op {
            BinaryOperator::Eq => l == r,
            BinaryOperator::NotEq => l != r,
            _ => {
                return Err(EvalError::BinaryTypeMismatch {
                    context,
                    op: binary_operator_name(op),
                    left: json_value_type_name(left),
                    right: json_value_type_name(right),
                })
            }
        }),
        _ => Err(EvalError::BinaryTypeMismatch {
            context,
            op: binary_operator_name(op),
            left: json_value_type_name(left),
            right: json_value_type_name(right),
        }),
    }
}

fn value_to_non_negative_u64(value: &Value, context: &'static str) -> Result<u64, EvalError> {
    if let Some(raw) = value.as_u64() {
        return Ok(raw);
    }
    let number = value.as_f64().ok_or(EvalError::InvalidTopicLevelValue {
        context,
        actual: json_value_type_name(value),
    })?;
    if number < 0.0 || number.fract() != 0.0 || number > u64::MAX as f64 {
        return Err(EvalError::InvalidTopicLevelValue {
            context,
            actual: json_value_type_name(value),
        });
    }
    Ok(number as u64)
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

fn payload_value_by_path<'a>(
    payload: &'a MsgIr,
    path: &CompiledPayloadField,
) -> Result<&'a PayloadValue, EvalError> {
    payload
        .get_field(path)
        .ok_or_else(|| EvalError::MissingPayloadField {
            field: path.key().to_string(),
        })
}

impl RuntimeValue<'_> {
    fn kind(&self) -> &'static str {
        match self {
            RuntimeValue::Number(_) => "number",
            RuntimeValue::String(_) => "string",
            RuntimeValue::Bool(_) => "bool",
        }
    }
}

fn json_value_type_name(value: &Value) -> &'static str {
    match value {
        Value::Null => "null",
        Value::Bool(_) => "bool",
        Value::Number(_) => "number",
        Value::String(_) => "string",
        Value::Array(_) => "array",
        Value::Object(_) => "object",
    }
}

fn payload_value_type_name(value: &PayloadValue) -> &'static str {
    match value {
        PayloadValue::Null => "null",
        PayloadValue::Bool(_) => "bool",
        PayloadValue::Number(_) => "number",
        PayloadValue::String(_) => "string",
        PayloadValue::Array(_) => "array",
        PayloadValue::Object(_) => "object",
    }
}

fn binary_operator_name(op: &BinaryOperator) -> &'static str {
    match op {
        BinaryOperator::Plus => "+",
        BinaryOperator::Minus => "-",
        BinaryOperator::Multiply => "*",
        BinaryOperator::Divide => "/",
        BinaryOperator::Gt => ">",
        BinaryOperator::GtEq => ">=",
        BinaryOperator::Lt => "<",
        BinaryOperator::LtEq => "<=",
        BinaryOperator::Eq => "=",
        BinaryOperator::NotEq => "!=",
        BinaryOperator::And => "and",
        BinaryOperator::Or => "or",
        _ => "unknown",
    }
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
        evaluate_rule_with_payload_and_topic_parts(
            rule,
            message,
            &obj,
            &topic_parts,
            &EvalMetrics::default(),
        )
        .expect("evaluation should not error")
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
            expression: "select (height + 2) * 2 as h from data where temp >= 20".to_string(),
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
        assert!(!compiled.requires_topic_parts);
    }

    #[test]
    fn where_with_topic_level_function() {
        let rule = RuleDefinition {
            expression: "select * from 'sensors/+/temp' as t where topic_level(t, 2) = 'room1'"
                .to_string(),
            destinations: vec!["dest".to_string()],
        };
        let compiled = compile_rule(rule).expect("compile");
        assert!(compiled.requires_topic_parts);
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
            EvalExprPlan::Binary {
                left: _,
                op: _,
                right,
            } => {
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
        assert_eq!(
            compiled.where_plan,
            Some(EvalExprPlan::Const(Value::Bool(true)))
        );

        let payload = serde_json::json!({"temp": 10});
        let message = Message::new("data", serde_json::to_vec(&payload).unwrap());
        assert!(evaluate_rule(&compiled, &message).is_some());
    }
}
