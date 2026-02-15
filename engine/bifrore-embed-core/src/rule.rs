use crate::message::Message;
use regex::Regex;
use serde::{Deserialize, Serialize};
use serde_json::Value;
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompiledRule {
    pub id: String,
    pub topic_filter: String,
    pub aliased_topic_filter: String,
    pub select: SelectSpec,
    pub select_plan: SelectPlan,
    pub where_expr: Option<Expr>,
    pub where_plan: Option<EvalExprPlan>,
    pub destinations: Vec<String>,
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
        destinations: rule.destinations,
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
        Expr::BinaryOp { left, op, right } => EvalExprPlan::Binary {
            left: Box::new(compile_expr_plan(left)),
            op: op.clone(),
            right: Box::new(compile_expr_plan(right)),
        },
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

pub fn evaluate_rule(rule: &CompiledRule, message: &Message) -> Option<Message> {
    let payload_value: Value = serde_json::from_slice(&message.payload).ok()?;
    let obj = payload_value.as_object()?;
    evaluate_rule_with_payload(rule, message, obj)
}

pub(crate) fn evaluate_rule_with_payload(
    rule: &CompiledRule,
    message: &Message,
    payload_obj: &serde_json::Map<String, Value>,
) -> Option<Message> {
    let context = EvalContext {
        message,
        payload: payload_obj,
        rule_alias: &rule.aliased_topic_filter,
    };

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
    payload: &'a serde_json::Map<String, Value>,
    rule_alias: &'a str,
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
        _ => {
            let value = evaluate_plan_value(plan, ctx)?;
            match value {
                Value::Bool(v) => Some(v),
                _ => None,
            }
        }
    }
}

fn evaluate_plan_value(plan: &EvalExprPlan, ctx: &EvalContext) -> Option<Value> {
    match plan {
        EvalExprPlan::Const(value) => Some(value.clone()),
        EvalExprPlan::PayloadField(field) => ctx.payload.get(field).cloned(),
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
            let level_value = evaluate_plan_value(level, ctx)?.as_u64()?;
            let topic_parts: Vec<&str> = ctx.message.topic.split('/').collect();
            let index = if level_value == 0 {
                0
            } else {
                (level_value - 1) as usize
            };
            topic_parts
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

fn derive_alias(expr: &Expr) -> String {
    match expr {
        Expr::Identifier(Ident { value, .. }) => value.clone(),
        _ => expr.to_string(),
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
    let re = Regex::new(r"(?i)from\\s+'([^']+)'").expect("regex");
    re.replace(sql, r#"from "$1""#).to_string()
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
}
