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
    pub where_expr: Option<Expr>,
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
    let rule_id = format!("{:x}", fxhash::hash64(&rule.expression));
    Ok(CompiledRule {
        id: rule_id,
        topic_filter,
        aliased_topic_filter,
        select,
        where_expr,
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

pub fn evaluate_rule(rule: &CompiledRule, message: &Message) -> Option<Message> {
    let payload_value: Value = serde_json::from_slice(&message.payload).ok()?;
    let obj = payload_value.as_object()?;
    let context = EvalContext {
        message,
        payload: obj,
        rule_alias: &rule.aliased_topic_filter,
    };

    if let Some(expr) = &rule.where_expr {
        if !evaluate_where(expr, &context) {
            return None;
        }
    }

    match &rule.select {
        SelectSpec::All => Some(message.clone()),
        SelectSpec::Columns(columns) => {
            let mut output = serde_json::Map::with_capacity(columns.len());
            for col in columns {
                let value = evaluate_expr_value(&col.expr, &context).unwrap_or(Value::Null);
                output.insert(col.alias.clone(), value);
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

fn evaluate_where(expr: &Expr, ctx: &EvalContext) -> bool {
    evaluate_expr_bool(expr, ctx).unwrap_or(false)
}

fn extract_value(expr: &Expr, ctx: &EvalContext) -> Option<Value> {
    evaluate_expr_value(expr, ctx)
}

fn evaluate_expr_bool(expr: &Expr, ctx: &EvalContext) -> Option<bool> {
    match expr {
        Expr::BinaryOp { left, op, right } => {
            if matches!(op, BinaryOperator::And | BinaryOperator::Or) {
                let left_val = evaluate_expr_bool(left, ctx)?;
                let right_val = evaluate_expr_bool(right, ctx)?;
                return Some(match op {
                    BinaryOperator::And => left_val && right_val,
                    BinaryOperator::Or => left_val || right_val,
                    _ => false,
                });
            }

            let left_val = evaluate_expr_value(left, ctx)?;
            let right_val = evaluate_expr_value(right, ctx)?;
            compare_values(&left_val, &right_val, op)
        }
        Expr::Nested(inner) => evaluate_expr_bool(inner, ctx),
        Expr::Value(value) => match value {
            sqlparser::ast::Value::Boolean(b) => Some(*b),
            _ => None,
        },
        Expr::Identifier(Ident { value, .. }) => match value.as_str() {
            "retain" => Some(ctx.message.retain),
            "dup" => Some(ctx.message.dup),
            _ => ctx.payload.get(value).and_then(|v| v.as_bool()),
        },
        _ => None,
    }
}

fn evaluate_expr_value(expr: &Expr, ctx: &EvalContext) -> Option<Value> {
    match expr {
        Expr::Identifier(Ident { value, .. }) => match value.as_str() {
            "qos" => Some(Value::Number(serde_json::Number::from(ctx.message.qos))),
            "retain" => Some(Value::Bool(ctx.message.retain)),
            "dup" => Some(Value::Bool(ctx.message.dup)),
            "timestamp" => Some(Value::Number(serde_json::Number::from(
                ctx.message.timestamp_millis,
            ))),
            "clientId" => ctx
                .message
                .client_id
                .as_ref()
                .map(|v| Value::String(v.clone())),
            "username" => ctx
                .message
                .username
                .as_ref()
                .map(|v| Value::String(v.clone())),
            _ => ctx.payload.get(value).cloned(),
        },
        Expr::Value(value) => match value {
            sqlparser::ast::Value::Number(num, _) => num.parse::<f64>().ok().and_then(|v| {
                serde_json::Number::from_f64(v).map(Value::Number)
            }),
            sqlparser::ast::Value::SingleQuotedString(s) => Some(Value::String(s.clone())),
            sqlparser::ast::Value::Boolean(b) => Some(Value::Bool(*b)),
            _ => None,
        },
        Expr::BinaryOp { left, op, right } => {
            let left_val = evaluate_expr_value(left, ctx)?;
            let right_val = evaluate_expr_value(right, ctx)?;
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
                | BinaryOperator::NotEq => compare_values(&left_val, &right_val, op)
                    .map(Value::Bool),
                BinaryOperator::And | BinaryOperator::Or => evaluate_expr_bool(expr, ctx).map(Value::Bool),
                _ => None,
            }
        }
        Expr::Nested(inner) => evaluate_expr_value(inner, ctx),
        Expr::Function(func) => evaluate_function(func, ctx),
        Expr::MapAccess { column, keys } => {
            if let Expr::Identifier(Ident { value, .. }) = &**column {
                if value == "properties" {
                    if let Some(first_key) = keys.first() {
                        if let Some(key) = evaluate_expr_value(&first_key.key, ctx) {
                            if let Value::String(key) = key {
                                if let Some(val) = ctx.message.properties.get(&key) {
                                    return Some(Value::String(val.clone()));
                                }
                            }
                        }
                    }
                }
            }
            None
        }
        Expr::ArrayIndex { obj, indexes } => {
            if let Expr::Identifier(Ident { value, .. }) = &**obj {
                if value == "properties" {
                    if let Some(index_expr) = indexes.get(0) {
                        if let Some(key) = evaluate_expr_value(index_expr, ctx) {
                            if let Value::String(key) = key {
                                if let Some(val) = ctx.message.properties.get(&key) {
                                    return Some(Value::String(val.clone()));
                                }
                            }
                        }
                    }
                }
            }
            None
        }
        _ => None,
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

fn evaluate_function(func: &sqlparser::ast::Function, ctx: &EvalContext) -> Option<Value> {
    let name = func.name.to_string().to_lowercase();
    match name.as_str() {
        "topic_level" => {
            let args = match &func.args {
                FunctionArguments::List(list) => &list.args,
                _ => return None,
            };

            if args.len() != 2 {
                return None;
            }

            let level = match &args[1] {
                FunctionArg::Unnamed(arg_expr) => match arg_expr {
                    FunctionArgExpr::Expr(expr) => {
                        evaluate_expr_value(expr, ctx)?.as_u64()
                    }
                    _ => None,
                },
                _ => None,
            }?;
            let topic = ctx.message.topic.as_str();
            let parts: Vec<&str> = topic.split('/').collect();
            let idx = if level == 0 { 0 } else { (level - 1) as usize };
            parts.get(idx).map(|val| Value::String((*val).to_string()))
        }
        _ => None,
    }
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
