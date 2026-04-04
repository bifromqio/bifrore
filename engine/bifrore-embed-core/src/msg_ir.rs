use serde_json::{Map, Number, Value};
use std::collections::{HashMap, HashSet};

#[derive(Debug, Clone, PartialEq)]
pub enum PayloadValue {
    Null,
    Bool(bool),
    Number(f64),
    String(String),
    Array(Vec<PayloadValue>),
    Object(Vec<(String, PayloadValue)>),
}

impl PayloadValue {
    pub fn as_f64(&self) -> Option<f64> {
        match self {
            Self::Number(value) => Some(*value),
            _ => None,
        }
    }

    pub fn as_str(&self) -> Option<&str> {
        match self {
            Self::String(value) => Some(value),
            _ => None,
        }
    }

    pub fn to_json_value(&self) -> Value {
        match self {
            Self::Null => Value::Null,
            Self::Bool(value) => Value::Bool(*value),
            Self::Number(value) => {
                if value.is_finite() && value.fract() == 0.0 {
                    if *value >= 0.0 && *value <= u64::MAX as f64 {
                        return Value::Number(Number::from(*value as u64));
                    }
                    if *value >= i64::MIN as f64 && *value <= i64::MAX as f64 {
                        return Value::Number(Number::from(*value as i64));
                    }
                }
                Number::from_f64(*value)
                    .map(Value::Number)
                    .unwrap_or(Value::Null)
            }
            Self::String(value) => Value::String(value.clone()),
            Self::Array(values) => {
                Value::Array(values.iter().map(PayloadValue::to_json_value).collect())
            }
            Self::Object(entries) => Value::Object(
                entries
                    .iter()
                    .map(|(key, value)| (key.clone(), value.to_json_value()))
                    .collect::<Map<String, Value>>(),
            ),
        }
    }
}

impl From<bool> for PayloadValue {
    fn from(value: bool) -> Self {
        Self::Bool(value)
    }
}

impl From<f64> for PayloadValue {
    fn from(value: f64) -> Self {
        Self::Number(value)
    }
}

impl From<&str> for PayloadValue {
    fn from(value: &str) -> Self {
        Self::String(value.to_string())
    }
}

impl From<String> for PayloadValue {
    fn from(value: String) -> Self {
        Self::String(value)
    }
}

impl TryFrom<Value> for PayloadValue {
    type Error = String;

    fn try_from(value: Value) -> Result<Self, Self::Error> {
        match value {
            Value::Null => Ok(Self::Null),
            Value::Bool(flag) => Ok(Self::Bool(flag)),
            Value::Number(number) => number
                .as_f64()
                .map(Self::Number)
                .ok_or_else(|| "unsupported non-f64 JSON number".to_string()),
            Value::String(text) => Ok(Self::String(text)),
            Value::Array(values) => values
                .into_iter()
                .map(PayloadValue::try_from)
                .collect::<Result<Vec<_>, _>>()
                .map(Self::Array),
            Value::Object(map) => map
                .into_iter()
                .map(|(key, value)| PayloadValue::try_from(value).map(|value| (key, value)))
                .collect::<Result<Vec<_>, _>>()
                .map(Self::Object),
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq)]
pub struct MsgIr {
    fields: HashMap<String, PayloadValue>,
}

impl MsgIr {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn insert(&mut self, path: impl Into<String>, value: PayloadValue) {
        self.fields.insert(path.into(), value);
    }

    pub fn get_key(&self, key: &str) -> Option<&PayloadValue> {
        self.fields.get(key)
    }

    pub fn retain_fields(&mut self, required_fields: &HashSet<String>) {
        self.fields
            .retain(|key, _| required_fields.contains(key));
    }

    pub fn from_json_object(map: Map<String, Value>) -> Result<Self, String> {
        let root = PayloadValue::Object(
            map.into_iter()
                .map(|(key, value)| PayloadValue::try_from(value).map(|value| (key, value)))
                .collect::<Result<Vec<_>, _>>()?,
        );
        Self::from_root_object(root)
    }

    pub fn from_root_object(root: PayloadValue) -> Result<Self, String> {
        let PayloadValue::Object(entries) = root else {
            return Err("payload must decode to an object".to_string());
        };
        let mut ir = Self::new();
        for (key, value) in entries {
            ir.flatten_insert(&key, &value);
        }
        Ok(ir)
    }

    fn flatten_insert(&mut self, path: &str, value: &PayloadValue) {
        self.fields.insert(path.to_string(), value.clone());
        if let PayloadValue::Object(entries) = value {
            for (key, child) in entries {
                self.flatten_insert(&format!("{path}.{key}"), child);
            }
        }
    }

    pub fn from_json_object_with_required_fields(
        map: &Map<String, Value>,
        required_fields: Option<&HashSet<String>>,
    ) -> Result<Self, String> {
        match required_fields {
            Some(required_fields) if !required_fields.is_empty() => {
                let mut ir = Self::new();
                for key in required_fields {
                    if let Some(value) = lookup_json_path(map, key) {
                        ir.insert(key.clone(), PayloadValue::try_from(value.clone())?);
                    }
                }
                Ok(ir)
            }
            _ => Self::from_json_object(map.clone()),
        }
    }

    pub fn from_protobuf_struct_with_required_fields(
        fields: &[(String, PayloadValue)],
        required_fields: Option<&HashSet<String>>,
    ) -> Self {
        match required_fields {
            Some(required_fields) if !required_fields.is_empty() => {
                let mut ir = Self::new();
                for key in required_fields {
                    if let Some(value) = lookup_payload_path(fields, key) {
                        ir.insert(key.clone(), value.clone());
                    }
                }
                ir
            }
            _ => {
                let mut ir = Self::new();
                for (key, value) in fields {
                    ir.flatten_insert(key, value);
                }
                ir
            }
        }
    }
}

fn lookup_json_path<'a>(map: &'a Map<String, Value>, key: &str) -> Option<&'a Value> {
    let mut segments = key.split('.');
    let first = segments.next()?;
    let mut current = map.get(first)?;
    for segment in segments {
        current = current.get(segment)?;
    }
    Some(current)
}

fn lookup_payload_path<'a>(fields: &'a [(String, PayloadValue)], key: &str) -> Option<&'a PayloadValue> {
    let mut segments = key.split('.');
    let first = segments.next()?;
    let mut current = fields
        .iter()
        .find(|(name, _)| name == first)
        .map(|(_, value)| value)?;
    for segment in segments {
        let PayloadValue::Object(entries) = current else {
            return None;
        };
        current = entries
            .iter()
            .find(|(name, _)| name == segment)
            .map(|(_, value)| value)?;
    }
    Some(current)
}
