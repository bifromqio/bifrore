use prost_reflect::{
    DynamicMessage, MapKey as ProtobufMapKey, ReflectMessage, Value as ProtobufValue,
};
use serde_json::{Map, Number, Value as JsonValue};
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

    pub fn to_json_value(&self) -> JsonValue {
        match self {
            Self::Null => JsonValue::Null,
            Self::Bool(value) => JsonValue::Bool(*value),
            Self::Number(value) => {
                if value.is_finite() && value.fract() == 0.0 {
                    if *value >= 0.0 && *value <= u64::MAX as f64 {
                        return JsonValue::Number(Number::from(*value as u64));
                    }
                    if *value >= i64::MIN as f64 && *value <= i64::MAX as f64 {
                        return JsonValue::Number(Number::from(*value as i64));
                    }
                }
                Number::from_f64(*value)
                    .map(JsonValue::Number)
                    .unwrap_or(JsonValue::Null)
            }
            Self::String(value) => JsonValue::String(value.clone()),
            Self::Array(values) => {
                JsonValue::Array(values.iter().map(PayloadValue::to_json_value).collect())
            }
            Self::Object(entries) => JsonValue::Object(
                entries
                    .iter()
                    .map(|(key, value)| (key.clone(), value.to_json_value()))
                    .collect::<Map<String, JsonValue>>(),
            ),
        }
    }

    pub fn try_from_json_ref(value: &JsonValue) -> Result<Self, String> {
        match value {
            JsonValue::Null => Ok(Self::Null),
            JsonValue::Bool(flag) => Ok(Self::Bool(*flag)),
            JsonValue::Number(number) => number
                .as_f64()
                .map(Self::Number)
                .ok_or_else(|| "unsupported non-f64 JSON number".to_string()),
            JsonValue::String(text) => Ok(Self::String(text.clone())),
            JsonValue::Array(values) => values
                .iter()
                .map(PayloadValue::try_from_json_ref)
                .collect::<Result<Vec<_>, _>>()
                .map(Self::Array),
            JsonValue::Object(map) => map
                .iter()
                .map(|(key, value)| {
                    PayloadValue::try_from_json_ref(value).map(|value| (key.clone(), value))
                })
                .collect::<Result<Vec<_>, _>>()
                .map(Self::Object),
        }
    }

    pub fn try_from_protobuf_ref(value: &ProtobufValue) -> Result<Self, String> {
        match value {
            ProtobufValue::Bool(flag) => Ok(Self::Bool(*flag)),
            ProtobufValue::I32(number) => Ok(Self::Number(*number as f64)),
            ProtobufValue::I64(number) => Ok(Self::Number(*number as f64)),
            ProtobufValue::U32(number) => Ok(Self::Number(*number as f64)),
            ProtobufValue::U64(number) => Ok(Self::Number(*number as f64)),
            ProtobufValue::F32(number) => Ok(Self::Number(*number as f64)),
            ProtobufValue::F64(number) => Ok(Self::Number(*number)),
            ProtobufValue::String(text) => Ok(Self::String(text.clone())),
            ProtobufValue::Bytes(bytes) => Ok(Self::Array(
                bytes
                    .iter()
                    .map(|byte| PayloadValue::Number(*byte as f64))
                    .collect(),
            )),
            ProtobufValue::EnumNumber(number) => Ok(Self::Number(*number as f64)),
            ProtobufValue::Message(message) => message_to_payload_value(message),
            ProtobufValue::List(values) => values
                .iter()
                .map(PayloadValue::try_from_protobuf_ref)
                .collect::<Result<Vec<_>, _>>()
                .map(Self::Array),
            ProtobufValue::Map(entries) => entries
                .iter()
                .map(|(key, value)| {
                    PayloadValue::try_from_protobuf_ref(value)
                        .map(|value| (protobuf_map_key_to_string(key), value))
                })
                .collect::<Result<Vec<_>, _>>()
                .map(Self::Object),
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

impl TryFrom<JsonValue> for PayloadValue {
    type Error = String;

    fn try_from(value: JsonValue) -> Result<Self, Self::Error> {
        Self::try_from_json_ref(&value)
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

    pub fn from_json_object_ref(map: &Map<String, JsonValue>) -> Result<Self, String> {
        let mut ir = Self::new();
        for (key, value) in map {
            flatten_json_value_into_ir(&mut ir, key, value)?;
        }
        Ok(ir)
    }

    pub fn from_json_object_with_required_fields(
        map: &Map<String, JsonValue>,
        required_fields: Option<&HashSet<String>>,
    ) -> Result<Self, String> {
        match required_fields {
            Some(required_fields) if !required_fields.is_empty() => {
                let mut ir = Self::new();
                for key in required_fields {
                    if let Some(value) = lookup_json_path(map, key) {
                        ir.insert(key.clone(), PayloadValue::try_from_json_ref(value)?);
                    }
                }
                Ok(ir)
            }
            _ => Self::from_json_object_ref(map),
        }
    }

    pub fn from_protobuf_message_with_required_fields(
        message: &DynamicMessage,
        required_fields: Option<&HashSet<String>>,
    ) -> Result<Self, String> {
        match required_fields {
            Some(required_fields) if !required_fields.is_empty() => {
                let mut ir = Self::new();
                for key in required_fields {
                    if let Some(value) = extract_protobuf_payload_value(message, key)? {
                        ir.insert(key.clone(), value);
                    }
                }
                Ok(ir)
            }
            _ => {
                let mut ir = Self::new();
                flatten_dynamic_message_into_ir(&mut ir, message, None)?;
                Ok(ir)
            }
        }
    }
}

fn lookup_json_path<'a>(map: &'a Map<String, JsonValue>, key: &str) -> Option<&'a JsonValue> {
    let mut segments = key.split('.');
    let first = segments.next()?;
    let mut current = map.get(first)?;
    for segment in segments {
        current = current.get(segment)?;
    }
    Some(current)
}

fn flatten_json_value_into_ir(ir: &mut MsgIr, path: &str, value: &JsonValue) -> Result<(), String> {
    let payload = PayloadValue::try_from_json_ref(value)?;
    ir.insert(path.to_string(), payload.clone());
    if let PayloadValue::Object(entries) = payload {
        for (key, child) in entries {
            flatten_payload_value_into_ir(ir, &format!("{path}.{key}"), &child);
        }
    }
    Ok(())
}

fn flatten_dynamic_message_into_ir(
    ir: &mut MsgIr,
    message: &DynamicMessage,
    prefix: Option<&str>,
) -> Result<(), String> {
    for (field_desc, value) in message.fields() {
        let path = match prefix {
            Some(prefix) => format!("{prefix}.{}", field_desc.name()),
            None => field_desc.name().to_string(),
        };
        let payload = PayloadValue::try_from_protobuf_ref(value)?;
        ir.insert(path.clone(), payload.clone());
        if let PayloadValue::Object(entries) = payload {
            for (key, child) in entries {
                flatten_payload_value_into_ir(ir, &format!("{path}.{key}"), &child);
            }
        }
    }
    Ok(())
}

fn flatten_payload_value_into_ir(ir: &mut MsgIr, path: &str, value: &PayloadValue) {
    ir.insert(path.to_string(), value.clone());
    if let PayloadValue::Object(entries) = value {
        for (key, child) in entries {
            flatten_payload_value_into_ir(ir, &format!("{path}.{key}"), child);
        }
    }
}

fn extract_protobuf_payload_value(
    message: &DynamicMessage,
    key: &str,
) -> Result<Option<PayloadValue>, String> {
    let segments: Vec<&str> = key.split('.').collect();
    if segments.is_empty() {
        return Ok(None);
    }
    extract_protobuf_payload_value_from_segments(message, &segments)
}

fn extract_protobuf_payload_value_from_segments(
    message: &DynamicMessage,
    segments: &[&str],
) -> Result<Option<PayloadValue>, String> {
    let Some((segment, rest)) = segments.split_first() else {
        return Ok(None);
    };
    let Some(field_desc) = message.descriptor().get_field_by_name(segment) else {
        return Ok(None);
    };
    if !message.has_field(&field_desc) {
        return Ok(None);
    }
    let value = message.get_field(&field_desc).into_owned();
    if rest.is_empty() {
        return PayloadValue::try_from_protobuf_ref(&value).map(Some);
    }
    let ProtobufValue::Message(next_message) = value else {
        return Ok(None);
    };
    extract_protobuf_payload_value_from_segments(&next_message, rest)
}

fn message_to_payload_value(message: &DynamicMessage) -> Result<PayloadValue, String> {
    message
        .fields()
        .map(|(field_desc, value)| {
            PayloadValue::try_from_protobuf_ref(value).map(|value| (field_desc.name().to_string(), value))
        })
        .collect::<Result<Vec<_>, _>>()
        .map(PayloadValue::Object)
}

fn protobuf_map_key_to_string(key: &ProtobufMapKey) -> String {
    match key {
        ProtobufMapKey::Bool(value) => value.to_string(),
        ProtobufMapKey::I32(value) => value.to_string(),
        ProtobufMapKey::I64(value) => value.to_string(),
        ProtobufMapKey::U32(value) => value.to_string(),
        ProtobufMapKey::U64(value) => value.to_string(),
        ProtobufMapKey::String(value) => value.clone(),
    }
}
