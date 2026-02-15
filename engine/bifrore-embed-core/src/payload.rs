use prost::Message;
use prost_types::{value::Kind, ListValue, Struct, Value as ProstValue};
use serde_json::{Map, Number, Value};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum PayloadFormat {
    #[default]
    Json,
    Protobuf,
}

impl PayloadFormat {
    pub fn from_ffi_code(code: i32) -> Option<Self> {
        match code {
            1 => Some(Self::Json),
            2 => Some(Self::Protobuf),
            _ => None,
        }
    }
}

pub fn decode_payload_object(
    payload: &[u8],
    format: PayloadFormat,
) -> Result<Map<String, Value>, String> {
    match format {
        PayloadFormat::Json => decode_json_object(payload),
        PayloadFormat::Protobuf => decode_protobuf_struct(payload),
    }
}

fn decode_json_object(payload: &[u8]) -> Result<Map<String, Value>, String> {
    #[cfg(feature = "simd-json")]
    let parsed: Value = {
        let mut buffer = payload.to_vec();
        simd_json::serde::from_slice(&mut buffer).map_err(|err| err.to_string())?
    };

    #[cfg(not(feature = "simd-json"))]
    let parsed: Value = serde_json::from_slice(payload).map_err(|err| err.to_string())?;

    parsed
        .as_object()
        .cloned()
        .ok_or_else(|| "payload must be a JSON object".to_string())
}

fn decode_protobuf_struct(payload: &[u8]) -> Result<Map<String, Value>, String> {
    let parsed = Struct::decode(payload).map_err(|err| err.to_string())?;
    Ok(protobuf_fields_to_json_object(parsed.fields))
}

fn protobuf_fields_to_json_object(
    fields: std::collections::BTreeMap<String, ProstValue>,
) -> Map<String, Value> {
    let mut output = Map::with_capacity(fields.len());
    for (key, value) in fields {
        output.insert(key, protobuf_value_to_json(value));
    }
    output
}

fn protobuf_value_to_json(value: ProstValue) -> Value {
    match value.kind {
        Some(Kind::NullValue(_)) | None => Value::Null,
        Some(Kind::NumberValue(num)) => Number::from_f64(num)
            .map(Value::Number)
            .unwrap_or(Value::Null),
        Some(Kind::StringValue(text)) => Value::String(text),
        Some(Kind::BoolValue(flag)) => Value::Bool(flag),
        Some(Kind::StructValue(struct_value)) => {
            Value::Object(protobuf_fields_to_json_object(struct_value.fields))
        }
        Some(Kind::ListValue(list)) => Value::Array(protobuf_list_to_json(list)),
    }
}

fn protobuf_list_to_json(list: ListValue) -> Vec<Value> {
    list.values
        .into_iter()
        .map(protobuf_value_to_json)
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decode_protobuf_struct_with_scalar_fields() {
        let mut fields = std::collections::BTreeMap::new();
        fields.insert(
            "temp".to_string(),
            ProstValue {
                kind: Some(Kind::NumberValue(30.0)),
            },
        );
        fields.insert(
            "online".to_string(),
            ProstValue {
                kind: Some(Kind::BoolValue(true)),
            },
        );
        fields.insert(
            "device".to_string(),
            ProstValue {
                kind: Some(Kind::StringValue("sensor-a".to_string())),
            },
        );

        let payload = Struct { fields }.encode_to_vec();
        let decoded =
            decode_payload_object(&payload, PayloadFormat::Protobuf).expect("decode protobuf");
        assert_eq!(decoded.get("temp"), Some(&Value::from(30.0)));
        assert_eq!(decoded.get("online"), Some(&Value::from(true)));
        assert_eq!(decoded.get("device"), Some(&Value::from("sensor-a")));
    }

    #[test]
    fn decode_protobuf_struct_with_nested_and_list() {
        let mut nested_fields = std::collections::BTreeMap::new();
        nested_fields.insert(
            "room".to_string(),
            ProstValue {
                kind: Some(Kind::StringValue("kitchen".to_string())),
            },
        );

        let list = ListValue {
            values: vec![
                ProstValue {
                    kind: Some(Kind::StringValue("hot".to_string())),
                },
                ProstValue {
                    kind: Some(Kind::StringValue("battery".to_string())),
                },
            ],
        };

        let mut fields = std::collections::BTreeMap::new();
        fields.insert(
            "meta".to_string(),
            ProstValue {
                kind: Some(Kind::StructValue(Struct {
                    fields: nested_fields,
                })),
            },
        );
        fields.insert(
            "tags".to_string(),
            ProstValue {
                kind: Some(Kind::ListValue(list)),
            },
        );

        let payload = Struct { fields }.encode_to_vec();
        let decoded =
            decode_payload_object(&payload, PayloadFormat::Protobuf).expect("decode protobuf");
        assert_eq!(decoded["meta"]["room"], Value::from("kitchen"));
        assert_eq!(decoded["tags"][0], Value::from("hot"));
        assert_eq!(decoded["tags"][1], Value::from("battery"));
    }

    #[test]
    fn decode_protobuf_rejects_invalid_bytes() {
        let err = decode_payload_object(&[0xFF, 0x00, 0xFF], PayloadFormat::Protobuf)
            .expect_err("invalid protobuf should fail");
        assert!(!err.is_empty());
    }
}
