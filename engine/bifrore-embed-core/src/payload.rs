use crate::msg_ir::{MsgIr, PayloadValue};
use prost::Message;
use prost_reflect::{DescriptorPool, DynamicMessage};
use prost_types::{value, ListValue, Struct, Value as PbValue};
use serde_json::Value;
use std::collections::HashSet;
use std::fs;
use std::path::Path;
use std::sync::Arc;

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

type TypedDecodeFn =
    dyn Fn(&[u8], Option<&HashSet<String>>) -> Result<MsgIr, String> + Send + Sync + 'static;

#[derive(Clone)]
pub enum PayloadDecoder {
    Json,
    ProtobufTyped(Arc<TypedDecodeFn>),
}

impl std::fmt::Debug for PayloadDecoder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Json => f.write_str("Json"),
            Self::ProtobufTyped(_) => f.write_str("ProtobufTyped"),
        }
    }
}

impl Default for PayloadDecoder {
    fn default() -> Self {
        Self::Json
    }
}

impl PayloadDecoder {
    pub fn from_format(format: PayloadFormat) -> Self {
        match format {
            PayloadFormat::Json => Self::Json,
            PayloadFormat::Protobuf => Self::ProtobufTyped(Arc::new(decode_protobuf_struct_object)),
        }
    }
}

pub fn typed_protobuf_decoder<T, F>(map_fn: F) -> PayloadDecoder
where
    T: Message + Default + Send + Sync + 'static,
    F: Fn(T) -> Result<MsgIr, String> + Send + Sync + 'static,
{
    let decode = move |payload: &[u8]| {
        let message = T::decode(payload).map_err(|err| err.to_string())?;
        map_fn(message)
    };
    let decode = move |payload: &[u8], required_fields: Option<&HashSet<String>>| {
        let mut decoded = decode(payload)?;
        if let Some(required_fields) = required_fields {
            decoded.retain_fields(required_fields);
        }
        Ok(decoded)
    };
    PayloadDecoder::ProtobufTyped(Arc::new(decode))
}

pub fn dynamic_protobuf_decoder_from_descriptor_set_file<P: AsRef<Path>>(
    descriptor_set_path: P,
    message_name: &str,
) -> Result<PayloadDecoder, String> {
    let descriptor_set = fs::read(descriptor_set_path).map_err(|err| err.to_string())?;
    dynamic_protobuf_decoder_from_descriptor_set_bytes(&descriptor_set, message_name)
}

pub fn dynamic_protobuf_decoder_from_descriptor_set_bytes(
    descriptor_set: &[u8],
    message_name: &str,
) -> Result<PayloadDecoder, String> {
    let pool = DescriptorPool::decode(descriptor_set).map_err(|err| err.to_string())?;
    let message_descriptor = pool
        .get_message_by_name(message_name)
        .ok_or_else(|| format!("protobuf message not found in descriptor set: {message_name}"))?;
    let decode = move |payload: &[u8], required_fields: Option<&HashSet<String>>| {
        let message =
            DynamicMessage::decode(message_descriptor.clone(), payload).map_err(|err| err.to_string())?;
        let value = serde_json::to_value(&message).map_err(|err| err.to_string())?;
        let object = value
            .as_object()
            .cloned()
            .ok_or_else(|| "decoded protobuf message must map to a JSON object".to_string())?;
        MsgIr::from_json_object_with_required_fields(&object, required_fields)
    };
    Ok(PayloadDecoder::ProtobufTyped(Arc::new(decode)))
}

pub fn decode_payload_ir(payload: &[u8], format: PayloadFormat) -> Result<MsgIr, String> {
    decode_payload_ir_with_decoder(payload, &PayloadDecoder::from_format(format))
}

pub fn decode_payload_ir_with_decoder(payload: &[u8], decoder: &PayloadDecoder) -> Result<MsgIr, String> {
    decode_payload_ir_with_decoder_and_required_fields(payload, decoder, None)
}

pub fn decode_payload_ir_with_decoder_and_required_fields(
    payload: &[u8],
    decoder: &PayloadDecoder,
    required_fields: Option<&HashSet<String>>,
) -> Result<MsgIr, String> {
    match decoder {
        PayloadDecoder::Json => decode_json_ir(payload, required_fields),
        PayloadDecoder::ProtobufTyped(decode) => decode(payload, required_fields),
    }
}

fn decode_json_ir(payload: &[u8], required_fields: Option<&HashSet<String>>) -> Result<MsgIr, String> {
    #[cfg(feature = "simd-json")]
    let parsed: Value = {
        let mut buffer = payload.to_vec();
        simd_json::serde::from_slice(&mut buffer).map_err(|err| err.to_string())?
    };

    #[cfg(not(feature = "simd-json"))]
    let parsed: Value = serde_json::from_slice(payload).map_err(|err| err.to_string())?;

    let object = parsed
        .as_object()
        .cloned()
        .ok_or_else(|| "payload must be a JSON object".to_string())?;
    MsgIr::from_json_object_with_required_fields(&object, required_fields)
}

fn decode_protobuf_struct_object(
    payload: &[u8],
    required_fields: Option<&HashSet<String>>,
) -> Result<MsgIr, String> {
    let parsed = Struct::decode(payload).map_err(|err| err.to_string())?;
    let fields = convert_protobuf_struct(parsed);
    Ok(MsgIr::from_protobuf_struct_with_required_fields(&fields, required_fields))
}

fn convert_protobuf_struct(input: Struct) -> Vec<(String, PayloadValue)> {
    input
        .fields
        .into_iter()
        .map(|(key, value)| (key, convert_protobuf_value(value)))
        .collect()
}

fn convert_protobuf_list(input: ListValue) -> Vec<PayloadValue> {
    input
        .values
        .into_iter()
        .map(convert_protobuf_value)
        .collect()
}

fn convert_protobuf_value(input: PbValue) -> PayloadValue {
    match input.kind {
        Some(value::Kind::NullValue(_)) => PayloadValue::Null,
        Some(value::Kind::NumberValue(number)) => PayloadValue::Number(number),
        Some(value::Kind::StringValue(text)) => PayloadValue::String(text),
        Some(value::Kind::BoolValue(flag)) => PayloadValue::Bool(flag),
        Some(value::Kind::StructValue(object)) => PayloadValue::Object(convert_protobuf_struct(object)),
        Some(value::Kind::ListValue(list)) => PayloadValue::Array(convert_protobuf_list(list)),
        None => PayloadValue::Null,
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[derive(Clone, PartialEq, ::prost::Message)]
    struct TypedPayload {
        #[prost(double, tag = "1")]
        temp: f64,
        #[prost(string, tag = "2")]
        device: String,
    }

    #[test]
    fn decode_json_rejects_invalid_bytes() {
        let err = decode_payload_ir(&[0xFF, 0x00, 0xFF], PayloadFormat::Json)
            .expect_err("invalid json should fail");
        assert!(!err.is_empty());
    }

    #[test]
    fn decode_typed_protobuf_payload() {
        let decoder = typed_protobuf_decoder::<TypedPayload, _>(|message| {
            let mut output = MsgIr::new();
            output.insert("temp", PayloadValue::from(message.temp));
            output.insert("device", PayloadValue::from(message.device));
            Ok(output)
        });
        let payload = TypedPayload {
            temp: 30.0,
            device: "sensor-a".to_string(),
        }
        .encode_to_vec();

        let decoded = decode_payload_ir_with_decoder(&payload, &decoder).expect("decode typed protobuf");
        assert_eq!(decoded.get_key("temp"), Some(&PayloadValue::from(30.0)));
        assert_eq!(decoded.get_key("device"), Some(&PayloadValue::from("sensor-a")));
    }

    #[test]
    fn decode_protobuf_struct_payload() {
        let mut nested = Struct::default();
        nested
            .fields
            .insert("hum".to_string(), PbValue { kind: Some(value::Kind::NumberValue(61.0)) });

        let mut root = Struct::default();
        root.fields.insert(
            "temp".to_string(),
            PbValue {
                kind: Some(value::Kind::NumberValue(30.0)),
            },
        );
        root.fields.insert(
            "meta".to_string(),
            PbValue {
                kind: Some(value::Kind::StructValue(nested)),
            },
        );
        let payload = root.encode_to_vec();

        let decoded = decode_payload_ir(&payload, PayloadFormat::Protobuf).expect("decode protobuf struct");
        assert_eq!(decoded.get_key("temp"), Some(&PayloadValue::from(30.0)));
        assert_eq!(decoded.get_key("meta.hum"), Some(&PayloadValue::from(61.0)));
    }
}
