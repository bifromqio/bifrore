use crate::msg_ir::MsgIr;
use prost_reflect::{DescriptorPool, DynamicMessage};
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

type DecodeFn = dyn Fn(&[u8], Option<&HashSet<String>>) -> Result<MsgIr, String> + Send + Sync + 'static;

#[derive(Clone)]
pub enum PayloadDecoder {
    Json,
    #[doc(hidden)]
    Protobuf(Arc<DecodeFn>),
}

impl std::fmt::Debug for PayloadDecoder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Json => f.write_str("Json"),
            Self::Protobuf(_) => f.write_str("Protobuf"),
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
            PayloadFormat::Protobuf => Self::Protobuf(Arc::new(unsupported_protobuf_decoder)),
        }
    }
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
    Ok(PayloadDecoder::Protobuf(Arc::new(decode)))
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
        PayloadDecoder::Protobuf(decode) => decode(payload, required_fields),
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

fn unsupported_protobuf_decoder(
    _payload: &[u8],
    _required_fields: Option<&HashSet<String>>,
) -> Result<MsgIr, String> {
    Err("protobuf payload decoding requires a descriptor-set file and fully-qualified message name".to_string())
}
#[cfg(test)]
mod tests {
    use super::*;
    use prost::Message;

    #[derive(Clone, PartialEq, ::prost::Message)]
    struct EvalPayload {
        #[prost(double, tag = "1")]
        temp: f64,
        #[prost(double, tag = "2")]
        hum: f64,
    }

    #[test]
    fn decode_json_rejects_invalid_bytes() {
        let err = decode_payload_ir(&[0xFF, 0x00, 0xFF], PayloadFormat::Json)
            .expect_err("invalid json should fail");
        assert!(!err.is_empty());
    }

    #[test]
    fn decode_schema_based_protobuf_payload() {
        let decoder = dynamic_protobuf_decoder_from_descriptor_set_bytes(
            include_bytes!("../testdata/bifrore_test.desc"),
            "bifrore.test.EvalPayload",
        )
        .expect("protobuf decoder");
        let payload = EvalPayload {
            temp: 30.0,
            hum: 61.0,
        }
        .encode_to_vec();

        let decoded = decode_payload_ir_with_decoder(&payload, &decoder).expect("decode protobuf");
        assert_eq!(decoded.get_key("temp").and_then(|value| value.as_f64()), Some(30.0));
        assert_eq!(decoded.get_key("hum").and_then(|value| value.as_f64()), Some(61.0));
    }

    #[test]
    fn decode_protobuf_without_schema_is_rejected() {
        let err = decode_payload_ir(&[0x08, 0x96, 0x01], PayloadFormat::Protobuf)
            .expect_err("protobuf without schema should fail");
        assert!(err.contains("descriptor-set"));
    }
}
