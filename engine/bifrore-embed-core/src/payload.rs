use prost::Message;
use serde_json::{Map, Value};
use std::sync::Arc;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum PayloadFormat {
    #[default]
    Json,
}

impl PayloadFormat {
    pub fn from_ffi_code(code: i32) -> Option<Self> {
        match code {
            1 => Some(Self::Json),
            _ => None,
        }
    }
}

type TypedDecodeFn =
    dyn Fn(&[u8]) -> Result<Map<String, Value>, String> + Send + Sync + 'static;

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
        }
    }
}

pub fn typed_protobuf_decoder<T, F>(map_fn: F) -> PayloadDecoder
where
    T: Message + Default + Send + Sync + 'static,
    F: Fn(T) -> Result<Map<String, Value>, String> + Send + Sync + 'static,
{
    let decode = move |payload: &[u8]| {
        let message = T::decode(payload).map_err(|err| err.to_string())?;
        map_fn(message)
    };
    PayloadDecoder::ProtobufTyped(Arc::new(decode))
}

pub fn decode_payload_object(
    payload: &[u8],
    format: PayloadFormat,
) -> Result<Map<String, Value>, String> {
    decode_payload_object_with_decoder(payload, &PayloadDecoder::from_format(format))
}

pub fn decode_payload_object_with_decoder(
    payload: &[u8],
    decoder: &PayloadDecoder,
) -> Result<Map<String, Value>, String> {
    match decoder {
        PayloadDecoder::Json => decode_json_object(payload),
        PayloadDecoder::ProtobufTyped(decode) => decode(payload),
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
        let err = decode_payload_object(&[0xFF, 0x00, 0xFF], PayloadFormat::Json)
            .expect_err("invalid json should fail");
        assert!(!err.is_empty());
    }

    #[test]
    fn decode_typed_protobuf_payload() {
        let decoder = typed_protobuf_decoder::<TypedPayload, _>(|message| {
            let mut output = Map::new();
            output.insert("temp".to_string(), Value::from(message.temp));
            output.insert("device".to_string(), Value::from(message.device));
            Ok(output)
        });
        let payload = TypedPayload {
            temp: 30.0,
            device: "sensor-a".to_string(),
        }
        .encode_to_vec();

        let decoded =
            decode_payload_object_with_decoder(&payload, &decoder).expect("decode typed protobuf");
        assert_eq!(decoded.get("temp"), Some(&Value::from(30.0)));
        assert_eq!(decoded.get("device"), Some(&Value::from("sensor-a")));
    }
}
