use crate::metrics::{EvalMetrics, LatencyStage};
use crate::msg_ir::{CompiledPayloadField, MsgIr};
use prost_reflect::{DescriptorPool, DynamicMessage};
use serde_json::Value;
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

type DecodeFn =
    dyn Fn(&[u8], PayloadDecodePlan<'_>, Option<&EvalMetrics>, Option<&str>) -> Result<MsgIr, String>
    + Send
    + Sync
    + 'static;

#[derive(Debug, Clone, Copy)]
pub enum PayloadDecodePlan<'a> {
    None,
    Sparse(&'a [&'a CompiledPayloadField]),
    Full,
}

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

pub fn dynamic_protobuf_registry_from_descriptor_set_file<P: AsRef<Path>>(
    descriptor_set_path: P,
) -> Result<PayloadDecoder, String> {
    let descriptor_set = fs::read(descriptor_set_path).map_err(|err| err.to_string())?;
    dynamic_protobuf_registry_from_descriptor_set_bytes(&descriptor_set)
}

pub fn dynamic_protobuf_registry_from_descriptor_set_bytes(
    descriptor_set: &[u8],
) -> Result<PayloadDecoder, String> {
    let pool = Arc::new(DescriptorPool::decode(descriptor_set).map_err(|err| err.to_string())?);
    let decode = move |payload: &[u8],
                       plan: PayloadDecodePlan<'_>,
                       metrics: Option<&EvalMetrics>,
                       schema_name: Option<&str>| {
        let schema_name = schema_name
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .ok_or_else(|| "protobuf schema name is required per rule".to_string())?;
        let message_descriptor = pool
            .get_message_by_name(schema_name)
            .ok_or_else(|| format!("protobuf message not found in descriptor set: {schema_name}"))?;
        let decode_timer = metrics.map(|metrics| metrics.start_stage());
        let message =
            DynamicMessage::decode(message_descriptor, payload).map_err(|err| err.to_string())?;
        if let Some(metrics) = metrics {
            metrics.finish_stage(
                LatencyStage::PayloadDecode,
                decode_timer.expect("stage timer present with metrics"),
            );
        }

        let ir_timer = metrics.map(|metrics| metrics.start_stage());
        let ir = MsgIr::from_protobuf_message_with_decode_plan(&message, plan)?;
        if let Some(metrics) = metrics {
            metrics.finish_stage(
                LatencyStage::MsgIrBuild,
                ir_timer.expect("stage timer present with metrics"),
            );
        }
        Ok(ir)
    };
    Ok(PayloadDecoder::Protobuf(Arc::new(decode)))
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
    let registry = dynamic_protobuf_registry_from_descriptor_set_bytes(descriptor_set)?;
    let default_schema = message_name.to_string();
    match registry {
        PayloadDecoder::Protobuf(decode) => Ok(PayloadDecoder::Protobuf(Arc::new(
            move |payload, plan, metrics, _schema_name| {
                decode(payload, plan, metrics, Some(default_schema.as_str()))
            },
        ))),
        PayloadDecoder::Json => unreachable!("protobuf registry must produce protobuf decoder"),
    }
}

pub fn decode_payload_ir(payload: &[u8], format: PayloadFormat) -> Result<MsgIr, String> {
    decode_payload_ir_with_decoder(payload, &PayloadDecoder::from_format(format))
}

pub fn decode_payload_ir_with_decoder(
    payload: &[u8],
    decoder: &PayloadDecoder,
) -> Result<MsgIr, String> {
    decode_payload_ir_with_decoder_and_plan_and_metrics(
        payload,
        decoder,
        PayloadDecodePlan::Full,
        None,
    )
}

pub fn decode_payload_ir_with_decoder_and_plan_and_metrics(
    payload: &[u8],
    decoder: &PayloadDecoder,
    plan: PayloadDecodePlan<'_>,
    metrics: Option<&EvalMetrics>,
) -> Result<MsgIr, String> {
    decode_payload_ir_with_decoder_and_plan_and_metrics_and_schema(
        payload, decoder, plan, metrics, None,
    )
}

pub fn decode_payload_ir_with_decoder_and_plan_and_metrics_and_schema(
    payload: &[u8],
    decoder: &PayloadDecoder,
    plan: PayloadDecodePlan<'_>,
    metrics: Option<&EvalMetrics>,
    schema_name: Option<&str>,
) -> Result<MsgIr, String> {
    match decoder {
        PayloadDecoder::Json => decode_json_ir(payload, plan, metrics),
        PayloadDecoder::Protobuf(decode) => decode(payload, plan, metrics, schema_name),
    }
}

fn decode_json_ir(
    payload: &[u8],
    plan: PayloadDecodePlan<'_>,
    metrics: Option<&EvalMetrics>,
) -> Result<MsgIr, String> {
    if matches!(plan, PayloadDecodePlan::None) {
        return Ok(MsgIr::new());
    }

    let decode_timer = metrics.map(|metrics| metrics.start_stage());
    #[cfg(feature = "simd-json")]
    let parsed: Value = {
        let mut buffer = payload.to_vec();
        simd_json::serde::from_slice(&mut buffer).map_err(|err| err.to_string())?
    };

    #[cfg(not(feature = "simd-json"))]
    let parsed: Value = serde_json::from_slice(payload).map_err(|err| err.to_string())?;
    if let Some(metrics) = metrics {
        metrics.finish_stage(
            LatencyStage::PayloadDecode,
            decode_timer.expect("stage timer present with metrics"),
        );
    }

    let object = parsed
        .as_object()
        .ok_or_else(|| "payload must be a JSON object".to_string())?;
    let ir_timer = metrics.map(|metrics| metrics.start_stage());
    let ir = MsgIr::from_json_object_with_decode_plan(object, plan)?;
    if let Some(metrics) = metrics {
        metrics.finish_stage(
            LatencyStage::MsgIrBuild,
            ir_timer.expect("stage timer present with metrics"),
        );
    }
    Ok(ir)
}

fn unsupported_protobuf_decoder(
    _payload: &[u8],
    _plan: PayloadDecodePlan<'_>,
    _metrics: Option<&EvalMetrics>,
    _schema_name: Option<&str>,
) -> Result<MsgIr, String> {
    Err(
        "protobuf payload decoding requires a descriptor-set file and fully-qualified message name"
            .to_string(),
    )
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
        assert_eq!(
            decoded.get_key("temp").and_then(|value| value.as_f64()),
            Some(30.0)
        );
        assert_eq!(
            decoded.get_key("hum").and_then(|value| value.as_f64()),
            Some(61.0)
        );
    }

    #[test]
    fn decode_protobuf_without_schema_is_rejected() {
        let err = decode_payload_ir(&[0x08, 0x96, 0x01], PayloadFormat::Protobuf)
            .expect_err("protobuf without schema should fail");
        assert!(err.contains("descriptor-set"));
    }
}
