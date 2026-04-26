package com.bifrore;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Objects;
import java.util.function.ToDoubleFunction;

final class BifroREMetricsBinder {
    private static final String METER_REGISTRY_CLASS = "io.micrometer.core.instrument.MeterRegistry";
    private static final String GAUGE_CLASS = "io.micrometer.core.instrument.Gauge";
    private static final String FUNCTION_COUNTER_CLASS = "io.micrometer.core.instrument.FunctionCounter";

    private BifroREMetricsBinder() {}

    static void bind(BifroRE engine, Object registry) {
        bind(engine, registry, "bifrore");
    }

    static void bind(BifroRE engine, Object registry, String prefix) {
        Objects.requireNonNull(engine, "engine");
        Objects.requireNonNull(registry, "registry");
        String metricPrefix = normalizePrefix(prefix);
        BifroREMetricsView metrics = new BifroREMetricsView(engine);

        bindCounter(registry, metricPrefix + "_evals", metrics, BifroREMetricsView::evalCount);
        bindCounter(registry, metricPrefix + "_eval_errors", metrics, BifroREMetricsView::evalErrorCount);
        bindCounter(registry, metricPrefix + "_eval_type_errors", metrics, BifroREMetricsView::evalTypeErrorCount);
        bindCounter(registry, metricPrefix + "_payload_schema_errors", metrics, BifroREMetricsView::payloadSchemaErrorCount);
        bindCounter(registry, metricPrefix + "_payload_decode_errors", metrics, BifroREMetricsView::payloadDecodeErrorCount);
        bindCounter(registry, metricPrefix + "_payload_build_errors", metrics, BifroREMetricsView::payloadBuildErrorCount);
        bindCounter(registry, metricPrefix + "_core_eval_latency_samples", metrics, BifroREMetricsView::coreEvalCount);
        bindCounter(registry, metricPrefix + "_core_eval_latency_nanos", metrics, BifroREMetricsView::coreEvalTotalNanos);
        bindGauge(registry, metricPrefix + "_core_eval_latency_max_nanos", metrics, BifroREMetricsView::coreEvalMaxNanos);
        bindCounter(registry, metricPrefix + "_worker_pipeline_latency_samples", metrics, BifroREMetricsView::workerPipelineCount);
        bindCounter(registry, metricPrefix + "_worker_pipeline_latency_nanos", metrics, BifroREMetricsView::workerPipelineTotalNanos);
        bindGauge(registry, metricPrefix + "_worker_pipeline_latency_max_nanos", metrics, BifroREMetricsView::workerPipelineMaxNanos);
        bindCounter(registry, metricPrefix + "_exec_latency_samples", metrics, BifroREMetricsView::execCount);
        bindCounter(registry, metricPrefix + "_exec_latency_nanos", metrics, BifroREMetricsView::execTotalNanos);
        bindGauge(registry, metricPrefix + "_exec_latency_max_nanos", metrics, BifroREMetricsView::execMaxNanos);
        bindCounter(registry, metricPrefix + "_topic_match_latency_samples", metrics, BifroREMetricsView::topicMatchCount);
        bindCounter(registry, metricPrefix + "_topic_match_latency_nanos", metrics, BifroREMetricsView::topicMatchTotalNanos);
        bindGauge(registry, metricPrefix + "_topic_match_latency_max_nanos", metrics, BifroREMetricsView::topicMatchMaxNanos);
        bindCounter(registry, metricPrefix + "_payload_decode_latency_samples", metrics, BifroREMetricsView::payloadDecodeCount);
        bindCounter(registry, metricPrefix + "_payload_decode_latency_nanos", metrics, BifroREMetricsView::payloadDecodeTotalNanos);
        bindGauge(registry, metricPrefix + "_payload_decode_latency_max_nanos", metrics, BifroREMetricsView::payloadDecodeMaxNanos);
        bindCounter(registry, metricPrefix + "_msg_ir_build_latency_samples", metrics, BifroREMetricsView::msgIrBuildCount);
        bindCounter(registry, metricPrefix + "_msg_ir_build_latency_nanos", metrics, BifroREMetricsView::msgIrBuildTotalNanos);
        bindGauge(registry, metricPrefix + "_msg_ir_build_latency_max_nanos", metrics, BifroREMetricsView::msgIrBuildMaxNanos);
        bindCounter(registry, metricPrefix + "_predicate_latency_samples", metrics, BifroREMetricsView::predicateCount);
        bindCounter(registry, metricPrefix + "_predicate_latency_nanos", metrics, BifroREMetricsView::predicateTotalNanos);
        bindGauge(registry, metricPrefix + "_predicate_latency_max_nanos", metrics, BifroREMetricsView::predicateMaxNanos);
        bindCounter(registry, metricPrefix + "_projection_latency_samples", metrics, BifroREMetricsView::projectionCount);
        bindCounter(registry, metricPrefix + "_projection_latency_nanos", metrics, BifroREMetricsView::projectionTotalNanos);
        bindGauge(registry, metricPrefix + "_projection_latency_max_nanos", metrics, BifroREMetricsView::projectionMaxNanos);
        bindCounter(registry, metricPrefix + "_ingress_messages", metrics, BifroREMetricsView::ingressMessageCount);
        bindGauge(registry, metricPrefix + "_core_queue_depth", metrics, BifroREMetricsView::coreQueueDepth);
        bindGauge(registry, metricPrefix + "_core_queue_depth_max", metrics, BifroREMetricsView::coreQueueDepthMax);
        bindCounter(registry, metricPrefix + "_core_queue_drops", metrics, BifroREMetricsView::coreQueueDropCount);
        bindGauge(registry, metricPrefix + "_ffi_queue_depth", metrics, BifroREMetricsView::ffiQueueDepth);
        bindGauge(registry, metricPrefix + "_ffi_queue_depth_max", metrics, BifroREMetricsView::ffiQueueDepthMax);
        bindCounter(registry, metricPrefix + "_ffi_queue_drops", metrics, BifroREMetricsView::ffiQueueDropCount);
        bindCounter(registry, metricPrefix + "_callback_drops", metrics, BifroREMetricsView::callbackDroppedCount);
        bindGauge(registry, metricPrefix + "_callback_pending_count", metrics, BifroREMetricsView::callbackPendingCount);
        bindGauge(registry, metricPrefix + "_callback_queue_depth", metrics, BifroREMetricsView::callbackQueueDepth);
        bindCounter(registry, metricPrefix + "_heap_poll_errors", metrics, BifroREMetricsView::heapPollErrorCount);
        bindCounter(registry, metricPrefix + "_heap_poll_invalid_arguments", metrics, BifroREMetricsView::heapPollInvalidArgumentCount);
        bindCounter(registry, metricPrefix + "_heap_poll_invalid_states", metrics, BifroREMetricsView::heapPollInvalidStateCount);
        bindCounter(registry, metricPrefix + "_heap_poll_internal_queue_errors", metrics, BifroREMetricsView::heapPollInternalQueueErrorCount);
        bindCounter(registry, metricPrefix + "_heap_poll_unknown_errors", metrics, BifroREMetricsView::heapPollUnknownErrorCount);
        bindCounter(registry, metricPrefix + "_heap_poll_messages", metrics, BifroREMetricsView::heapPollMessageCount);
        bindCounter(registry, metricPrefix + "_heap_poll_payload_bytes", metrics, BifroREMetricsView::heapPollPayloadBytes);
        bindCounter(registry, metricPrefix + "_heap_poll_empty_polls", metrics, BifroREMetricsView::heapPollNoDataCount);
        bindCounter(registry, metricPrefix + "_shutdown_drops", metrics, BifroREMetricsView::shutdownDroppedCount);
        bindCounter(registry, metricPrefix + "_poller_timeout_pending_events", metrics, BifroREMetricsView::pollerTimeoutPendingCount);
    }

    private static String normalizePrefix(String prefix) {
        if (prefix == null || prefix.isBlank()) {
            return "bifrore";
        }
        return prefix.trim();
    }

    private static void bindGauge(
        Object registry,
        String name,
        BifroREMetricsView metrics,
        ToDoubleFunction<BifroREMetricsView> extractor
    ) {
        bindMeter(registry, GAUGE_CLASS, name, metrics, extractor);
    }

    private static void bindCounter(
        Object registry,
        String name,
        BifroREMetricsView metrics,
        ToDoubleFunction<BifroREMetricsView> extractor
    ) {
        bindMeter(registry, FUNCTION_COUNTER_CLASS, name, metrics, extractor);
    }

    private static void bindMeter(
        Object registry,
        String meterClassName,
        String name,
        BifroREMetricsView metrics,
        ToDoubleFunction<BifroREMetricsView> extractor
    ) {
        try {
            Class<?> meterRegistryClass = Class.forName(METER_REGISTRY_CLASS);
            if (!meterRegistryClass.isInstance(registry)) {
                throw new IllegalArgumentException(
                    "registry must be an instance of " + METER_REGISTRY_CLASS
                );
            }
            Class<?> meterClass = Class.forName(meterClassName);
            Method builderMethod =
                meterClass.getMethod("builder", String.class, Object.class, ToDoubleFunction.class);
            Object builder = builderMethod.invoke(null, name, metrics, extractor);
            useStrongReference(builder);
            Method registerMethod = builder.getClass().getMethod("register", meterRegistryClass);
            registerMethod.invoke(builder, registry);
        } catch (ClassNotFoundException e) {
            throw new IllegalStateException(
                "Micrometer is not on the classpath; add io.micrometer:micrometer-core",
                e
            );
        } catch (NoSuchMethodException | IllegalAccessException e) {
            throw new IllegalStateException("Unsupported Micrometer version", e);
        } catch (InvocationTargetException e) {
            Throwable cause = e.getCause();
            if (cause instanceof RuntimeException) {
                throw (RuntimeException) cause;
            }
            throw new IllegalStateException("Failed to bind BifroRE metrics", cause);
        }
    }

    private static void useStrongReference(Object builder) {
        try {
            Method strongReferenceMethod = builder.getClass().getMethod("strongReference", boolean.class);
            strongReferenceMethod.invoke(builder, true);
        } catch (NoSuchMethodException ignored) {
        } catch (ReflectiveOperationException e) {
            throw new IllegalStateException("Failed to configure Micrometer strong reference", e);
        }
    }
}
