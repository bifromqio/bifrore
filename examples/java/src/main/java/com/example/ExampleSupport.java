package com.example;

import com.bifrore.BifroRE;
import com.bifrore.BifroREMetricsView;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpServer;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.prometheusmetrics.PrometheusConfig;
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry;

import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;

final class ExampleSupport {
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final String DEFAULT_RULE_RESOURCE = "/com/example/rule.json";
    private static final String PROTOBUF_RULE_RESOURCE = "/com/example/rule-protobuf.json";
    private static final String PROTOBUF_DESCRIPTOR_RESOURCE = "/com/example/telemetry.desc";

    private ExampleSupport() {}

    static String prettyPayload(byte[] payloadBlob, int offset, int length) {
        try {
            JsonNode node = MAPPER.readTree(payloadBlob, offset, length);
            return MAPPER.writerWithDefaultPrettyPrinter().writeValueAsString(node);
        } catch (Exception ignored) {
            return new String(payloadBlob, offset, length, StandardCharsets.UTF_8);
        }
    }

    static String extractRuleResource() throws Exception {
        return extractResource(DEFAULT_RULE_RESOURCE, "bifrore-rule-", ".json");
    }

    static String extractProtobufRuleResource() throws Exception {
        return extractResource(PROTOBUF_RULE_RESOURCE, "bifrore-rule-protobuf-", ".json");
    }

    static String extractProtobufDescriptorResource() throws Exception {
        return extractResource(PROTOBUF_DESCRIPTOR_RESOURCE, "bifrore-protobuf-", ".desc");
    }

    private static String extractResource(String resourcePath, String prefix, String suffix) throws Exception {
        try (InputStream input = ExampleSupport.class.getResourceAsStream(resourcePath)) {
            if (input == null) {
                throw new IllegalStateException("missing resource: " + resourcePath);
            }
            Path tempFile = Files.createTempFile(prefix, suffix);
            Files.copy(input, tempFile, StandardCopyOption.REPLACE_EXISTING);
            tempFile.toFile().deleteOnExit();
            return tempFile.toAbsolutePath().toString();
        }
    }

    static void bindMetrics(BifroRE engine, PrometheusMeterRegistry registry) {
        BifroREMetricsView metrics = new BifroREMetricsView(engine);
        Gauge.builder("bifrore_eval_count", metrics, BifroREMetricsView::evalCount).register(registry);
        Gauge.builder("bifrore_eval_error_count", metrics, BifroREMetricsView::evalErrorCount).register(registry);
        Gauge.builder("bifrore_eval_type_error_count", metrics, BifroREMetricsView::evalTypeErrorCount).register(registry);
        Gauge.builder("bifrore_message_pipeline_total_nanos", metrics, BifroREMetricsView::messagePipelineTotalNanos).register(registry);
        Gauge.builder("bifrore_message_pipeline_max_nanos", metrics, BifroREMetricsView::messagePipelineMaxNanos).register(registry);
        Gauge.builder("bifrore_exec_total_nanos", metrics, BifroREMetricsView::execTotalNanos).register(registry);
        Gauge.builder("bifrore_exec_max_nanos", metrics, BifroREMetricsView::execMaxNanos).register(registry);
        Gauge.builder("bifrore_topic_match_count", metrics, BifroREMetricsView::topicMatchCount).register(registry);
        Gauge.builder("bifrore_topic_match_total_nanos", metrics, BifroREMetricsView::topicMatchTotalNanos).register(registry);
        Gauge.builder("bifrore_topic_match_max_nanos", metrics, BifroREMetricsView::topicMatchMaxNanos).register(registry);
        Gauge.builder("bifrore_payload_decode_count", metrics, BifroREMetricsView::payloadDecodeCount).register(registry);
        Gauge.builder("bifrore_payload_decode_total_nanos", metrics, BifroREMetricsView::payloadDecodeTotalNanos).register(registry);
        Gauge.builder("bifrore_payload_decode_max_nanos", metrics, BifroREMetricsView::payloadDecodeMaxNanos).register(registry);
        Gauge.builder("bifrore_msg_ir_build_count", metrics, BifroREMetricsView::msgIrBuildCount).register(registry);
        Gauge.builder("bifrore_msg_ir_build_total_nanos", metrics, BifroREMetricsView::msgIrBuildTotalNanos).register(registry);
        Gauge.builder("bifrore_msg_ir_build_max_nanos", metrics, BifroREMetricsView::msgIrBuildMaxNanos).register(registry);
        Gauge.builder("bifrore_predicate_count", metrics, BifroREMetricsView::predicateCount).register(registry);
        Gauge.builder("bifrore_predicate_total_nanos", metrics, BifroREMetricsView::predicateTotalNanos).register(registry);
        Gauge.builder("bifrore_predicate_max_nanos", metrics, BifroREMetricsView::predicateMaxNanos).register(registry);
        Gauge.builder("bifrore_projection_count", metrics, BifroREMetricsView::projectionCount).register(registry);
        Gauge.builder("bifrore_projection_total_nanos", metrics, BifroREMetricsView::projectionTotalNanos).register(registry);
        Gauge.builder("bifrore_projection_max_nanos", metrics, BifroREMetricsView::projectionMaxNanos).register(registry);
        Gauge.builder("bifrore_callback_dropped_count", metrics, BifroREMetricsView::callbackDroppedCount).register(registry);
        Gauge.builder("bifrore_callback_pending_count", metrics, BifroREMetricsView::callbackPendingCount).register(registry);
        Gauge.builder("bifrore_callback_queue_depth", metrics, BifroREMetricsView::callbackQueueDepth).register(registry);
        Gauge.builder("bifrore_heap_poll_message_count", metrics, BifroREMetricsView::heapPollMessageCount).register(registry);
        Gauge.builder("bifrore_heap_poll_payload_bytes", metrics, BifroREMetricsView::heapPollPayloadBytes).register(registry);
        Gauge.builder("bifrore_heap_poll_error_count", metrics, BifroREMetricsView::heapPollErrorCount).register(registry);
        Gauge.builder("bifrore_poller_timeout_pending_count", metrics, BifroREMetricsView::pollerTimeoutPendingCount).register(registry);
        Gauge.builder("bifrore_shutdown_dropped_count", metrics, BifroREMetricsView::shutdownDroppedCount).register(registry);
    }

    static HttpServer startMetricsServer(PrometheusMeterRegistry registry) throws Exception {
        HttpServer server = HttpServer.create(new InetSocketAddress("0.0.0.0", 9464), 0);
        server.createContext("/metrics", exchange -> {
            byte[] body = registry.scrape().getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().add("Content-Type", "text/plain; version=0.0.4; charset=utf-8");
            exchange.sendResponseHeaders(200, body.length);
            try (OutputStream output = exchange.getResponseBody()) {
                output.write(body);
            }
        });
        server.start();
        System.out.println("Prometheus metrics exposed on http://127.0.0.1:9464/metrics");
        return server;
    }

    static void installShutdown(BifroRE engine, PrometheusMeterRegistry registry, HttpServer metricsServer, Runnable extraClose) {
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            if (extraClose != null) {
                extraClose.run();
            }
            metricsServer.stop(0);
            registry.close();
            engine.close();
        }));
    }
}
