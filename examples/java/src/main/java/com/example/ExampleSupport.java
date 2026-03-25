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
        try (InputStream input = ExampleSupport.class.getResourceAsStream("/com/example/rule.json")) {
            if (input == null) {
                throw new IllegalStateException("missing resource: /com/example/rule.json");
            }
            Path tempFile = Files.createTempFile("bifrore-rule-", ".json");
            Files.copy(input, tempFile, StandardCopyOption.REPLACE_EXISTING);
            tempFile.toFile().deleteOnExit();
            return tempFile.toAbsolutePath().toString();
        }
    }

    static void bindMetrics(BifroRE engine, PrometheusMeterRegistry registry) {
        BifroREMetricsView metrics = new BifroREMetricsView(engine);
        Gauge.builder("bifrore_eval_count", metrics, BifroREMetricsView::evalCount).register(registry);
        Gauge.builder("bifrore_eval_error_count", metrics, BifroREMetricsView::evalErrorCount).register(registry);
        Gauge.builder("bifrore_eval_total_nanos", metrics, BifroREMetricsView::evalTotalNanos).register(registry);
        Gauge.builder("bifrore_eval_max_nanos", metrics, BifroREMetricsView::evalMaxNanos).register(registry);
        Gauge.builder("bifrore_eval_avg_nanos", metrics, BifroREMetricsView::evalAvgNanos).register(registry);
        Gauge.builder("bifrore_callback_dropped_count", metrics, BifroREMetricsView::callbackDroppedCount).register(registry);
        Gauge.builder("bifrore_callback_pending_count", metrics, BifroREMetricsView::callbackPendingCount).register(registry);
        Gauge.builder("bifrore_callback_queue_depth", metrics, BifroREMetricsView::callbackQueueDepth).register(registry);
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
