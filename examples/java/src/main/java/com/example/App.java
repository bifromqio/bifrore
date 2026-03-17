package com.example;

import com.bifrore.BifroRE;
import com.bifrore.BifroREMetricsView;
import com.bifrore.BifroREOptions;
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

public final class App {
    private static final ObjectMapper MAPPER = new ObjectMapper();

    public static void main(String[] args) throws Exception {
        BifroRE engine = new BifroRE(
            new BifroREOptions()
                .host("127.0.0.1")
                .port(1883)
                .callbackQueueCapacity(1024)
                .pollBatchLimit(64)
                .ruleJsonPath(extractRuleResource())
        );
        PrometheusMeterRegistry registry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
        HttpServer metricsServer = startMetricsServer(registry);
        bindMetrics(engine, registry);
        engine.onNext(result -> {
            System.out.println("ruleIndex=" + result.ruleIndex);
            if (result.metadata != null) {
                System.out.println("destinations=" + result.metadata.destinationsJson);
            }
            System.out.println("payload=" + prettyPayload(result.payload));
        });
        engine.start();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            metricsServer.stop(0);
            registry.close();
            engine.close();
        }));
        Thread.currentThread().join();
    }

    private static String prettyPayload(byte[] payload) {
        try {
            JsonNode node = MAPPER.readTree(payload);
            return MAPPER.writerWithDefaultPrettyPrinter().writeValueAsString(node);
        } catch (Exception ignored) {
            return new String(payload, StandardCharsets.UTF_8);
        }
    }

    private static String extractRuleResource() throws Exception {
        try (InputStream input = App.class.getResourceAsStream("/com/example/rule.json")) {
            if (input == null) {
                throw new IllegalStateException("missing resource: /com/example/rule.json");
            }
            Path tempFile = Files.createTempFile("bifrore-rule-", ".json");
            Files.copy(input, tempFile, StandardCopyOption.REPLACE_EXISTING);
            tempFile.toFile().deleteOnExit();
            return tempFile.toAbsolutePath().toString();
        }
    }

    private static void bindMetrics(BifroRE engine, PrometheusMeterRegistry registry) {
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

    private static HttpServer startMetricsServer(PrometheusMeterRegistry registry) throws Exception {
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
}
