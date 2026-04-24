package com.example;

import com.bifrore.BifroRE;
import com.bifrore.BifroREOptions;
import com.sun.net.httpserver.HttpServer;
import io.micrometer.prometheusmetrics.PrometheusConfig;
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry;

import java.util.Arrays;

public final class AppHeap {
    public static void main(String[] args) throws Exception {
        BifroRE engine = new BifroRE(
            new BifroREOptions()
                .mqtt(mqtt -> mqtt
                    .host("127.0.0.1")
                    .port(1883)
                    .username("dev")
                    .password("dev")
                )
                .jvm(jvm -> jvm
                    .callbackQueueCapacity(1024)
                    .pollBatchLimit(64)
                )
                .ffi(ffi -> ffi
                    .ruleJsonPath(ExampleSupport.extractRuleResource())
                )
        );
        PrometheusMeterRegistry registry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
        HttpServer metricsServer = ExampleSupport.startMetricsServer(registry);
        engine.bindMetrics(registry);
        engine.onNext((ruleIndex, payloadBlob, offset, length, metadata) -> {
            System.out.println("ruleIndex=" + ruleIndex);
            if (metadata != null) {
                System.out.println("destinations=" + Arrays.toString(metadata.destinations));
            }
            System.out.println("payload=" + ExampleSupport.prettyPayload(payloadBlob, offset, length));
        });
        engine.start();
        ExampleSupport.installShutdown(engine, registry, metricsServer, null);
        Thread.currentThread().join();
    }
}
