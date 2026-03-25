package com.example;

import com.bifrore.BifroRE;
import com.bifrore.BifroREOptions;
import com.sun.net.httpserver.HttpServer;
import io.micrometer.prometheusmetrics.PrometheusConfig;
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;

public final class AppDirectKafka {
    public static void main(String[] args) throws Exception {
        BifroRE engine = new BifroRE(
            new BifroREOptions()
                .host("127.0.0.1")
                .port(1883)
                .callbackQueueCapacity(1024)
                .pollBatchLimit(64)
                .directPollSlotCount(4)
                .directPayloadBufferBytes(1024 * 1024)
                .ruleJsonPath(ExampleSupport.extractRuleResource())
        );
        PrometheusMeterRegistry registry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
        HttpServer metricsServer = ExampleSupport.startMetricsServer(registry);
        ExampleSupport.bindMetrics(engine, registry);

        KafkaProducer<String, byte[]> producer = createKafkaProducer();
        engine.onNextAsyncDirect((ruleIndex, payloadBuffer, offset, length, metadata) -> {
            if (metadata != null) {
                System.out.println("ruleIndex=" + ruleIndex + " destinations=" + Arrays.toString(metadata.destinations));
            }

            byte[] retainedPayload = new byte[length];
            ByteBuffer copyView = payloadBuffer.asReadOnlyBuffer();
            copyView.position(offset);
            copyView.limit(offset + length);
            copyView.get(retainedPayload);

            System.out.println(
                "retained payload for async Kafka send=" + ExampleSupport.prettyPayload(retainedPayload, 0, retainedPayload.length)
            );

            CompletableFuture<Void> future = new CompletableFuture<>();
            ProducerRecord<String, byte[]> record = new ProducerRecord<>("bifrore-output", retainedPayload);
            producer.send(record, (metadataRecord, error) -> {
                if (error != null) {
                    future.completeExceptionally(error);
                } else {
                    future.complete(null);
                }
            });
            return future;
        });

        engine.start();
        ExampleSupport.installShutdown(engine, registry, metricsServer, producer::close);
        Thread.currentThread().join();
    }

    private static KafkaProducer<String, byte[]> createKafkaProducer() {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "127.0.0.1:9092");
        properties.put("acks", "all");
        properties.put("key.serializer", StringSerializer.class.getName());
        properties.put("value.serializer", ByteArraySerializer.class.getName());
        return new KafkaProducer<>(properties);
    }
}
