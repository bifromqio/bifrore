package com.bifrore.test;

import com.bifrore.BifroRE;
import com.bifrore.BifroREOptions;
import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.eclipse.paho.mqttv5.client.MqttClient;
import org.eclipse.paho.mqttv5.client.MqttConnectionOptions;
import org.eclipse.paho.mqttv5.client.MqttConnectionOptionsBuilder;
import org.eclipse.paho.mqttv5.common.MqttException;
import org.eclipse.paho.mqttv5.common.MqttMessage;
import org.eclipse.paho.mqttv5.common.packet.MqttProperties;
import org.eclipse.paho.mqttv5.common.packet.UserProperty;
import org.junit.jupiter.api.Test;

import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class BifroREIntegrationTest {
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final Duration AWAIT_TIMEOUT = Duration.ofSeconds(20);
    private static final String BROKER_URI = "tcp://127.0.0.1:1883";

    @Test
    void heapJsonCoversExactPlusHashArithmeticAndMultiRule() throws Exception {
        Path clientIdsDir = Files.createTempDirectory("bifrore-itest-json-");
        String rulePath = writeRuleFile("""
            {
              "rules": [
                {
                  "expression": "select temp + 2 as plus_two from 'sensors/room1/temp' where temp > 20",
                  "destinations": ["exact"]
                },
                {
                  "expression": "select topic_level(t, 2) as room, temp from 'sensors/+/temp' as t where properties['source'] = 'ci'",
                  "destinations": ["plus"]
                },
                {
                  "expression": "select hum as humidity from 'sensors/#' where hum >= 60",
                  "destinations": ["hash"]
                }
              ]
            }
            """);

        CountDownLatch latch = new CountDownLatch(3);
        Set<String> destinationsSeen = ConcurrentHashMap.newKeySet();
        ConcurrentLinkedQueue<JsonNode> payloads = new ConcurrentLinkedQueue<>();

        try (BifroRE engine = new BifroRE(
            baseOptions(rulePath, clientIdsDir, "itest-json-" + UUID.randomUUID())
        )) {
            engine.onNext((ruleIndex, payloadBlob, offset, length, metadata) -> {
                try {
                    payloads.add(MAPPER.readTree(payloadBlob, offset, length));
                    destinationsSeen.add(metadata.destinations[0]);
                    latch.countDown();
                } catch (Exception error) {
                    throw new RuntimeException(error);
                }
            });
            assertEquals(0, engine.start());
            awaitSubscriptionWarmup();

            publish(
                "sensors/room1/temp",
                "{\"temp\":25,\"hum\":61}".getBytes(StandardCharsets.UTF_8),
                List.of(new UserProperty("source", "ci"))
            );

            assertTrue(latch.await(AWAIT_TIMEOUT.toSeconds(), TimeUnit.SECONDS));
        }

        assertEquals(Set.of("exact", "plus", "hash"), destinationsSeen);
        assertEquals(3, payloads.size());
        assertTrue(payloads.stream().anyMatch(payload -> payload.has("plus_two") && payload.get("plus_two").asDouble() == 27.0));
        assertTrue(payloads.stream().anyMatch(payload -> payload.has("room") && "room1".equals(payload.get("room").asText()) && payload.get("temp").asDouble() == 25.0));
        assertTrue(payloads.stream().anyMatch(payload -> payload.has("humidity") && payload.get("humidity").asDouble() == 61.0));
    }

    @Test
    void directProtobufProjectionWorks() throws Exception {
        Path clientIdsDir = Files.createTempDirectory("bifrore-itest-protobuf-");
        String descriptorPath = extractResource("/proto/telemetry.desc", "bifrore-itest-protobuf-", ".desc");
        String rulePath = writeRuleFile("""
            {
              "rules": [
                {
                  "expression": "select sensor.device as device, sensor.reading.temp as temperature, sensor.reading.hum as humidity, site from data where sensor.reading.online = true and sensor.reading.temp >= 20",
                  "destinations": ["kafka", "log"]
                }
              ]
            }
            """);

        CountDownLatch latch = new CountDownLatch(1);
        ConcurrentLinkedQueue<JsonNode> payloads = new ConcurrentLinkedQueue<>();

        try (BifroRE engine = new BifroRE(
            baseOptions(rulePath, clientIdsDir, "itest-protobuf-" + UUID.randomUUID())
                .payloadFormat(BifroRE.PAYLOAD_PROTOBUF)
                .protobufDescriptorSetPath(descriptorPath)
                .protobufMessageName("example.telemetry.Envelope")
                .directPollSlotCount(2)
                .directPayloadBufferBytes(1024 * 256)
        )) {
            engine.onNextAsyncDirect((ruleIndex, payloadBuffer, offset, length, metadata) -> {
                try {
                    byte[] bytes = new byte[length];
                    ByteBuffer view = payloadBuffer.asReadOnlyBuffer();
                    view.position(offset);
                    view.limit(offset + length);
                    view.get(bytes);
                    payloads.add(MAPPER.readTree(bytes));
                    latch.countDown();
                    return CompletableFuture.completedFuture(null);
                } catch (Exception error) {
                    CompletableFuture<Void> failed = new CompletableFuture<>();
                    failed.completeExceptionally(error);
                    return failed;
                }
            });
            assertEquals(0, engine.start());
            awaitSubscriptionWarmup();

            byte[] payload = buildTelemetryPayload("sensor-a", 23.5, 61.0, true, "factory-a");

            publish("data", payload, List.of());
            assertTrue(latch.await(AWAIT_TIMEOUT.toSeconds(), TimeUnit.SECONDS));
        }

        assertEquals(1, payloads.size());
        JsonNode projected = payloads.peek();
        assertEquals("sensor-a", projected.get("device").asText());
        assertEquals(23.5, projected.get("temperature").asDouble());
        assertEquals(61.0, projected.get("humidity").asDouble());
        assertEquals("factory-a", projected.get("site").asText());
    }

    @Test
    void persistentSessionReceivesQueuedAndLiveMessages() throws Exception {
        Path clientIdsDir = Files.createTempDirectory("bifrore-itest-session-");
        String groupName = "test-session-" + UUID.randomUUID();
        String rulePath = writeRuleFile("""
            {
              "rules": [
                {
                  "expression": "select temp, hum from data where temp >= 20",
                  "destinations": ["persist"]
                }
              ]
            }
            """);

        try (BifroRE engine = new BifroRE(
            baseOptions(rulePath, clientIdsDir, groupName)
                .cleanStart(false)
                .sessionExpiryInterval(3600)
        )) {
            CountDownLatch latch = new CountDownLatch(2);
            AtomicInteger callbackCount = new AtomicInteger();
            Set<Integer> temperatures = ConcurrentHashMap.newKeySet();
            engine.onNext((ruleIndex, payloadBlob, offset, length, metadata) -> {
                try {
                    JsonNode payload = MAPPER.readTree(payloadBlob, offset, length);
                    temperatures.add(payload.get("temp").asInt());
                    callbackCount.incrementAndGet();
                    latch.countDown();
                } catch (Exception error) {
                    throw new RuntimeException(error);
                }
            });
            assertEquals(0, engine.start());
            awaitSubscriptionWarmup();
            assertEquals(0, engine.stop());
            publish("data", "{\"temp\":21,\"hum\":50}".getBytes(StandardCharsets.UTF_8), List.of());
            assertEquals(0, engine.start());
            awaitSubscriptionWarmup();
            publish("data", "{\"temp\":22,\"hum\":51}".getBytes(StandardCharsets.UTF_8), List.of());
            assertTrue(latch.await(AWAIT_TIMEOUT.toSeconds(), TimeUnit.SECONDS));
            assertEquals(2, callbackCount.get());
            assertEquals(Set.of(21, 22), temperatures);
        }
    }

    private static BifroREOptions baseOptions(String rulePath, Path clientIdsDir, String groupName) {
        return new BifroREOptions()
            .host("127.0.0.1")
            .port(1883)
            .ruleJsonPath(rulePath)
            .clientIdsPath(clientIdsDir.toString())
            .groupName(groupName)
            .callbackQueueCapacity(64)
            .pollBatchLimit(16);
    }

    private static void publish(String topic, byte[] payload, List<UserProperty> userProperties)
        throws MqttException {
        String clientId = "bifrore-itest-publisher-" + System.nanoTime();
        MqttClient client = new MqttClient(BROKER_URI, clientId);
        MqttConnectionOptions options = new MqttConnectionOptionsBuilder()
            .cleanStart(true)
            .sessionExpiryInterval(0L)
            .build();
        client.connect(options);
        try {
            MqttMessage message = new MqttMessage(payload);
            message.setQos(1);
            MqttProperties properties = new MqttProperties();
            properties.setUserProperties(userProperties);
            message.setProperties(properties);
            client.publish(topic, message);
        } finally {
            client.disconnect();
            client.close();
        }
    }

    private static void awaitSubscriptionWarmup() throws InterruptedException {
        Thread.sleep(1500L);
    }

    private static String writeRuleFile(String content) throws Exception {
        Path tempFile = Files.createTempFile("bifrore-itest-rule-", ".json");
        Files.writeString(tempFile, content, StandardCharsets.UTF_8);
        tempFile.toFile().deleteOnExit();
        return tempFile.toAbsolutePath().toString();
    }

    private static String extractResource(String resourcePath, String prefix, String suffix) throws Exception {
        try (InputStream input = BifroREIntegrationTest.class.getResourceAsStream(resourcePath)) {
            if (input == null) {
                throw new IllegalStateException("missing resource: " + resourcePath);
            }
            Path tempFile = Files.createTempFile(prefix, suffix);
            Files.copy(input, tempFile, StandardCopyOption.REPLACE_EXISTING);
            tempFile.toFile().deleteOnExit();
            return tempFile.toAbsolutePath().toString();
        }
    }

    private static byte[] buildTelemetryPayload(
        String device,
        double temp,
        double hum,
        boolean online,
        String site
    ) throws Exception {
        DescriptorProtos.FileDescriptorSet descriptorSet;
        try (InputStream input = BifroREIntegrationTest.class.getResourceAsStream("/proto/telemetry.desc")) {
            if (input == null) {
                throw new IllegalStateException("missing resource: /proto/telemetry.desc");
            }
            descriptorSet = DescriptorProtos.FileDescriptorSet.parseFrom(input);
        }
        Descriptors.Descriptor envelopeDescriptor =
            findMessageDescriptor(buildFileDescriptorMap(descriptorSet), "example.telemetry.Envelope");
        Descriptors.Descriptor sensorDescriptor = envelopeDescriptor.findFieldByName("sensor").getMessageType();
        Descriptors.Descriptor readingDescriptor = sensorDescriptor.findFieldByName("reading").getMessageType();

        DynamicMessage reading = DynamicMessage.newBuilder(readingDescriptor)
            .setField(readingDescriptor.findFieldByName("temp"), temp)
            .setField(readingDescriptor.findFieldByName("hum"), hum)
            .setField(readingDescriptor.findFieldByName("online"), online)
            .build();
        DynamicMessage sensor = DynamicMessage.newBuilder(sensorDescriptor)
            .setField(sensorDescriptor.findFieldByName("device"), device)
            .setField(sensorDescriptor.findFieldByName("reading"), reading)
            .build();
        return DynamicMessage.newBuilder(envelopeDescriptor)
            .setField(envelopeDescriptor.findFieldByName("sensor"), sensor)
            .setField(envelopeDescriptor.findFieldByName("site"), site)
            .build()
            .toByteArray();
    }

    private static Map<String, Descriptors.FileDescriptor> buildFileDescriptorMap(
        DescriptorProtos.FileDescriptorSet descriptorSet
    ) throws Exception {
        java.util.HashMap<String, DescriptorProtos.FileDescriptorProto> protoByName = new java.util.HashMap<>();
        for (DescriptorProtos.FileDescriptorProto proto : descriptorSet.getFileList()) {
            protoByName.put(proto.getName(), proto);
        }
        java.util.HashMap<String, Descriptors.FileDescriptor> built = new java.util.HashMap<>();
        for (DescriptorProtos.FileDescriptorProto proto : descriptorSet.getFileList()) {
            buildFileDescriptor(proto.getName(), protoByName, built);
        }
        return built;
    }

    private static Descriptors.FileDescriptor buildFileDescriptor(
        String name,
        Map<String, DescriptorProtos.FileDescriptorProto> protoByName,
        Map<String, Descriptors.FileDescriptor> built
    ) throws Exception {
        Descriptors.FileDescriptor existing = built.get(name);
        if (existing != null) {
            return existing;
        }
        DescriptorProtos.FileDescriptorProto proto = protoByName.get(name);
        if (proto == null) {
            throw new IllegalStateException("missing descriptor proto: " + name);
        }
        Descriptors.FileDescriptor[] dependencies = new Descriptors.FileDescriptor[proto.getDependencyCount()];
        for (int i = 0; i < proto.getDependencyCount(); i++) {
            dependencies[i] = buildFileDescriptor(proto.getDependency(i), protoByName, built);
        }
        Descriptors.FileDescriptor fileDescriptor =
            Descriptors.FileDescriptor.buildFrom(proto, dependencies);
        built.put(name, fileDescriptor);
        return fileDescriptor;
    }

    private static Descriptors.Descriptor findMessageDescriptor(
        Map<String, Descriptors.FileDescriptor> files,
        String fullName
    ) {
        for (Descriptors.FileDescriptor file : files.values()) {
            for (Descriptors.Descriptor message : file.getMessageTypes()) {
                Descriptors.Descriptor found = findMessageDescriptor(message, fullName);
                if (found != null) {
                    return found;
                }
            }
        }
        throw new IllegalStateException("missing descriptor: " + fullName);
    }

    private static Descriptors.Descriptor findMessageDescriptor(
        Descriptors.Descriptor descriptor,
        String fullName
    ) {
        if (descriptor.getFullName().equals(fullName)) {
            return descriptor;
        }
        for (Descriptors.Descriptor nested : descriptor.getNestedTypes()) {
            Descriptors.Descriptor found = findMessageDescriptor(nested, fullName);
            if (found != null) {
                return found;
            }
        }
        return null;
    }
}
