package bifrore.destination.plugin;

import bifrore.commontype.Message;
import bifrore.monitoring.metrics.SysMeter;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.binder.jvm.ExecutorServiceMetrics;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

import static bifrore.monitoring.metrics.SysMetric.DestinationMissCount;

@Slf4j
public class BuiltinKafkaProducer implements IProducer{
    private final Map<String, Producer<byte[], byte[]>> callers = new HashMap<>();
    private final Executor ioExecutor;
    private final String kafkaDelimiter = ".";

    public BuiltinKafkaProducer() {
        this.ioExecutor = ExecutorServiceMetrics.monitor(Metrics.globalRegistry,
                Executors.newFixedThreadPool(1), "bifrore-kafka-flush");
    }

    @Override
    public CompletableFuture<Void> produce(Message message, String callerId) {
        CompletableFuture<Void> future = new CompletableFuture<>();
        Producer<byte[], byte[]> producer = callers.get(callerId);
        if (producer == null) {
            SysMeter.INSTANCE.recordCount(DestinationMissCount);
            log.warn("No producer found for callerId={}, silently return", callerId);
            future.complete(null);
            return future;
        }
        ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(
                message.getTopic().replace(DELIMITER, kafkaDelimiter), null, message.getPayload().toByteArray());
        producer.send(record, (metadata, exception) -> {
            if (exception != null) {
                log.error("Failed to send message", exception);
                future.completeExceptionally(exception);
            }else {
                future.complete(null);
            }
        });
        return future;
    }

    @Override
    public CompletableFuture<String> initCaller(Map<String, String> callerCfgMap) {
        String callerId = createProducerInstance(callerCfgMap, Optional.empty());
        return CompletableFuture.completedFuture(callerId);
    }

    @Override
    public CompletableFuture<Void> syncCaller(String callerId, Map<String, String> callerCfgMap) {
        if (!callers.containsKey(callerId)) {
            createProducerInstance(callerCfgMap, Optional.of(callerId));
        }
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<Void> closeCaller(String callerId) {
        CompletableFuture<Void> future = new CompletableFuture<>();
        Producer<byte[], byte[]> producer = callers.remove(callerId);
        if (producer != null) {
            CompletableFuture.runAsync(producer::flush, this.ioExecutor)
                    .whenComplete((v, e) -> {
                        producer.close();
                        if (e != null) {
                            future.completeExceptionally(e);
                        }else {
                            future.complete(null);
                        }
                    });
        }else {
            future.completeExceptionally(new IllegalStateException("Caller not found: " + callerId));
        }
        return future;
    }

    @Override
    public String getName() {
        return "kafka";
    }

    @Override
    public void close() {
        List<CompletableFuture<Void>> futures = new ArrayList<>();
        List<String> keys = new ArrayList<>(callers.keySet());
        keys.forEach(callerId -> futures.add(closeCaller(callerId)));
        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
    }

    private String createProducerInstance(Map<String, String> callerCfgMap, Optional<String> callerIdPresent) {
        String callerId = callerIdPresent.orElseGet(() -> this.getName() + IProducer.DELIMITER + UUID.randomUUID());
        Properties props = new Properties();
        props.putAll(callerCfgMap);
        props.putIfAbsent(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        props.putIfAbsent(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        props.put(ProducerConfig.CLIENT_ID_CONFIG, callerId);
        props.putIfAbsent(ProducerConfig.LINGER_MS_CONFIG, 5);
        callers.putIfAbsent(callerId, new KafkaProducer<>(props));
        return callerId;
    }
}
