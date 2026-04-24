# Java Example

This example shows how to consume the generated `bifrore-java.jar` from a Maven project.

## 1. Build the jar

```bash
./build.sh java
```

This produces:

```text
build/bifrore-java.jar
```

## 2. Install the jar into your local Maven repository

```bash
mvn install:install-file \
  -Dfile=build/bifrore-java.jar \
  -DgroupId=com.bifrore \
  -DartifactId=bifrore-java \
  -Dversion=0.1.0 \
  -Dpackaging=jar
```

## 3. Add the dependency to your `pom.xml`

See `examples/java/pom.xml`.

## 4. Use the API

Two example entrypoints are provided:

- Heap-based JNI path: `examples/java/src/main/java/com/example/AppHeap.java`
- Direct-buffer JNI path with async Kafka forwarding and protobuf payload decoding:
  `examples/java/src/main/java/com/example/AppDirectKafka.java`

The example exposes Prometheus metrics on:

```text
http://127.0.0.1:9464/metrics
```

The Java bindings provide:

- `BifroREOptions` for engine configuration
- `bindMetrics(...)` for binding all SDK metrics to a Micrometer registry
- `BifroREMetricsView` for low-level lazy scrape-triggered metric reads
- `disconnect()` to stop MQTT intake before final `close()`

The example calls `engine.bindMetrics(registry)`. Metrics are pulled lazily on scrape.
No dedicated metrics polling thread is used.
`bindMetrics(...)` accepts a Micrometer `MeterRegistry`; use `PrometheusMeterRegistry`
for Prometheus or another Micrometer registry for OTLP, Datadog, StatsD, JMX, etc.
Monotonic totals are bound as Micrometer function counters, while depths and max values
are bound as gauges.

Backpressure-related knobs in `BifroREOptions`:

- `callbackQueueCapacity(...)` for the default Java callback executor queue
- `pollBatchLimit(...)` for the Rust-to-Java batch drain cap
- `directPollSlotCount(...)` for the Java direct-buffer slot pool
- `directPayloadBufferBytes(...)` for each direct payload buffer capacity

## Heap vs direct example

`AppHeap` uses the heap-oriented callback:

- `onNext((ruleIndex, payloadBlob, offset, length, metadata) -> { ... })`

The callback reads the payload slice directly from the shared heap batch blob.

`AppDirectKafka` uses the direct-buffer async callback:

- `onNextAsyncDirect((ruleIndex, payloadBuffer, offset, length, metadata) -> CompletionStage<?>)`

The direct example also configures protobuf payload decoding at engine init with:

- `payloadFormat(BifroRE.PAYLOAD_PROTOBUF)`
- `protobufDescriptorSetPath(...)`

The nested protobuf schema source lives at:

- `examples/java/src/main/proto/telemetry.proto`

Important contract for the direct callback:

- the SDK-owned direct buffer is valid only until the returned `CompletionStage` completes
- if user code forwards the message asynchronously, it must copy the slice into caller-owned heap memory first
- the example copies the slice into `byte[] retainedPayload` before calling Kafka `send(...)`

That direct example demonstrates both:

- SDK responsibility: keep the direct-buffer slot alive until `CompletionStage` completion
- user responsibility: copy the payload if downstream async code retains it beyond the callback body
