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

See `examples/java/src/main/java/com/example/App.java`.

The example exposes Prometheus metrics on:

```text
http://127.0.0.1:9464/metrics
```

The Java bindings provide:

- `BifroREOptions` for engine configuration
- `BifroREMetricsView` for lazy scrape-triggered metric reads

The example binds `BifroREMetricsView` to Micrometer gauges. Metrics are pulled lazily on scrape.
No dedicated metrics polling thread is used.
