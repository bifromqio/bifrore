# BifroRE (Embedded MQTT Rule Engine)

BifroRE is now an embedded MQTT rule engine delivered as a Rust library with a C ABI.
It connects to an MQTT broker via shared subscriptions, evaluates SQL-like rules in memory,
and returns evaluation results to the host application via callbacks (JNI / Python / C).

## Architecture

```mermaid
flowchart LR
  subgraph Host[Host Application]
    JNI[JNI Wrapper]
    PY[Python Wrapper]
    CFFI[C ABI]
  end

  subgraph Engine[Embedded Rule Engine (Rust)]
    ADP[MQTT Adapter (v5, shared subscription)]
    RT[Rule Runtime]
    PARSE[SQL Rule Parser]
    EVAL[Rule Evaluator]
    METRICS[Eval Metrics]
  end

  Broker[MQTT Broker]

  Broker -->|shared subscription| ADP
  ADP --> RT
  RT --> EVAL
  PARSE --> RT
  EVAL -->|callback with results| CFFI
  CFFI --> JNI
  CFFI --> PY
```

## Key Differences (Old vs New)

- Old: standalone Java services (router/processor/admin). New: embedded Rust library with C ABI.
- Old: rule matching done in Java router. New: in-memory trie matcher in Rust.
- Old: SQL rules parsed by Trino + MVEL. New: SQL subset parser in Rust with MQTT-native extensions.
- Old: downstream delivery handled by built-in plugins. New: host application handles destinations.

## Rule DSL (SQL + MQTT extensions)

Examples:
- Topic filters and alias:
  - `select * from 'sensors/+/temp' as t`
- Topic level function:
  - `where topic_level(t, 2) = 'room1'`
- Metadata/properties:
  - `where qos >= 1 and properties['content-type'] = 'application/json'`

Supported (current):
- `SELECT *` or `SELECT expr [AS alias]`
- `FROM <topic filter>` with `+` and `#` wildcards
- `WHERE` with `AND/OR`, comparisons, arithmetic
- `topic_level(alias, index)` (1-based index)
- Metadata: `qos`, `retain`, `dup`, `timestamp`, `clientId`, `username`
- MQTT v5 properties via `properties['key']`

## Build

Requirements:
- Rust (cargo)
- For JNI: JDK (JAVA_HOME set)

Build the artifacts:

```bash
./build.sh jni     # Rust cdylib + JNI bridge
./build.sh python  # Rust cdylib only (Python wrapper is pure ctypes)
./build.sh all     # both
./build.sh bench   # run runtime benchmarks
```

Artifacts are placed under `build/`:
- `libbifrore_embed.(so|dylib)`
- `libbifrore_jni.(so|dylib)` (JNI)

## JNI Usage (Java)

```java
BifroRE re = new BifroRE("127.0.0.1", 1883);
re.onMessage((ruleId, payload, destinationsJson) -> {
    // handle evaluated payload + destinations
});
re.loadRules("/path/to/rule.json");
re.start();
```

## Python Usage

```python
from bindings.python.bifrore import BifroRE

engine = BifroRE("./build/libbifrore_embed.dylib")
engine.on_message(lambda rule_id, payload, destinations: print(rule_id, destinations))
engine.load_rules("./rule.json")
engine.start_mqtt("127.0.0.1", 1883, "client-1", "bifrore-group")
```

## Using Pre-built Releases (Users)

When you publish Release assets (Linux/macOS, x86_64/arm64), users can run without building.

1) Download the correct tarball from GitHub Releases and extract it:

```bash
tar -xzf bifrore-embed-<os>-<arch>.tar.gz
```

2) Use the extracted libraries.

Java (JNI):

```java
BifroRE re = new BifroRE("127.0.0.1", 1883);
re.onMessage((ruleId, payload, destinationsJson) -> {
    // handle evaluated payload + destinations
});
re.loadRules("/path/to/rule.json");
re.start();
```

Run with library path pointing to the extracted folder:

```bash
java -Djava.library.path=/path/to/extracted/libs YourApp
```

Python (ctypes):

```python
from bindings.python.bifrore import BifroRE

engine = BifroRE("/path/to/extracted/libs/libbifrore_embed.dylib")
engine.on_message(lambda rule_id, payload, destinations: print(rule_id, destinations))
engine.load_rules("/path/to/rule.json")
engine.start_mqtt("127.0.0.1", 1883, "client-1", "bifrore-group")
```

## Rule File Format

```json
{
  "rules": [
    {
      "expression": "select * from 'sensors/+/temp' as t where topic_level(t, 2) = 'room1'",
      "destinations": ["destA", "destB"]
    }
  ]
}
```

## Benchmarks

A Criterion benchmark is provided at:
- `engine/bifrore-embed-core/benches/runtime_bench.rs`

Run with:

```bash
./build.sh bench
```
