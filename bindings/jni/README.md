# JNI bindings (scaffold)

This JNI layer calls into the Rust C ABI defined in `bifrore-embed-ffi`.

## Files
- `src/main/java/com/bifrore/BifroRE.java` Java API
- `src/main/c/bifrore_jni.c` JNI bridge calling the C ABI

## Notes
- Build the Rust cdylib (`libbifrore_embed.dylib` / `.so` / `.dll`) and load it on the JVM library path.
- The JNI C code expects the Rust library to export the `bre_*` symbols.
- This is a scaffold; integrate into your Java build (Gradle/Maven) and generate JNI headers as needed.
- Logging can be wired with `BifroRE.onLog(handler, minLevel)` where levels are `1=ERROR`, `2=WARN`, `3=INFO`, `4=DEBUG`, `5=TRACE`.
- `onMessage` and `onLog` support custom `Executor`; if omitted, a default single-thread executor is used.
- Rules are loaded during `new BifroRE(host, port, ruleJsonPath)`; `loadRules`/`eval` are not part of public API.
- For multiple MQTT clients, use `new BifroRE(host, port, ruleJsonPath, clientPrefix, nodeId, clientCount)`.
- Runtime client ids follow `node_id_client_prefix_<index>`; when `nodeId` is null/empty, Rust generates one.
