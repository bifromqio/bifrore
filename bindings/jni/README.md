# JNI bindings

This JNI layer calls into the Rust C ABI defined in `bifrore-embed-ffi`.

## Files
- `src/main/java/com/bifrore/BifroRE.java` Java API
- `src/main/c/bifrore_jni.c` JNI bridge calling the C ABI

## Notes
- `./build.sh java` builds a platform jar with bundled native libraries.
- `./build.sh jni` still builds the raw native libraries only.
- The jar extracts `libbifrore_embed` and `libbifrore_jni` to a temp directory and loads them automatically.
- Logging can be wired with `BifroRE.onLog(handler, minLevel)` where levels are `1=ERROR`, `2=WARN`, `3=INFO`, `4=DEBUG`, `5=TRACE`.
- `onMessage` and `onLog` support custom `Executor`; if omitted, a default single-thread executor is used.
- Rules are loaded during `new BifroRE(host, port, ruleJsonPath)`; `loadRules`/`eval` are not part of public API.
- For multiple MQTT clients, use `new BifroRE(host, port, ruleJsonPath, nodeId, clientCount, multiNci)`.
- Runtime client ids are loaded from the client-id file if present; otherwise BifroRE generates plain `nodeId_index` defaults.
