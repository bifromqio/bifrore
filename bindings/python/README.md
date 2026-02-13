# Python bindings (ctypes scaffold)

This wrapper calls the Rust C ABI exported by `bifrore-embed-ffi`.

Usage example:
```
from bifrore import BifroRE

engine = BifroRE("/path/to/libbifrore_embed.so")
engine.on_message(lambda rule_id, payload, destinations: print(rule_id, destinations))
engine.on_log(
    lambda level, target, message, ts, thread_id, module_path, file, line:
        print(level, target, message, ts, thread_id, module_path, file, line),
    min_level=3
)
engine.load_rules("/path/to/rule.json")
engine.start_mqtt("127.0.0.1", 1883, "client-1", "bifrore-group")
```

Log levels: `1=ERROR`, `2=WARN`, `3=INFO`, `4=DEBUG`, `5=TRACE`.

Callbacks may arrive from native worker threads; pass `loop=...` or `executor=...` to `on_message`/`on_log` to marshal execution context.
