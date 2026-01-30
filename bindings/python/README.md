# Python bindings (ctypes scaffold)

This wrapper calls the Rust C ABI exported by `bifrore-embed-ffi`.

Usage example:
```
from bifrore import BifroRE

engine = BifroRE("/path/to/libbifrore_embed.so")
engine.on_message(lambda rule_id, payload, destinations: print(rule_id, destinations))
engine.load_rules("/path/to/rule.json")
engine.start_mqtt("127.0.0.1", 1883, "client-1", "bifrore-group")
```
