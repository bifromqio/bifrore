# Python bindings

This wrapper calls the Rust C ABI exported by `bifrore-embed-ffi`.

`./build.sh python` builds a platform wheel with the bundled native library, so callers do not
need to manage `libbifrore_embed` manually.

Usage example:
```
import asyncio

from bifrore import BifroRE

async def main():
    async with BifroRE(
        "/path/to/rule.json",
        host="127.0.0.1",
        port=1883,
        node_id=None,
        client_count=2,
    ) as stream:
        async for message in stream:
            print(message.rule_id, message.destinations)

asyncio.run(main())
```

For in-repo direct use, the older explicit-native-path form still works:

```
engine = BifroRE("/path/to/libbifrore_embed.so", "/path/to/rule.json")
```

Log levels: `1=ERROR`, `2=WARN`, `3=INFO`, `4=DEBUG`, `5=TRACE`.

Log callbacks may arrive from native worker threads; pass `loop=...` or `executor=...` to `on_log`
to marshal execution context.
