# JNI Notes

`BifroRE` is a JVM-wide singleton in the Java binding.

## Singleton contract

- Only one `com.bifrore.BifroRE` instance is allowed in the same JVM.
- Construction is guarded in `BifroRE.java`.
- The singleton is released when `close()` completes.

## Why this exists

The direct-buffer JNI path keeps one pending packed batch on the JNI side and assumes:

- one engine instance per JVM
- one poller thread owns direct polling

This keeps the JNI fast path simple:

- no per-instance native registry
- no mutex around pending packed-batch state
- fixed direct-buffer capacities come from Java `PollSlot`

## Direct path ownership

- Rust remains language-neutral and only exposes packed poll results.
- JNI copies packed results into Java-owned direct buffers.
- Java manages direct-buffer slots and ties slot reuse to callback completion.
