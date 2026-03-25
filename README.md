# Vertex Engine Drop Segfault Reproduction

Reproduces a segmentation fault when dropping a `tashi_vertex::Engine` while other engines on separate threads are still running.

## Steps to reproduce

```bash
cargo run
```

## Expected behavior

Engine can be cleanly dropped while other engines continue operating.

## Actual behavior

Process crashes with `SIGSEGV` (segmentation fault) when the dropped engine's `tv_free` is called.

## What the repro does

1. Generates 3 Ed25519 keypairs
2. Starts 3 Vertex engines on separate OS threads (each with `tokio::runtime::Builder::new_current_thread()`)
3. Each engine sends a "hello" transaction after receiving its first SyncPoint
4. Waits 5 seconds for consensus to establish and transactions to flow
5. Signals node-2 to stop — the thread returns, dropping the `Engine` (which calls `tv_free`)
6. Segfault occurs during or shortly after the drop

## Findings

The segfault occurs **regardless of configuration options**:

| Configuration | Result |
|---|---|
| `Options::default()` (pure defaults) | Segfault (exit code 139) |
| `set_fallen_behind_kick_s(10)` only | Segfault |
| `set_heartbeat_us(50_000)` only | Segfault |
| `set_base_min_event_interval_us(10_000)` only | Segfault |
| All three combined | Segfault |

With faster heartbeat/event intervals the crash is immediate. With defaults (500ms heartbeat) it takes slightly longer but still crashes.

This is not a configuration issue — `tv_free` in the C library does not support concurrent engine teardown while other engines are still running and gossiping.

## Environment

- macOS Darwin 25.2.0
- tashi-vertex from `https://github.com/tashigit/tashi-vertex-rs.git`
- 3 engines running on separate OS threads, each with its own single-threaded tokio runtime
