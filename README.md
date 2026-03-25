# Vertex Engine Drop Segfault — Reproduction and Fix

Demonstrates a segmentation fault when dropping a `tashi_vertex::Engine` in-process while other engines are still running, and proves the fix: running each engine as a separate OS process.

## Run the test

```bash
cargo run
```

This spawns 3 Vertex engines as separate child processes, establishes consensus, kills one process, and verifies the remaining two are stable. Expected output ends with:

```
[node-0] still running OK
[node-1] still running OK

No segfault. Cleaning up...
Done.
```

## The bug

When multiple Vertex engines run as **threads within a single process**, dropping one engine (via `tv_free` in the C library) while others are still gossiping causes a segmentation fault (SIGSEGV, exit code 139).

This occurs **regardless of configuration options**:

| Configuration | Result (in-process threads) |
|---|---|
| `Options::default()` | Segfault |
| `set_fallen_behind_kick_s(10)` only | Segfault |
| `set_heartbeat_us(50_000)` only | Segfault |
| `set_base_min_event_interval_us(10_000)` only | Segfault |

## The fix

Run each engine as a **separate OS process**. The Vertex C library's `tv_free` is safe when the engine is the only one in the process. Killing the process (SIGKILL) cleanly deallocates everything without affecting other engines in other processes.

This matches the intended usage pattern shown in the [official warmup template](https://github.com/tashigit/warmup-vertex-rust), which runs each node as a separate `cargo run` invocation.

## Environment

- macOS Darwin 25.2.0
- tashi-vertex from `https://github.com/tashigit/tashi-vertex-rs.git`
