# Vertex Engine Drop Segfault Reproduction

Reproduces a segmentation fault when dropping a `tashi_vertex::Engine` while other engines on separate threads are still running.

## Steps to reproduce

```bash
cargo run
```

## Expected behavior

Engine can be cleanly dropped while other engines continue operating.

## Actual behavior

Process crashes with `SIGSEGV` (segmentation fault) when the dropped engine's `tv_free` is called, presumably because other engines are still gossiping with it.

## Environment

- macOS (Darwin)
- tashi-vertex from `https://github.com/tashigit/tashi-vertex-rs.git`
- 3 engines running on separate threads, each with its own single-threaded tokio runtime

## What the repro does

1. Generates 3 Ed25519 keypairs
2. Starts 3 Vertex engines on separate OS threads (each with `tokio::runtime::Builder::new_current_thread()`)
3. Each engine sends a "hello" transaction after receiving its first SyncPoint
4. Waits 5 seconds for consensus to establish and transactions to flow
5. Signals node-2 to stop — the thread returns, dropping the `Engine` (which calls `tv_free`)
6. Segfault occurs during or shortly after the drop
