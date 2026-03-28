# Vertex Reconnection Issue — Minimal Reproduction

A restarted Vertex node can **send** transactions but **never receives** events back, even with state sharing enabled.

## Quick start

```bash
cargo run
```

## What it does

1. Starts 4 Vertex nodes as separate OS processes (f=1 fault tolerance)
2. All 4 nodes reach consensus and exchange hello transactions
3. Kills node-3
4. Waits 15s for the remaining nodes to kick node-3 (`fallen_behind_kick_s=10`)
5. Restarts node-3 with the same keypair, port, and peer list
6. Node-3 sends a hello — **other nodes receive it**
7. Node-3 **never receives any events back**

## Options used

All nodes use identical options:

```rust
let mut options = Options::default();
options.set_fallen_behind_kick_s(10);
options.set_enable_state_sharing(true);
options.set_epoch_states_to_cache(10);
```

`dynamic_epoch_size` defaults to `true`. Values of `epoch_states_to_cache` from `3` (default) up to `50` were tested with no difference.

## Expected

After restart, node-3 downloads missed state from peers and resumes receiving consensus events.

## Actual

- Node-3 boots and gets SyncPoint #1
- Node-3 sends a hello transaction — other nodes receive it via consensus
- Node-3 never receives another SyncPoint or Event
- The other 3 nodes continue consensus normally among themselves
- This is a **unidirectional** problem: outbound works, inbound is broken

## Environment

- macOS Darwin 25.2.0
- `tashi-vertex` from https://github.com/tashigit/tashi-vertex-rs.git
- 4 engines as separate OS processes, each with its own single-threaded tokio runtime
- Same keypair and port reused for the restarted node
