# Vertex Restart-After-Eviction Repro

Reproduction harness for restart/rejoin failures in `tashi-vertex 0.13.0`.

## Quick start

```bash
cargo run
```

## What it does

1. Starts 4 Vertex nodes as separate OS processes
2. Waits 10 s for consensus to establish
3. Kills node-1
4. Leaves it down for 10 s (well within `fallen_behind_kick_s=120`, so the swarm has **not** evicted it)
5. Restarts node-1 with `joining=true`, same keypair and port — before the kick threshold

The other three nodes send transactions at scripted times:

- `pre:*` while all 4 nodes are alive
- `offline:*` while node-1 is down
- `live:*` after node-1 restarts

The test verifies three things about the restarted node:

| Check | What it proves | How |
|-------|---------------|-----|
| Replay | Missed state is recovered via state sharing | Restarted node sees an `offline:*` event |
| Inbound | Live consensus resumes | Restarted node sees a `live:*` event |
| Outbound | Restarted node can send | A surviving node receives `hello:node-1` |

## Expected (passing)

```
[PASS] Replay + Inbound: restarted node received both
       "offline:" (replay) and "live:" (inbound) events
[PASS] Outbound: node-0 received "hello:node-1" from the restarted node
```

## Actual failure (issue reproduces)

```
[FAIL] ISSUE REPRODUCED: restarted node timed out waiting for events.
```

The restarted node never receives any events and exits with code 42 after timing out.

## Options

All nodes use identical options:

```rust
let mut options = Options::default();
options.set_fallen_behind_kick_s(120);
options.set_enable_state_sharing(true);
options.set_epoch_states_to_cache(10);
```
