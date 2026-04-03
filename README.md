# Vertex Reconnection Issue — Reproduction

Reproduction harness for a reconnection bug in `tashi-vertex 0.13.0`.

After a node is killed and restarted with `joining=true` (before the idle kick
threshold), it fails to participate in live consensus. State replay is
intermittent, and live event delivery never works.

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
| Replay | Missed state is recovered via state sharing | Restarted node sees `offline:*` events |
| Inbound | Live consensus resumes after catch-up | Restarted node sees a `live:*` event |
| Outbound | Restarted node can send after catch-up | A surviving node receives `rejoin:node-1` |

## Test results (5 consecutive runs)

| Run | Replay (`offline:*`) | Live (`live:*`) | Outbound (`rejoin:`) | Engine closed? |
|-----|---------------------|----------------|---------------------|---------------|
| 1 | Received all 3 | Never received | Not checked | No |
| 2 | Never received | Never received | Not checked | No |
| 3 | Never received | Never received | Not checked | No |
| 4 | Received all 3 | Never received | Received (during replay) | Yes — closed after SyncPoint #2 |
| 5 | Received all 3 | Never received | Not checked | No |

**Result: 5/5 runs failed.** Live consensus never resumes after reconnection.

### Key observations

1. **State replay is intermittent** — in 3 out of 5 runs the restarted node
   successfully replayed missed `offline:*` events; in 2 runs it received
   nothing at all after SyncPoint #1.

2. **Live consensus never works** — across all 5 runs, the restarted node
   never received a single `live:*` event. The node is completely stuck after
   catch-up completes.

3. **Engine sometimes closes unexpectedly** — in run 4, the engine shut down
   after SyncPoint #2 instead of continuing to deliver live events. The
   `rejoin:node-1` transaction was received by surviving nodes (confirming
   outbound worked during replay), but the engine closed before any live
   traffic could arrive.

4. **The problem is unidirectional during replay** — when replay works, the
   restarted node can send transactions that other nodes receive (run 4 shows
   `rejoin:node-1` delivered to peers). But after the replay window closes,
   both inbound and outbound stop.

## Expected (passing)

```
[PASS] Replay + Inbound: restarted node received both
       "offline:" (replay) and "live:" (inbound) events
[PASS] Outbound: node-0 received "rejoin:node-1" from the restarted node
```

## Options

All nodes use identical options:

```rust
let mut options = Options::default();
options.set_fallen_behind_kick_s(120);
options.set_enable_state_sharing(true);
options.set_epoch_states_to_cache(10);
```
