# Vertex Engine Reconnection Issue — Reproduction

A restarted Vertex node can send transactions to the cluster but never receives events back. Consensus continues among the remaining nodes but the restarted node is permanently frozen.

## Steps to reproduce

```bash
cargo run
```

## What the repro does

1. Starts 4 Vertex engines as separate OS processes (f=1 fault tolerance)
2. Waits for consensus — all 4 nodes exchange hello transactions successfully
3. Kills node-3
4. Waits 15 seconds (node-3 gets kicked via `fallen_behind_kick_s(10)`)
5. Restarts node-3 with the same keypair, port, and peer list
6. Node-3 sends a hello — the other 3 nodes receive it through consensus
7. Waits 60 seconds — node-3 never receives any events

## Expected behavior

After restart, node-3 should receive consensus events from other nodes and fully participate in the cluster.

## Actual behavior

- Node-3 gets SyncPoint #1 on boot, sends its hello transaction
- Other nodes receive node-3's hello (consensus processes it)
- Node-3 never receives another SyncPoint or Event
- Consensus continues normally among nodes 0, 1, 2 (they exchange pings and get further SyncPoints)
- Node-3 is permanently frozen — it can inject one transaction but never receives

## Configurations tested

All produce the same result:

| Configuration | Receives events after restart? |
|---|---|
| `Options::default()` | No |
| `fallen_behind_kick_s(10)` | No |
| `fallen_behind_kick_s(-1)` (never kick) | No |
| `enable_state_sharing(true)` + `epoch_states_to_cache(3)` | No |
| `fallen_behind_kick_s(10)` + `enable_state_sharing(true)` + `epoch_states_to_cache(3)` | No |
| `report_gossip_events(true)` + `fallen_behind_kick_s(10)` | No |
| `fallen_behind_kick_s(10)` + `enable_dynamic_epoch_size(false)` | No |

## Environment

- macOS Darwin 25.2.0
- tashi-vertex from `https://github.com/tashigit/tashi-vertex-rs.git`
- 4 engines as separate OS processes, each with its own single-threaded tokio runtime
- Same keypair and port used for the restarted node
