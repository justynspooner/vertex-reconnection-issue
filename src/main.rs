mod node;

use std::io::{BufRead, BufReader};
use std::process::{Command, Stdio};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use tashi_vertex::KeySecret;

const ADDRS: [&str; 4] = [
    "127.0.0.1:9000",
    "127.0.0.1:9001",
    "127.0.0.1:9002",
    "127.0.0.1:9003",
];

/// Hard ceiling so the test never hangs indefinitely.
const TEST_TIMEOUT_S: u64 = 120;

// ─── Entry point ────────────────────────────────────────────────────────────

fn main() {
    let args: Vec<String> = std::env::args().collect();

    // When invoked as a child process, run a single node.
    if args.len() > 1 && args[1] == "node" {
        return run_node(&args[2..]);
    }

    // Otherwise run the test orchestrator.
    run_test();
}

// ─── Child process: parse args and run one node ─────────────────────────────

fn run_node(args: &[String]) {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_writer(std::io::stderr)
        .init();

    let mut bind = String::new();
    let mut secret = String::new();
    let mut label = String::new();
    let mut joining = false;
    let mut peer_addrs = Vec::new();
    let mut peer_pubkeys = Vec::new();
    let mut send_after_ms: Vec<u64> = Vec::new();
    let mut send_payloads: Vec<String> = Vec::new();
    let mut watch_prefixes: Vec<String> = Vec::new();
    let mut watch_timeout_s: u64 = 0;

    let mut i = 0;
    while i < args.len() {
        match args[i].as_str() {
            "--bind" => {
                bind = args[i + 1].clone();
                i += 2;
            }
            "--secret" => {
                secret = args[i + 1].clone();
                i += 2;
            }
            "--label" => {
                label = args[i + 1].clone();
                i += 2;
            }
            "--joining" => {
                joining = true;
                i += 1;
            }
            "--peer-addr" => {
                peer_addrs.push(args[i + 1].clone());
                i += 2;
            }
            "--peer-pubkey" => {
                peer_pubkeys.push(args[i + 1].clone());
                i += 2;
            }
            "--send-after-ms" => {
                send_after_ms.push(args[i + 1].parse().unwrap());
                i += 2;
            }
            "--send-payload" => {
                send_payloads.push(args[i + 1].clone());
                i += 2;
            }
            "--watch-prefix" => {
                watch_prefixes.push(args[i + 1].clone());
                i += 2;
            }
            "--watch-timeout-s" => {
                watch_timeout_s = args[i + 1].parse().unwrap();
                i += 2;
            }
            other => {
                eprintln!("WARNING: unknown node argument ignored: {other}");
                i += 1;
            }
        }
    }

    assert_eq!(
        send_after_ms.len(),
        send_payloads.len(),
        "--send-after-ms and --send-payload must be paired"
    );

    let scheduled_sends: Vec<(u64, String)> =
        send_after_ms.into_iter().zip(send_payloads).collect();
    let peers: Vec<(String, String)> = peer_addrs.into_iter().zip(peer_pubkeys).collect();

    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();
    rt.block_on(node::run(
        &bind,
        &secret,
        &label,
        joining,
        scheduled_sends,
        watch_prefixes,
        watch_timeout_s,
        peers,
    ));
}

// ─── Spawning child node processes ──────────────────────────────────────────

struct SpawnedNode {
    child: std::process::Child,
    /// Captured stdout lines — used by the parent to verify outbound events.
    stdout_lines: Arc<Mutex<Vec<String>>>,
}

fn spawn_node(
    exe: &std::path::Path,
    i: usize,
    secrets: &[String],
    pubkeys: &[String],
    joining: bool,
    scheduled_sends: &[(u64, &str)],
    watch_prefixes: &[&str],
    watch_timeout_s: u64,
) -> SpawnedNode {
    let mut cmd = Command::new(exe);
    cmd.arg("node")
        .arg("--bind")
        .arg(ADDRS[i])
        .arg("--secret")
        .arg(&secrets[i])
        .arg("--label")
        .arg(format!("node-{i}"));

    if joining {
        cmd.arg("--joining");
    }

    for (after_ms, payload) in scheduled_sends {
        cmd.arg("--send-after-ms")
            .arg(after_ms.to_string())
            .arg("--send-payload")
            .arg(payload);
    }

    for prefix in watch_prefixes {
        cmd.arg("--watch-prefix").arg(prefix);
    }
    if watch_timeout_s > 0 {
        cmd.arg("--watch-timeout-s")
            .arg(watch_timeout_s.to_string());
    }
    for j in 0..ADDRS.len() {
        if j == i {
            continue;
        }
        cmd.arg("--peer-addr")
            .arg(ADDRS[j])
            .arg("--peer-pubkey")
            .arg(&pubkeys[j]);
    }

    cmd.env("RUST_LOG", "info");
    cmd.stdout(Stdio::piped()).stderr(Stdio::piped());

    let mut child = cmd.spawn().expect("failed to spawn node");

    let stdout = child.stdout.take().unwrap();
    let stderr = child.stderr.take().unwrap();
    let stdout_lines = Arc::new(Mutex::new(Vec::<String>::new()));
    let lines_for_thread = stdout_lines.clone();
    let idx = i;

    // Forward stdout, keeping a copy for later inspection.
    thread::spawn(move || {
        let reader = BufReader::new(stdout);
        for line in reader.lines() {
            if let Ok(line) = line {
                println!("[node-{idx}] {line}");
                lines_for_thread.lock().unwrap().push(line);
            }
        }
    });

    // Forward stderr (tracing/log output).
    thread::spawn(move || {
        let reader = BufReader::new(stderr);
        for line in reader.lines() {
            if let Ok(line) = line {
                eprintln!("[node-{idx}:log] {line}");
            }
        }
    });

    SpawnedNode {
        child,
        stdout_lines,
    }
}

// ─── Test orchestrator ──────────────────────────────────────────────────────

fn run_test() {
    // Generate stable keypairs for all 4 nodes.
    let mut secrets = Vec::new();
    let mut pubkeys = Vec::new();
    for _ in 0..ADDRS.len() {
        let key = KeySecret::generate();
        pubkeys.push(key.public().to_string());
        secrets.push(key.to_string());
    }

    let exe = std::env::current_exe().unwrap();

    // ── Scheduled transactions ──────────────────────────────────────────
    //
    // Times are milliseconds after each node's engine starts.
    //
    // Absolute timeline (approximate):
    //   t=0..3   Nodes start (1 s apart, reverse order: 3, 2, 1, 0)
    //   t=13     Kill node-1
    //   t=23     Restart node-1 with joining=true
    //
    // "pre:"      — sent while all 4 nodes are alive
    // "offline:"  — sent while node-1 is down  (must be caught up on reconnect)
    // "live:"     — sent after node-1 restarts  (must arrive via live consensus)

    let schedules: [Vec<(u64, &str)>; 4] = [
        // node-0 (starts at t~3)
        vec![
            (5_000, "pre:node-0"),      //  t~8
            (13_000, "offline:node-0"), //  t~16
            (25_000, "live:node-0"),    //  t~28
        ],
        // node-1 (starts at t~2, killed at t~13 — only the pre: tx fires)
        vec![(5_000, "pre:node-1")],
        // node-2 (starts at t~1)
        vec![
            (5_000, "pre:node-2"),      //  t~6
            (15_000, "offline:node-2"), //  t~16
            (27_000, "live:node-2"),    //  t~28
        ],
        // node-3 (starts at t~0)
        vec![
            (5_000, "pre:node-3"),      //  t~5
            (17_000, "offline:node-3"), //  t~17
            (29_000, "live:node-3"),    //  t~29
        ],
    ];

    // ── Phase 1: Start all 4 nodes ──────────────────────────────────────

    println!("=== Phase 1: Start 4 nodes ===\n");

    let mut nodes: Vec<Option<SpawnedNode>> = vec![None, None, None, None];
    for i in [3, 2, 1, 0usize] {
        println!("  Starting node-{i} on {}", ADDRS[i]);
        nodes[i] = Some(spawn_node(
            &exe,
            i,
            &secrets,
            &pubkeys,
            false, // initial nodes: joining=false
            &schedules[i],
            &[],   // no watch
            0,
        ));
        thread::sleep(Duration::from_secs(1));
    }

    println!("\nWaiting 10 s for consensus to establish...\n");
    thread::sleep(Duration::from_secs(10));

    // ── Phase 2: Kill node-1 ────────────────────────────────────────────

    println!("=== Phase 2: Kill node-1 and leave it down for 10 s ===");
    println!("  (fallen_behind_kick_s = 120, so the swarm has NOT evicted it)\n");

    let mut killed = nodes[1].take().unwrap();
    killed.child.kill().expect("failed to kill node-1");
    killed.child.wait().ok();
    println!("  node-1 killed. Waiting 10 s...\n");
    thread::sleep(Duration::from_secs(10));

    // ── Phase 3: Restart node-1 ─────────────────────────────────────────

    println!("=== Phase 3: Restart node-1 with joining=true ===");
    println!("  Same keypair and port. Watching for:");
    println!("    - \"offline:\" events  (proves replay of missed state)");
    println!("    - \"live:\"    events  (proves live inbound consensus)\n");

    nodes[1] = Some(spawn_node(
        &exe,
        1,
        &secrets,
        &pubkeys,
        true,                        // joining=true
        &[],                         // no scheduled sends
        &["offline:", "live:"],       // watch for these prefixes
        30,                          // timeout 30 s from engine start
    ));

    // Poll for the restarted node's exit, with an overall safety timeout.
    let test_deadline = Instant::now() + Duration::from_secs(TEST_TIMEOUT_S);
    let exit_status = loop {
        if Instant::now() > test_deadline {
            eprintln!("OVERALL TEST TIMEOUT ({TEST_TIMEOUT_S} s) — killing node-1");
            if let Some(ref mut n) = nodes[1] {
                let _ = n.child.kill();
                let _ = n.child.wait();
            }
            break None;
        }
        thread::sleep(Duration::from_secs(1));
        if let Some(ref mut n) = nodes[1] {
            match n.child.try_wait() {
                Ok(Some(status)) => break Some(status),
                Ok(None) => {}
                Err(e) => panic!("failed to poll restarted node-1: {e}"),
            }
        }
    };

    // ── Results ─────────────────────────────────────────────────────────

    println!("\n=== Results ===\n");

    // Check 1: Replay + Inbound (from the restarted node's own exit code).
    let inbound_ok = match exit_status.and_then(|s| s.code()) {
        Some(0) => {
            println!("[PASS] Replay + Inbound: restarted node received both");
            println!("       \"offline:\" (replay) and \"live:\" (inbound) events");
            true
        }
        Some(42) => {
            println!("[FAIL] ISSUE REPRODUCED: restarted node timed out waiting for events.");
            println!("       See [node-1] output above for which prefixes were still missing.");
            false
        }
        None => {
            println!("[FAIL] Test timed out after {TEST_TIMEOUT_S} s without the restarted");
            println!("       node reaching a verdict. The node may be stuck in state sync.");
            false
        }
        code => {
            println!("[FAIL] Unexpected exit code from restarted node: {code:?}");
            false
        }
    };

    // Check 2: Outbound (did any surviving node receive the restarted node's rejoin msg?).
    // The restarted node sends "rejoin:node-1" (not "hello:", to avoid matching the
    // original pre-kill hello) on its first SyncPoint. If any surviving node printed
    // an EVENT line containing that payload, outbound is verified.
    let mut outbound_ok = false;
    for i in [0, 2, 3] {
        if let Some(ref node) = nodes[i] {
            let lines = node.stdout_lines.lock().unwrap();
            if lines.iter().any(|l| l.contains("\"rejoin:node-1\"")) {
                println!("[PASS] Outbound: node-{i} received \"rejoin:node-1\" from the restarted node");
                outbound_ok = true;
                break;
            }
        }
    }
    if !outbound_ok {
        // Only flag as FAIL if the node actually came up (inbound passed).
        // If it never synced, outbound can't be expected to work either.
        if inbound_ok {
            println!("[FAIL] Outbound: no surviving node received \"rejoin:node-1\"");
        } else {
            println!("[SKIP] Outbound: not checked (inbound failed, so outbound is moot)");
        }
    }

    // ── Cleanup ─────────────────────────────────────────────────────────

    println!("\nCleaning up...");
    for slot in &mut nodes {
        if let Some(mut n) = slot.take() {
            let _ = n.child.kill();
            let _ = n.child.wait();
        }
    }
    println!("Done.");
}
