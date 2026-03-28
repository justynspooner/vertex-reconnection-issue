mod node;

use std::io::{BufRead, BufReader};
use std::process::{Command, Stdio};
use std::thread;
use std::time::Duration;

use tashi_vertex::KeySecret;

const ADDRS: [&str; 4] = [
    "127.0.0.1:9000",
    "127.0.0.1:9001",
    "127.0.0.1:9002",
    "127.0.0.1:9003",
];

fn main() {
    let args: Vec<String> = std::env::args().collect();

    // Internal: run as a child node process
    if args.len() > 1 && args[1] == "node" {
        return run_node(&args[2..]);
    }

    run_test();
}

fn run_node(args: &[String]) {
    let mut bind = String::new();
    let mut secret = String::new();
    let mut label = String::new();
    let mut reconnect_timeout_s: Option<u64> = None;
    let mut peer_addrs = Vec::new();
    let mut peer_pubkeys = Vec::new();

    let mut i = 0;
    while i < args.len() {
        match args[i].as_str() {
            "--bind" => { bind = args[i + 1].clone(); i += 2; }
            "--secret" => { secret = args[i + 1].clone(); i += 2; }
            "--label" => { label = args[i + 1].clone(); i += 2; }
            "--reconnect-timeout" => { reconnect_timeout_s = Some(args[i + 1].parse().unwrap()); i += 2; }
            "--peer-addr" => { peer_addrs.push(args[i + 1].clone()); i += 2; }
            "--peer-pubkey" => { peer_pubkeys.push(args[i + 1].clone()); i += 2; }
            _ => { i += 1; }
        }
    }

    let peers: Vec<(String, String)> = peer_addrs.into_iter().zip(peer_pubkeys).collect();
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    rt.block_on(node::run(&bind, &secret, &label, reconnect_timeout_s, peers));
}

fn spawn_node(
    exe: &std::path::Path,
    i: usize,
    secrets: &[String],
    pubkeys: &[String],
    reconnect_timeout: Option<u64>,
) -> std::process::Child {
    let mut cmd = Command::new(exe);
    cmd.arg("node")
        .arg("--bind").arg(ADDRS[i])
        .arg("--secret").arg(&secrets[i])
        .arg("--label").arg(format!("node-{i}"));

    if let Some(t) = reconnect_timeout {
        cmd.arg("--reconnect-timeout").arg(t.to_string());
    }

    for j in 0..ADDRS.len() {
        if j == i { continue; }
        cmd.arg("--peer-addr").arg(ADDRS[j])
            .arg("--peer-pubkey").arg(&pubkeys[j]);
    }

    cmd.stdout(Stdio::piped()).stderr(Stdio::inherit());
    let mut child = cmd.spawn().expect("failed to spawn node");

    let stdout = child.stdout.take().unwrap();
    let idx = i;
    thread::spawn(move || {
        let reader = BufReader::new(stdout);
        for line in reader.lines() {
            if let Ok(line) = line {
                println!("[node-{idx}] {line}");
            }
        }
    });

    child
}

fn run_test() {
    let n = ADDRS.len();
    let mut secrets = Vec::new();
    let mut pubkeys = Vec::new();
    for _ in 0..n {
        let key = KeySecret::generate();
        pubkeys.push(key.public().to_string());
        secrets.push(key.to_string());
    }

    let exe = std::env::current_exe().unwrap();

    // Phase 1: Start all 4 nodes
    println!("=== Phase 1: Start all {n} nodes ===");
    let mut children: Vec<std::process::Child> = Vec::new();
    for i in 0..n {
        children.push(spawn_node(&exe, i, &secrets, &pubkeys, None));
        thread::sleep(Duration::from_millis(500));
    }

    println!("\nWaiting 8s for consensus...");
    thread::sleep(Duration::from_secs(8));

    // Phase 2: Kill node-3
    println!("\n=== Phase 2: Kill node-3 ===");
    children[3].kill().expect("failed to kill");
    children[3].wait().ok();
    println!("node-3 killed.");

    println!("\nWaiting 15s for node-3 to be kicked (fallen_behind_kick_s=10)...");
    thread::sleep(Duration::from_secs(15));

    // Phase 3: Restart node-3, retry up to 5 times
    println!("\n=== Phase 3: Restart node-3 ===");
    let max_attempts = 5;
    for attempt in 1..=max_attempts {
        println!("\n--- Attempt {attempt}/{max_attempts} ---");
        children[3] = spawn_node(&exe, 3, &secrets, &pubkeys, Some(5));

        // 5s reconnect timeout + 3s buffer
        thread::sleep(Duration::from_secs(8));

        match children[3].try_wait() {
            Ok(Some(status)) if status.code() == Some(42) => {
                println!("node-3 timed out (exit 42), retrying...");
                continue;
            }
            Ok(Some(status)) => {
                println!("node-3 exited unexpectedly: {status}");
                break;
            }
            Ok(None) => {
                println!("node-3 reconnected successfully!");
                thread::sleep(Duration::from_secs(5));
                break;
            }
            Err(e) => {
                println!("Error: {e}");
                break;
            }
        }
    }

    println!("\nCleaning up...");
    for mut child in children {
        let _ = child.kill();
        let _ = child.wait();
    }
    println!("Done.");
}
