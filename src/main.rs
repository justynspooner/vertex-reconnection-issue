///! Tests Vertex node reconnection with 4 nodes (f=1).
///! With f=1, 3 of 4 nodes can continue consensus while 1 is offline.

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
    if args.len() > 1 && args[1] == "node" {
        return run_node(&args[2..]);
    }
    run_test();
}

fn run_node(args: &[String]) {
    let mut bind = String::new();
    let mut secret = String::new();
    let mut label = String::new();
    let mut peer_addrs = Vec::new();
    let mut peer_pubkeys = Vec::new();

    let mut i = 0;
    while i < args.len() {
        match args[i].as_str() {
            "--bind" => { bind = args[i + 1].clone(); i += 2; }
            "--secret" => { secret = args[i + 1].clone(); i += 2; }
            "--label" => { label = args[i + 1].clone(); i += 2; }
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
    rt.block_on(node::run(&bind, &secret, &label, peers));
}

fn spawn_node(exe: &std::path::Path, i: usize, secrets: &[String], pubkeys: &[String]) -> std::process::Child {
    let mut cmd = Command::new(exe);
    cmd.arg("node")
        .arg("--bind").arg(ADDRS[i])
        .arg("--secret").arg(&secrets[i])
        .arg("--label").arg(format!("node-{i}"));

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

    println!("=== Phase 1: Start all {n} nodes ===");
    let mut children: Vec<std::process::Child> = Vec::new();
    for i in 0..n {
        children.push(spawn_node(&exe, i, &secrets, &pubkeys));
        thread::sleep(Duration::from_millis(500));
    }

    println!("\nWaiting 8s for consensus...");
    thread::sleep(Duration::from_secs(8));

    println!("\n=== Phase 2: Kill node-3 ===");
    children[3].kill().expect("failed to kill");
    children[3].wait().ok();
    println!("node-3 killed. Remaining 3 nodes have f=1 tolerance.");

    println!("\nWaiting 15s (remaining nodes keep consensus, node-3 gets kicked after 10s)...");
    thread::sleep(Duration::from_secs(15));

    println!("\n=== Phase 3: Restart node-3 ===");
    children[3] = spawn_node(&exe, 3, &secrets, &pubkeys);

    println!("\nWaiting 10s for node-3 to rejoin...");
    thread::sleep(Duration::from_secs(10));

    // Phase 4: Send a transaction from node-0 and check if restarted node-3 receives it
    println!("\n=== Phase 4: Testing if restarted node-3 receives events ===");
    // Write a command file to trigger node-0 to send a transaction
    let cmd_path = std::env::current_dir().unwrap().join("node-0-send.flag");
    std::fs::write(&cmd_path, "send").unwrap();

    println!("Waiting 60s to see if node-3 eventually receives events...");
    thread::sleep(Duration::from_secs(60));

    let _ = std::fs::remove_file(&cmd_path);

    for i in 0..n {
        match children[i].try_wait() {
            Ok(None) => println!("[node-{i}] still running"),
            Ok(Some(status)) => println!("[node-{i}] exited: {status}"),
            Err(e) => println!("[node-{i}] error: {e}"),
        }
    }

    println!("\nCleaning up...");
    for mut child in children {
        let _ = child.kill();
        let _ = child.wait();
    }
    println!("Done.");
}
