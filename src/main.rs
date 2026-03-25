///! Reproduces a segmentation fault when dropping a Vertex Engine
///! while other engines on separate threads are still running.
///!
///! Steps:
///!   1. Generate 3 keypairs
///!   2. Start 3 engines on separate threads (each with its own tokio runtime)
///!   3. Wait for consensus to establish (SyncPoints + Hello exchange)
///!   4. Drop one engine by shutting down its thread
///!   5. Observe: segmentation fault
///!
///! Expected: Engine can be cleanly dropped while other engines continue.
///! Actual: Process crashes with SIGSEGV.

use std::rc::Rc;
use std::sync::mpsc as std_mpsc;
use std::thread;
use std::time::Duration;

use tashi_vertex::{
    Context, Engine, KeyPublic, KeySecret, Message, Options, Peers, Socket, Transaction,
};

const ADDRS: [&str; 3] = ["127.0.0.1:9000", "127.0.0.1:9001", "127.0.0.1:9002"];

fn main() {
    // Generate 3 keypairs, store as strings for cross-thread transfer
    let mut secrets = Vec::new();
    let mut pubkeys = Vec::new();
    for _ in 0..3 {
        let key = KeySecret::generate();
        pubkeys.push(key.public().to_string());
        secrets.push(key.to_string());
    }

    println!("Starting 3 Vertex engines...");
    for i in 0..3 {
        let short = &pubkeys[i][pubkeys[i].len().saturating_sub(8)..];
        println!("  node-{i}: bind={} id=...{short}", ADDRS[i]);
    }

    // Channel to signal node-2 to shut down
    let (stop_tx, stop_rx) = std_mpsc::channel::<()>();

    // Spawn 3 engine threads
    let mut handles = Vec::new();
    let mut stop_rx_opt = Some(stop_rx);
    for i in 0..3 {
        let secret = secrets[i].clone();
        let all_pubkeys: Vec<String> = pubkeys.clone();
        let stop_rx = if i == 2 { stop_rx_opt.take() } else { None };

        let handle = thread::spawn(move || {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();

            rt.block_on(async {
                let key: KeySecret = secret.parse().unwrap();

                // Build peers list (all 3 nodes)
                let mut peers = Peers::new().unwrap();
                for (j, pk) in all_pubkeys.iter().enumerate() {
                    let pub_key: KeyPublic = pk.parse().unwrap();
                    peers.insert(ADDRS[j], &pub_key, Default::default()).unwrap();
                }

                let context = Context::new().unwrap();
                let socket = Socket::bind(&context, ADDRS[i]).await.unwrap();

                let mut options = Options::default();
                options.set_fallen_behind_kick_s(10);
                options.set_heartbeat_us(50_000);
                options.set_base_min_event_interval_us(10_000);

                let engine = Rc::new(
                    Engine::start(&context, socket, options, &key, peers).unwrap()
                );

                println!("[node-{i}] Engine started");

                let mut sync_seen = false;
                loop {
                    // Check if we should stop (node-2 only)
                    if let Some(ref rx) = stop_rx {
                        if rx.try_recv().is_ok() {
                            println!("[node-{i}] Received stop signal, dropping engine...");
                            return;
                        }
                    }

                    let msg = tokio::select! {
                        msg = engine.recv_message() => msg,
                        _ = tokio::time::sleep(Duration::from_millis(50)) => continue,
                    };

                    match msg {
                        Ok(Some(Message::SyncPoint(_))) => {
                            if !sync_seen {
                                sync_seen = true;
                                println!("[node-{i}] First SyncPoint — sending hello");
                                let data = format!("hello from node-{i}");
                                let mut tx = Transaction::allocate(data.len());
                                tx.copy_from_slice(data.as_bytes());
                                engine.send_transaction(tx).unwrap();
                            }
                        }
                        Ok(Some(Message::Event(event))) => {
                            if event.transaction_count() > 0 {
                                println!(
                                    "[node-{i}] Event: txns={} consensus_at={}",
                                    event.transaction_count(),
                                    event.consensus_at()
                                );
                            }
                        }
                        Ok(None) => {
                            println!("[node-{i}] Engine closed");
                            return;
                        }
                        Err(e) => {
                            println!("[node-{i}] Error: {e}");
                            return;
                        }
                    }
                }
            });

            println!("[node-{i}] Thread exiting (engine dropped)");
        });

        handles.push(handle);
        thread::sleep(Duration::from_millis(500));
    }

    println!("\nWaiting 5 seconds for consensus to establish...");
    thread::sleep(Duration::from_secs(5));

    println!("\n=== Stopping node-2 (dropping its engine) ===");
    stop_tx.send(()).unwrap();

    // Wait for the thread to exit (engine gets dropped)
    if let Some(handle) = handles.pop() {
        handle.join().unwrap();
    }

    println!("node-2 stopped. If we got here without segfault, the bug is fixed.");
    println!("Waiting 3 more seconds to verify stability...");
    thread::sleep(Duration::from_secs(3));

    println!("Done. No segfault observed.");
}
