use std::rc::Rc;

use tashi_vertex::{
    Context, Engine, KeyPublic, KeySecret, Message, Options, Peers, Socket, Transaction,
};

/// Build Options based on the named configuration.
fn build_options(config: &str) -> Options {
    let mut opts = Options::default();
    opts.set_enable_state_sharing(true);

    match config {
        "simple" => {
            opts.set_fallen_behind_kick_s(10);
            opts.set_epoch_states_to_cache(10);
        }
        "default" => {
            opts.set_fallen_behind_kick_s(10);
        }
        "no_kick_fast_heartbeat" => {
            opts.set_fallen_behind_kick_s(0);
            opts.set_heartbeat_us(100_000);
        }
        "retry" => {
            opts.set_fallen_behind_kick_s(10);
            opts.set_heartbeat_us(100_000);
        }
        "kick_retry" => {
            opts.set_fallen_behind_kick_s(10);
            opts.set_heartbeat_us(100_000);
        }
        "kr_hole_punch" => {
            opts.set_fallen_behind_kick_s(10);
            opts.set_heartbeat_us(100_000);
            opts.set_enable_hole_punching(true);
        }
        "kr_state_share" => {
            opts.set_fallen_behind_kick_s(10);
            opts.set_heartbeat_us(100_000);
            opts.set_enable_state_sharing(true);
        }
        "kr_gossip" => {
            opts.set_fallen_behind_kick_s(10);
            opts.set_heartbeat_us(100_000);
            opts.set_report_gossip_events(true);
        }
        "kr_fast_events" => {
            opts.set_fallen_behind_kick_s(10);
            opts.set_heartbeat_us(100_000);
            opts.set_base_min_event_interval_us(10_000); // 10ms
        }
        "kr_dynamic_epoch" => {
            opts.set_fallen_behind_kick_s(10);
            opts.set_heartbeat_us(100_000);
            opts.set_enable_dynamic_epoch_size(true);
        }
        _ => {
            eprintln!("Unknown config '{config}', using default");
            opts.set_fallen_behind_kick_s(10);
        }
    }

    opts
}

pub async fn run(
    bind: &str,
    secret: &str,
    label: &str,
    config: &str,
    reconnect_timeout_s: Option<u64>,
    peers_info: Vec<(String, String)>,
) {
    let key: KeySecret = secret.parse().unwrap();

    let mut vertex_peers = Peers::new().unwrap();
    for (addr, pubkey) in &peers_info {
        let pub_key: KeyPublic = pubkey.parse().unwrap();
        vertex_peers.insert(addr, &pub_key, Default::default()).unwrap();
    }
    vertex_peers.insert(bind, &key.public(), Default::default()).unwrap();

    let context = Context::new().unwrap();
    let socket = {
        let mut attempts = 0;
        loop {
            match Socket::bind(&context, bind).await {
                Ok(s) => break s,
                Err(_) if attempts < 40 => {
                    attempts += 1;
                    println!("Port busy, retrying ({attempts}/40)...");
                    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
                }
                Err(e) => panic!("Failed to bind after retries: {e:?}"),
            }
        }
    };

    let options = build_options(config);
    println!("Engine starting with config={config}");

    let engine = Rc::new(Engine::start(&context, socket, options, &key, vertex_peers).unwrap());
    println!("Engine started");

    let mut sync_count = 0u64;
    let mut sent_hello = false;

    loop {
        // After SyncPoint #1, apply reconnect timeout if configured
        let msg = if sync_count == 1 && reconnect_timeout_s.is_some() {
            let timeout = std::time::Duration::from_secs(reconnect_timeout_s.unwrap());
            match tokio::time::timeout(timeout, engine.recv_message()).await {
                Ok(result) => result.unwrap(),
                Err(_) => {
                    println!("TIMEOUT: no message after SyncPoint #1 within {}s, exiting with code 42", timeout.as_secs());
                    std::process::exit(42);
                }
            }
        } else {
            engine.recv_message().await.unwrap()
        };

        let Some(msg) = msg else {
            println!("Engine closed");
            return;
        };

        match msg {
            Message::SyncPoint(_) => {
                sync_count += 1;
                println!("SyncPoint #{sync_count}");

                if !sent_hello {
                    sent_hello = true;
                    let data = format!("hello from {label}");
                    let mut tx = Transaction::allocate(data.len());
                    tx.copy_from_slice(data.as_bytes());
                    engine.send_transaction(tx).unwrap();
                    println!("Sent hello transaction");
                }

                if sync_count > 1 {
                    let data = format!("ping from {label} at sync {sync_count}");
                    let mut tx = Transaction::allocate(data.len());
                    tx.copy_from_slice(data.as_bytes());
                    engine.send_transaction(tx).unwrap();
                    println!("Sent ping at sync {sync_count}");
                }
            }
            Message::Event(event) => {
                if event.transaction_count() > 0 {
                    let creator = event.creator().to_string();
                    let short = &creator[creator.len().saturating_sub(8)..];
                    for tx_data in event.transactions() {
                        let payload = String::from_utf8_lossy(tx_data);
                        println!(
                            "EVENT from ...{short}: \"{}\" (consensus_at={})",
                            payload,
                            event.consensus_at()
                        );
                    }
                }
            }
        }
    }
}
