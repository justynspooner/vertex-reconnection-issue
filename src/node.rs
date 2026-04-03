use std::collections::VecDeque;
use std::time::Duration;

use tashi_vertex::{
    Context, Engine, KeyPublic, KeySecret, Message, Options, Peers, Socket, Transaction,
};

struct ScheduledSend {
    after_ms: u64,
    payload: String,
}

/// Run a single Vertex consensus node.
///
/// - `scheduled_sends`: list of (delay_ms, payload) to send at fixed times after engine start.
/// - `watch_prefixes`: if non-empty, the node watches for events matching each prefix.
///   Once every prefix has been seen at least once, the process exits with code 0.
///   If `watch_timeout_s` elapses first, it exits with code 42.
pub async fn run(
    bind: &str,
    secret: &str,
    label: &str,
    joining: bool,
    scheduled_sends: Vec<(u64, String)>,
    watch_prefixes: Vec<String>,
    watch_timeout_s: u64,
    peers_info: Vec<(String, String)>,
) {
    let key: KeySecret = secret.parse().unwrap();

    // Build the peer list (all nodes including ourselves).
    let mut peers = Peers::new().unwrap();
    for (addr, pubkey) in &peers_info {
        let pub_key: KeyPublic = pubkey.parse().unwrap();
        peers.insert(addr, &pub_key, Default::default()).unwrap();
    }
    peers.insert(bind, &key.public(), Default::default()).unwrap();

    // Bind socket (retry to handle port reuse after a killed process).
    let context = Context::new().unwrap();
    let socket = {
        let mut attempts = 0;
        loop {
            match Socket::bind(&context, bind).await {
                Ok(s) => break s,
                Err(_) if attempts < 40 => {
                    attempts += 1;
                    tokio::time::sleep(Duration::from_millis(500)).await;
                }
                Err(e) => panic!("failed to bind {bind} after {attempts} retries: {e:?}"),
            }
        }
    };

    let mut options = Options::default();
    options.set_fallen_behind_kick_s(120);
    options.set_enable_state_sharing(true);
    options.set_epoch_states_to_cache(10);

    let engine = Engine::start(&context, socket, options, &key, peers, joining).unwrap();
    println!("engine started (joining={joining})");

    // All timings are relative to engine start.
    let engine_started = tokio::time::Instant::now();

    let mut sync_count = 0u64;
    let mut sent_hello = false;
    let mut sends: VecDeque<ScheduledSend> = scheduled_sends
        .into_iter()
        .map(|(after_ms, payload)| ScheduledSend { after_ms, payload })
        .collect();

    // Watch state: track which prefixes we still need to see.
    let watching = !watch_prefixes.is_empty();
    let mut unseen: Vec<String> = watch_prefixes;
    let watch_deadline = if watching && watch_timeout_s > 0 {
        Some(engine_started + Duration::from_secs(watch_timeout_s))
    } else {
        None
    };

    loop {
        let next_send = sends
            .front()
            .map(|s| engine_started + Duration::from_millis(s.after_ms));

        let far_future = tokio::time::Instant::now() + Duration::from_secs(86_400);

        tokio::select! {
            // Branch 1: fire a scheduled transaction send.
            _ = tokio::time::sleep_until(next_send.unwrap_or(far_future)),
                if next_send.is_some() =>
            {
                let entry = sends.pop_front().unwrap();
                send_payload(&engine, &entry.payload);
                println!("SENT \"{}\"", entry.payload);
            }

            // Branch 2: watch deadline expired — report what we never saw.
            _ = tokio::time::sleep_until(watch_deadline.unwrap_or(far_future)),
                if watch_deadline.is_some() =>
            {
                println!("TIMEOUT after {watch_timeout_s}s — missing prefixes: {unseen:?}");
                std::process::exit(42);
            }

            // Branch 3: receive a message from the consensus engine.
            result = engine.recv_message() => {
                let Some(msg) = result.unwrap() else {
                    println!("engine closed");
                    return;
                };

                match msg {
                    Message::SyncPoint(_) => {
                        sync_count += 1;
                        println!("SyncPoint #{sync_count}");

                        if !sent_hello {
                            sent_hello = true;
                            let hello = format!("hello:{label}");
                            send_payload(&engine, &hello);
                            println!("SENT \"{hello}\"");
                        }
                    }
                    Message::Event(event) => {
                        let creator = event.creator().to_string();
                        let short = &creator[creator.len().saturating_sub(8)..];

                        for tx_data in event.transactions() {
                            let payload = String::from_utf8_lossy(tx_data);
                            println!("EVENT from ...{short}: \"{payload}\"");

                            // Check this event against unseen watch prefixes.
                            if watching {
                                unseen.retain(|prefix| !payload.starts_with(prefix));
                                if unseen.is_empty() {
                                    println!("SUCCESS: all watched prefixes observed");
                                    std::process::exit(0);
                                }
                            }
                        }
                    }
                }
            }
        }
    }
}

fn send_payload(engine: &Engine, payload: &str) {
    let mut tx = Transaction::allocate(payload.len());
    tx.copy_from_slice(payload.as_bytes());
    engine.send_transaction(tx).unwrap();
}
