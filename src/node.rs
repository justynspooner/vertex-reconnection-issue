use std::rc::Rc;

use tashi_vertex::{
    Context, Engine, KeyPublic, KeySecret, Message, Options, Peers, Socket, Transaction,
};

pub async fn run(
    bind: &str,
    secret: &str,
    label: &str,
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
                Err(_) if attempts < 20 => {
                    attempts += 1;
                    println!("Port busy, retrying ({attempts}/20)...");
                    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
                }
                Err(e) => panic!("Failed to bind after retries: {e:?}"),
            }
        }
    };

    let mut options = Options::default();
    options.set_fallen_behind_kick_s(10);

    let engine = Rc::new(Engine::start(&context, socket, options, &key, vertex_peers).unwrap());
    println!("Engine started");

    let mut sync_count = 0u64;
    let mut sent_hello = false;

    while let Some(msg) = engine.recv_message().await.unwrap() {
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

                // Send a ping every 3rd sync point
                if sync_count > 1 && sync_count % 3 == 0 {
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

    println!("Engine closed");
}
