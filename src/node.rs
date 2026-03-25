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
    let socket = Socket::bind(&context, bind).await.unwrap();

    let mut options = Options::default();
    options.set_fallen_behind_kick_s(10);
    options.set_heartbeat_us(50_000);
    options.set_base_min_event_interval_us(10_000);

    let engine = Rc::new(Engine::start(&context, socket, options, &key, vertex_peers).unwrap());
    println!("Engine started");

    let mut sync_seen = false;
    loop {
        let msg = tokio::select! {
            msg = engine.recv_message() => msg,
            _ = tokio::signal::ctrl_c() => {
                println!("Received signal, shutting down");
                return;
            }
        };

        match msg {
            Ok(Some(Message::SyncPoint(_))) => {
                if !sync_seen {
                    sync_seen = true;
                    println!("First SyncPoint — sending hello");
                    let data = format!("hello from {label}");
                    let mut tx = Transaction::allocate(data.len());
                    tx.copy_from_slice(data.as_bytes());
                    engine.send_transaction(tx).unwrap();
                }
            }
            Ok(Some(Message::Event(event))) => {
                if event.transaction_count() > 0 {
                    println!(
                        "Event: txns={} consensus_at={}",
                        event.transaction_count(),
                        event.consensus_at()
                    );
                }
            }
            Ok(None) => {
                println!("Engine closed");
                return;
            }
            Err(e) => {
                println!("Error: {e}");
                return;
            }
        }
    }
}
