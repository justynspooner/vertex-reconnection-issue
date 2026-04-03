use tashi_vertex::{
    Context, Engine, KeyPublic, KeySecret, Message, Options, Peers, Socket, Transaction,
};

pub async fn run(
    bind: &str,
    secret: &str,
    label: &str,
    reconnect_timeout_s: Option<u64>,
    peers_info: Vec<(String, String)>,
) {
    let key: KeySecret = secret.parse().unwrap();

    let mut peers = Peers::new().unwrap();
    for (addr, pubkey) in &peers_info {
        let pub_key: KeyPublic = pubkey.parse().unwrap();
        peers.insert(addr, &pub_key, Default::default()).unwrap();
    }
    peers.insert(bind, &key.public(), Default::default()).unwrap();

    let context = Context::new().unwrap();
    let socket = {
        let mut attempts = 0;
        loop {
            match Socket::bind(&context, bind).await {
                Ok(s) => break s,
                Err(_) if attempts < 40 => {
                    attempts += 1;
                    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
                }
                Err(e) => panic!("Failed to bind after retries: {e:?}"),
            }
        }
    };

    let mut options = Options::default();
    options.set_fallen_behind_kick_s(10);
    options.set_enable_state_sharing(true);
    options.set_epoch_states_to_cache(10);

    // If reconnect_timeout_s is set, this is a restarted node that needs
    // to join an existing session rather than creating a new one.
    let joining = reconnect_timeout_s.is_some();
    let engine = Engine::start(&context, socket, options, &key, peers, joining).unwrap();
    println!("Engine started");

    let mut sync_count = 0u64;
    let mut sent_hello = false;

    loop {
        let msg = if sync_count == 1 && reconnect_timeout_s.is_some() {
            let timeout = std::time::Duration::from_secs(reconnect_timeout_s.unwrap());
            match tokio::time::timeout(timeout, engine.recv_message()).await {
                Ok(result) => result.unwrap(),
                Err(_) => {
                    println!("TIMEOUT: no message within {}s after SyncPoint #1", timeout.as_secs());
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
                    println!("Sent hello");
                }
            }
            Message::Event(event) => {
                for tx_data in event.transactions() {
                    let payload = String::from_utf8_lossy(tx_data);
                    println!("EVENT: \"{payload}\"");
                }
            }
        }
    }
}
