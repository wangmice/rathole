use anyhow::{anyhow, Result};
use std::time::Duration;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
    sync::broadcast,
    time,
};
use tracing_subscriber::EnvFilter;

use crate::common::{run_rathole_client, run_rathole_server, PING, PONG};

mod common;

const CONFIG_PATH: &str = "tests/for_handshake_dos/noise_transport.toml";
const SERVER_ADDR: &str = "127.0.0.1:3383";
const LOCAL_ADDR: &str = "127.0.0.1:9181";
const EXPOSED_ADDR: &str = "127.0.0.1:3384";

fn init() {
    let level = "info";
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::from(level)),
        )
        .try_init();
}

#[tokio::test]
async fn slow_transport_handshake_does_not_block_accept_loop() -> Result<()> {
    if cfg!(not(all(
        feature = "client",
        feature = "server",
        feature = "noise"
    ))) {
        return Ok(());
    }

    init();

    tokio::spawn(async move {
        if let Err(e) = common::tcp::pingpong_server(LOCAL_ADDR).await {
            panic!("Failed to run the pingpong server for testing: {:?}", e);
        }
    });

    let (client_shutdown_tx, client_shutdown_rx) = broadcast::channel(1);
    let (server_shutdown_tx, server_shutdown_rx) = broadcast::channel(1);

    let server = tokio::spawn(async move {
        run_rathole_server(CONFIG_PATH, server_shutdown_rx)
            .await
            .unwrap();
    });
    time::sleep(Duration::from_millis(500)).await;

    // This raw TCP connection never sends a Noise handshake. Before the fix,
    // the server waited for its transport handshake inside the accept loop,
    // blocking legitimate clients until HANDSHAKE_TIMEOUT elapsed.
    let poison = TcpStream::connect(SERVER_ADDR).await?;
    time::sleep(Duration::from_millis(200)).await;

    let client = tokio::spawn(async move {
        run_rathole_client(CONFIG_PATH, client_shutdown_rx)
            .await
            .unwrap();
    });

    let result = time::timeout(Duration::from_secs(3), pingpong_with_retry(EXPOSED_ADDR)).await;

    drop(poison);
    let _ = client_shutdown_tx.send(true);
    let _ = server_shutdown_tx.send(true);
    let _ = tokio::join!(client, server);

    result
        .map_err(|_| anyhow!("legitimate client was blocked behind a slow transport handshake"))?
}

async fn pingpong_with_retry(addr: &str) -> Result<()> {
    let deadline = time::Instant::now() + Duration::from_secs(3);
    let mut last_err = None;

    while time::Instant::now() < deadline {
        match TcpStream::connect(addr).await {
            Ok(mut conn) => {
                conn.write_all(PING.as_bytes()).await?;

                let mut rd = [0u8; PONG.len()];
                conn.read_exact(&mut rd).await?;
                if rd == PONG.as_bytes() {
                    return Ok(());
                }
                return Err(anyhow!("unexpected pingpong response: {:?}", rd));
            }
            Err(e) => {
                last_err = Some(e);
                time::sleep(Duration::from_millis(100)).await;
            }
        }
    }

    Err(anyhow!(
        "failed to connect to {addr}: {}",
        last_err
            .map(|e| e.to_string())
            .unwrap_or_else(|| "no connection attempt was made".to_owned())
    ))
}
