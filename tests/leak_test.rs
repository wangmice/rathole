//! Regression tests for the two-phase forwarder.
//!
//! Two scenarios under one rathole instance:
//!   1. Half-close-then-respond (correctness): visitor writes, half-closes,
//!      upstream sees EOF, upstream writes a response, visitor must read it.
//!      Verifies we did NOT regress what `copy_bidirectional` did right.
//!   2. Stuck-peer cleanup (leak): visitor half-closes, upstream never
//!      writes nor closes; the post-half-close idle timeout must reap the
//!      connection so the visitor's read returns within a bounded time.
//!
//! Both scenarios run across every enabled transport.

use anyhow::Result;
use std::time::Duration;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::broadcast,
    time::{self, timeout},
};
use tracing::info;
use tracing_subscriber::EnvFilter;

use crate::common::{run_rathole_client, run_rathole_server};

mod common;

// === TCP transports ===
const DELAYED_RESPONSE_LOCAL: &str = "127.0.0.1:9080";
const SINKHOLE_LOCAL: &str = "127.0.0.1:9081";

fn init() {
    let level = "info";
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::from(level)),
        )
        .try_init();
}

/// Local target that reads the request, observes EOF, writes a response, then
/// closes. This is the half-close-then-respond pattern that
/// `copy_bidirectional` handles correctly and the broken `select!+copy`
/// approach destroys.
async fn delayed_response_server(addr: &'static str) -> Result<()> {
    let l = TcpListener::bind(addr).await?;
    loop {
        let (mut conn, _) = l.accept().await?;
        tokio::spawn(async move {
            let mut buf = [0u8; 4096];
            // Drain request until EOF.
            loop {
                match conn.read(&mut buf).await {
                    Ok(0) => break,
                    Ok(_) => continue,
                    Err(_) => return,
                }
            }
            // Small delay to simulate "compute after EOF" — must not race the
            // forwarder's idle timeout (which is 1s in the toml).
            time::sleep(Duration::from_millis(150)).await;
            let _ = conn.write_all(b"RESP").await;
            let _ = conn.shutdown().await;
        });
    }
}

/// Local target that drains the request and never closes its own write side.
/// Triggers the post-half-close idle path.
async fn sinkhole_server(addr: &'static str) -> Result<()> {
    let l = TcpListener::bind(addr).await?;
    loop {
        let (mut conn, _) = l.accept().await?;
        tokio::spawn(async move {
            let mut buf = [0u8; 4096];
            loop {
                match conn.read(&mut buf).await {
                    Ok(0) | Err(_) => break,
                    Ok(_) => continue,
                }
            }
            time::sleep(Duration::from_secs(120)).await;
            drop(conn);
        });
    }
}

#[tokio::test]
async fn forwarder_regression_across_transports() -> Result<()> {
    if cfg!(not(all(feature = "client", feature = "server"))) {
        return Ok(());
    }

    init();

    tokio::spawn(async move {
        if let Err(e) = delayed_response_server(DELAYED_RESPONSE_LOCAL).await {
            panic!("delayed-response server failed: {:?}", e);
        }
    });
    tokio::spawn(async move {
        if let Err(e) = sinkhole_server(SINKHOLE_LOCAL).await {
            panic!("sinkhole server failed: {:?}", e);
        }
    });
    time::sleep(Duration::from_millis(200)).await;

    run_for_transport(
        "tests/for_leak/tcp_transport.toml",
        true,
        "127.0.0.1:3334",
        "127.0.0.1:3335",
    )
    .await?;

    #[cfg(any(
        all(target_os = "macos", feature = "rustls"),
        all(
            not(target_os = "macos"),
            any(feature = "native-tls", feature = "rustls")
        ),
    ))]
    {
        // Schannel-backed native-tls on Windows does not preserve TCP
        // half-close-then-respond semantics. Still verify the leak cleanup
        // path there; rustls and non-Windows native-tls keep the full check.
        let check_tls_half_close_response =
            cfg!(feature = "rustls") || cfg!(not(target_os = "windows"));
        run_for_transport(
            "tests/for_leak/tls_transport.toml",
            check_tls_half_close_response,
            "127.0.0.1:3344",
            "127.0.0.1:3345",
        )
        .await?;
    }

    #[cfg(feature = "noise")]
    run_for_transport(
        "tests/for_leak/noise_transport.toml",
        true,
        "127.0.0.1:3354",
        "127.0.0.1:3355",
    )
    .await?;

    // WebSocket cannot carry a half-close-then-respond pattern because per
    // RFC 6455 the peer that receives a Close frame must respond with its
    // own Close, which closes its sending side too — we can still verify
    // the stuck-peer cleanup path though.
    #[cfg(any(feature = "websocket-native-tls", feature = "websocket-rustls"))]
    run_for_transport(
        "tests/for_leak/websocket_transport.toml",
        false,
        "127.0.0.1:3364",
        "127.0.0.1:3365",
    )
    .await?;

    #[cfg(not(target_os = "macos"))]
    #[cfg(any(feature = "websocket-native-tls", feature = "websocket-rustls"))]
    run_for_transport(
        "tests/for_leak/websocket_tls_transport.toml",
        false,
        "127.0.0.1:3374",
        "127.0.0.1:3375",
    )
    .await?;

    Ok(())
}

async fn run_for_transport(
    config_path: &'static str,
    check_half_close_response: bool,
    delayed_response_exposed: &'static str,
    sinkhole_exposed: &'static str,
) -> Result<()> {
    info!("forwarder regression for {}", config_path);

    let (client_shutdown_tx, client_shutdown_rx) = broadcast::channel(1);
    let (server_shutdown_tx, server_shutdown_rx) = broadcast::channel(1);

    let server = tokio::spawn(async move {
        run_rathole_server(config_path, server_shutdown_rx)
            .await
            .unwrap();
    });
    time::sleep(Duration::from_millis(500)).await;
    let client = tokio::spawn(async move {
        run_rathole_client(config_path, client_shutdown_rx)
            .await
            .unwrap();
    });
    time::sleep(Duration::from_millis(2500)).await;

    if check_half_close_response {
        check_half_close_with_response(delayed_response_exposed).await?;
    }
    check_stuck_peer_cleanup_tcp(sinkhole_exposed).await?;

    client_shutdown_tx.send(true)?;
    server_shutdown_tx.send(true)?;
    let _ = tokio::join!(client, server);
    time::sleep(Duration::from_millis(500)).await;
    Ok(())
}

/// Visitor sends a request, half-closes, then expects to read a response.
/// Under the broken `select!+copy` PR this would lose the response. Under the
/// two-phase forwarder this must succeed.
async fn check_half_close_with_response(addr: &str) -> Result<()> {
    info!("check half-close with response (TCP)");
    let mut conn = connect_with_retry_tcp(addr, Duration::from_secs(15)).await?;
    conn.write_all(b"REQ").await?;
    conn.shutdown().await?;

    let mut resp = [0u8; 4];
    let read = timeout(Duration::from_secs(5), conn.read_exact(&mut resp)).await;
    let r = read.map_err(|_| {
        anyhow::anyhow!(
            "visitor read hung waiting for delayed response \u{2014} forwarder regressed half-close semantics"
        )
    })?;
    r.map_err(|e| anyhow::anyhow!("visitor read errored: {e}"))?;
    assert_eq!(
        &resp, b"RESP",
        "expected RESP after half-close, got {:?}",
        resp
    );
    Ok(())
}

/// Visitor half-closes; sinkhole upstream never closes its write side. The
/// post-half-close idle timeout (1s in the toml) must reap the connection so
/// the visitor sees EOF promptly.
async fn check_stuck_peer_cleanup_tcp(addr: &str) -> Result<()> {
    info!("check stuck-peer cleanup (TCP)");
    let mut conn = connect_with_retry_tcp(addr, Duration::from_secs(15)).await?;
    conn.write_all(b"hello").await?;
    conn.shutdown().await?;

    let mut buf = [0u8; 16];
    let read = timeout(Duration::from_secs(5), conn.read(&mut buf)).await;
    let n = read
        .map_err(|_| {
            anyhow::anyhow!(
                "visitor read hung after half-close \u{2014} stuck-peer cleanup regressed"
            )
        })?
        .map_err(|e| anyhow::anyhow!("visitor read errored: {e}"))?;
    assert_eq!(n, 0, "expected EOF after stuck-peer cleanup, got {n} bytes");
    Ok(())
}

/// Connect with retry — the service listener on the rathole server side is
/// only bound after the client's control-channel handshake completes. On slow
/// runners a single connect attempt races the handshake.
async fn connect_with_retry_tcp(addr: &str, total: Duration) -> Result<TcpStream> {
    let deadline = tokio::time::Instant::now() + total;
    let mut last_err = None;
    while tokio::time::Instant::now() < deadline {
        match TcpStream::connect(addr).await {
            Ok(c) => return Ok(c),
            Err(e) => {
                last_err = Some(e);
                time::sleep(Duration::from_millis(100)).await;
            }
        }
    }
    Err(anyhow::anyhow!(
        "failed to connect to {addr} within {:?}: {}",
        total,
        last_err
            .map(|e| e.to_string())
            .unwrap_or_else(|| "no error".into())
    ))
}
