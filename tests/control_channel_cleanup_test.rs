use anyhow::{anyhow, Context, Result};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use sha2::{Digest as _, Sha256};
use std::path::PathBuf;
use std::time::{Duration, Instant};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::broadcast,
    time,
};
use tracing_subscriber::EnvFilter;

const CONFIG_PATH: &str = "tests/for_control_channel_cleanup/tcp_transport.toml";
const SERVER_ADDR: &str = "127.0.0.1:3395";
const SERVICE_ADDR: &str = "127.0.0.1:3396";
const SERVICE_NAME: &str = "cleanup";
const SHUTDOWN_CONFIG_PATH: &str = "tests/for_server_shutdown_cleanup/tcp_transport.toml";
const SHUTDOWN_SERVER_ADDR: &str = "127.0.0.1:3401";
const SHUTDOWN_SERVICE_ADDR: &str = "127.0.0.1:3402";
const SHUTDOWN_SERVICE_NAME: &str = "shutdown_cleanup";
const TOKEN: &str = "default_token_if_not_specify";
const CURRENT_PROTO_VERSION: u8 = 1;

type Digest = [u8; 32];

#[derive(Debug, Deserialize, Serialize)]
enum Hello {
    ControlChannelHello(u8, Digest),
    DataChannelHello(u8, Digest),
}

#[derive(Debug, Deserialize, Serialize)]
struct Auth(Digest);

#[derive(Debug, Deserialize, Serialize)]
enum Ack {
    Ok,
    ServiceNotExist,
    AuthFailed,
}

#[derive(Debug, Deserialize, PartialEq, Serialize)]
enum ControlChannelCmd {
    CreateDataChannel,
    HeartBeat,
}

fn init() {
    let level = "info";
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::from(level)),
        )
        .try_init();
}

#[tokio::test]
async fn server_drops_service_state_after_control_channel_heartbeat_failure() -> Result<()> {
    if cfg!(not(feature = "server")) {
        return Ok(());
    }

    init();

    let (server_shutdown_tx, server_shutdown_rx) = broadcast::channel(1);
    let server = tokio::spawn(async move {
        run_rathole_server(CONFIG_PATH, server_shutdown_rx)
            .await
            .unwrap();
    });

    let result = async {
        let mut control = connect_control_channel(SERVER_ADDR, SERVICE_NAME).await?;

        let precreated = read_until_control_cmd(
            &mut control,
            ControlChannelCmd::HeartBeat,
            Duration::from_secs(4),
        )
        .await?;
        if precreated == 0 {
            return Err(anyhow!("server did not pre-create any TCP data channels"));
        }

        let visitor = wait_for_connect(SERVICE_ADDR, Duration::from_secs(3)).await?;
        drop(visitor);

        read_until_control_cmd(
            &mut control,
            ControlChannelCmd::CreateDataChannel,
            Duration::from_secs(3),
        )
        .await?;

        drop(control);

        wait_until_bindable(SERVICE_ADDR, Duration::from_secs(7))
            .await
            .with_context(|| "service listener remained bound after the control channel died")
    }
    .await;

    let _ = server_shutdown_tx.send(true);
    let _ = time::timeout(Duration::from_secs(3), server).await;

    result
}

#[tokio::test]
async fn server_shutdown_releases_control_handles_with_live_control_socket() -> Result<()> {
    if cfg!(not(feature = "server")) {
        return Ok(());
    }

    init();

    let (server_shutdown_tx, server_shutdown_rx) = broadcast::channel(1);
    let server = tokio::spawn(async move {
        run_rathole_server(SHUTDOWN_CONFIG_PATH, server_shutdown_rx)
            .await
            .unwrap();
    });

    let result = async {
        let mut control =
            connect_control_channel(SHUTDOWN_SERVER_ADDR, SHUTDOWN_SERVICE_NAME).await?;

        let precreated = read_until_control_cmd(
            &mut control,
            ControlChannelCmd::CreateDataChannel,
            Duration::from_secs(3),
        )
        .await?;
        if precreated != 0 {
            return Err(anyhow!("unexpected command before TCP pool warmup"));
        }

        wait_for_connect(SHUTDOWN_SERVICE_ADDR, Duration::from_secs(3)).await?;

        server_shutdown_tx.send(true)?;
        time::timeout(Duration::from_secs(3), server).await??;

        wait_until_bindable(SHUTDOWN_SERVICE_ADDR, Duration::from_secs(3))
            .await
            .with_context(|| "server shutdown kept the service listener alive")?;

        drop(control);
        Ok(())
    }
    .await;

    result
}

async fn connect_control_channel(server_addr: &str, service_name: &str) -> Result<TcpStream> {
    let mut conn = wait_for_connect(server_addr, Duration::from_secs(3)).await?;
    let service_digest = digest(service_name.as_bytes());

    write_msg(
        &mut conn,
        &Hello::ControlChannelHello(CURRENT_PROTO_VERSION, service_digest),
    )
    .await?;

    let hello = read_msg::<Hello>(
        &mut conn,
        &Hello::ControlChannelHello(CURRENT_PROTO_VERSION, [0; 32]),
    )
    .await?;
    let nonce = match hello {
        Hello::ControlChannelHello(CURRENT_PROTO_VERSION, nonce) => nonce,
        other => return Err(anyhow!("unexpected server hello: {:?}", other)),
    };

    let mut auth_payload = Vec::from(TOKEN.as_bytes());
    auth_payload.extend_from_slice(&nonce);
    write_msg(&mut conn, &Auth(digest(&auth_payload))).await?;

    let ack = read_msg::<Ack>(&mut conn, &Ack::Ok).await?;
    match ack {
        Ack::Ok => Ok(conn),
        other => Err(anyhow!("unexpected ack: {:?}", other)),
    }
}

async fn run_rathole_server(
    config_path: &str,
    shutdown_rx: broadcast::Receiver<bool>,
) -> Result<()> {
    let cli = rathole::Cli {
        config_path: Some(PathBuf::from(config_path)),
        server: true,
        client: false,
        ..Default::default()
    };
    rathole::run(cli, shutdown_rx).await
}

async fn read_until_control_cmd(
    conn: &mut TcpStream,
    expected: ControlChannelCmd,
    timeout: Duration,
) -> Result<usize> {
    let deadline = Instant::now() + timeout;
    let mut create_count = 0;

    loop {
        let remaining = deadline
            .checked_duration_since(Instant::now())
            .ok_or_else(|| anyhow!("timed out waiting for {:?}", expected))?;
        let cmd = time::timeout(
            remaining,
            read_msg::<ControlChannelCmd>(conn, &ControlChannelCmd::HeartBeat),
        )
        .await??;

        if cmd == expected {
            return Ok(create_count);
        }
        if cmd == ControlChannelCmd::CreateDataChannel {
            create_count += 1;
        }
    }
}

async fn read_msg<T>(conn: &mut TcpStream, sample: &T) -> Result<T>
where
    T: DeserializeOwned + Serialize,
{
    let mut buf = vec![0; bincode::serialized_size(sample)? as usize];
    conn.read_exact(&mut buf).await?;
    Ok(bincode::deserialize(&buf)?)
}

async fn write_msg<T>(conn: &mut TcpStream, msg: &T) -> Result<()>
where
    T: Serialize,
{
    conn.write_all(&bincode::serialize(msg)?).await?;
    conn.flush().await?;
    Ok(())
}

async fn wait_for_connect(addr: &str, timeout: Duration) -> Result<TcpStream> {
    let deadline = Instant::now() + timeout;
    let mut last_err = None;

    while Instant::now() < deadline {
        match TcpStream::connect(addr).await {
            Ok(conn) => return Ok(conn),
            Err(e) => {
                last_err = Some(e);
                time::sleep(Duration::from_millis(50)).await;
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

async fn wait_until_bindable(addr: &str, timeout: Duration) -> Result<()> {
    let deadline = Instant::now() + timeout;
    let mut last_err = None;

    while Instant::now() < deadline {
        match TcpListener::bind(addr).await {
            Ok(listener) => {
                drop(listener);
                return Ok(());
            }
            Err(e) => {
                last_err = Some(e);
                time::sleep(Duration::from_millis(100)).await;
            }
        }
    }

    Err(anyhow!(
        "failed to bind {addr}: {}",
        last_err
            .map(|e| e.to_string())
            .unwrap_or_else(|| "no bind attempt was made".to_owned())
    ))
}

fn digest(data: &[u8]) -> Digest {
    Sha256::new().chain_update(data).finalize().into()
}
