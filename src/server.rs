use crate::config::{Config, ServerConfig, ServerServiceConfig, ServiceType, TransportType};
use crate::config_watcher::{ConfigChange, ServerServiceChange};
use crate::constants::{listen_backoff, UDP_BUFFER_SIZE};
use crate::helper::{retry_notify_with_deadline, write_and_flush};
use crate::multi_map::MultiMap;
use crate::protocol::Hello::{ControlChannelHello, DataChannelHello};
use crate::protocol::{
    self, read_ack, read_auth, read_hello, Ack, ControlChannelCmd, DataChannelCmd, Hello,
    UdpTraffic, HASH_WIDTH_IN_BYTES,
};
use crate::transport::{SocketOpts, TcpTransport, Transport};
use anyhow::{anyhow, bail, Context, Result};
use backoff::backoff::Backoff;
use backoff::ExponentialBackoff;

use rand::RngExt as _;
use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Weak};
use std::time::Duration;
use tokio::io::{self, AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream, UdpSocket};
use tokio::sync::{broadcast, mpsc, RwLock};
use tokio::time;
use tracing::{debug, error, info, info_span, instrument, warn, Instrument, Span};

#[cfg(feature = "noise")]
use crate::transport::NoiseTransport;
#[cfg(any(feature = "native-tls", feature = "rustls"))]
use crate::transport::TlsTransport;
#[cfg(any(feature = "websocket-native-tls", feature = "websocket-rustls"))]
use crate::transport::WebsocketTransport;

type ServiceDigest = protocol::Digest; // SHA256 of a service name
type Nonce = protocol::Digest; // Also called `session_key`

const TCP_POOL_SIZE: usize = 8; // The number of cached connections for TCP servies
const UDP_POOL_SIZE: usize = 2; // The number of cached connections for UDP services
const CHAN_SIZE: usize = 2048; // The capacity of various chans
const HANDSHAKE_TIMEOUT: u64 = 5; // Timeout for transport handshake
const TCP_POOL_HEARTBEAT_INTERVAL: u64 = 30; // Application-layer heartbeat for idle TCP data channels
const TCP_POOL_HEARTBEAT_TIMEOUT: u64 = 5;
#[cfg(not(test))]
const TCP_VISITOR_WAIT_TIMEOUT: u64 = 30; // Max time an accepted visitor may wait for a data channel
#[cfg(test)]
const TCP_VISITOR_WAIT_TIMEOUT: u64 = 1;
#[cfg(not(test))]
const TCP_DATA_CHANNEL_REQUEST_TIMEOUT: u64 = 15; // Max time a data-channel request may remain pending
#[cfg(test)]
const TCP_DATA_CHANNEL_REQUEST_TIMEOUT: u64 = 1;

// Surface the outcome of a forwarder task. Only the leak-guard reaper
// (the typed `PostHalfCloseIdleTimeout` sentinel) is debug-level; generic
// `TimedOut` from a transport could indicate a real handshake / keepalive
// problem and stays at warn.
fn log_forwarder_outcome(kind: &'static str, e: &io::Error) {
    if crate::forward::is_post_half_close_idle_timeout(e) {
        debug!(
            "Forwarder ({}) reaped by post-half-close idle timeout",
            kind
        );
    } else {
        warn!("Forwarder ({}) ended with error: {}", kind, e);
    }
}

// The entrypoint of running a server
pub async fn run_server(
    config: Config,
    shutdown_rx: broadcast::Receiver<bool>,
    update_rx: mpsc::Receiver<ConfigChange>,
) -> Result<()> {
    let config = match config.server {
            Some(config) => config,
            None => {
                return Err(anyhow!("Try to run as a server, but the configuration is missing. Please add the `[server]` block"))
            }
        };

    match config.transport.transport_type {
        TransportType::Tcp => {
            let mut server = Server::<TcpTransport>::from(config).await?;
            server.run(shutdown_rx, update_rx).await?;
        }
        TransportType::Tls => {
            #[cfg(any(feature = "native-tls", feature = "rustls"))]
            {
                let mut server = Server::<TlsTransport>::from(config).await?;
                server.run(shutdown_rx, update_rx).await?;
            }
            #[cfg(not(any(feature = "native-tls", feature = "rustls")))]
            crate::helper::feature_neither_compile("native-tls", "rustls")
        }
        TransportType::Noise => {
            #[cfg(feature = "noise")]
            {
                let mut server = Server::<NoiseTransport>::from(config).await?;
                server.run(shutdown_rx, update_rx).await?;
            }
            #[cfg(not(feature = "noise"))]
            crate::helper::feature_not_compile("noise")
        }
        TransportType::Websocket => {
            #[cfg(any(feature = "websocket-native-tls", feature = "websocket-rustls"))]
            {
                let mut server = Server::<WebsocketTransport>::from(config).await?;
                server.run(shutdown_rx, update_rx).await?;
            }
            #[cfg(not(any(feature = "websocket-native-tls", feature = "websocket-rustls")))]
            crate::helper::feature_neither_compile("websocket-native-tls", "websocket-rustls")
        }
    }

    Ok(())
}

// A hash map of ControlChannelHandles, indexed by ServiceDigest or Nonce
// See also MultiMap
type ControlChannelMap<T> = MultiMap<ServiceDigest, Nonce, ControlChannelHandle<T>>;

// Server holds all states of running a server
struct Server<T: Transport> {
    // `[server]` config
    config: Arc<ServerConfig>,

    // `[server.services]` config, indexed by ServiceDigest
    services: Arc<RwLock<HashMap<ServiceDigest, ServerServiceConfig>>>,
    // Collection of contorl channels
    control_channels: Arc<RwLock<ControlChannelMap<T>>>,
    // Wrapper around the transport layer
    transport: Arc<T>,
}

// Generate a hash map of services which is indexed by ServiceDigest
fn generate_service_hashmap(
    server_config: &ServerConfig,
) -> HashMap<ServiceDigest, ServerServiceConfig> {
    let mut ret = HashMap::new();
    for u in &server_config.services {
        ret.insert(protocol::digest(u.0.as_bytes()), (*u.1).clone());
    }
    ret
}

impl<T: 'static + Transport> Server<T> {
    // Create a server from `[server]`
    pub async fn from(config: ServerConfig) -> Result<Server<T>> {
        let config = Arc::new(config);
        let services = Arc::new(RwLock::new(generate_service_hashmap(&config)));
        let control_channels = Arc::new(RwLock::new(ControlChannelMap::new()));
        let transport = Arc::new(T::new(&config.transport)?);
        Ok(Server {
            config,
            services,
            control_channels,
            transport,
        })
    }

    // The entry point of Server
    pub async fn run(
        &mut self,
        mut shutdown_rx: broadcast::Receiver<bool>,
        mut update_rx: mpsc::Receiver<ConfigChange>,
    ) -> Result<()> {
        // Listen at `server.bind_addr`
        let l = self
            .transport
            .bind(&self.config.bind_addr)
            .await
            .with_context(|| "Failed to listen at `server.bind_addr`")?;
        info!("Listening at {}", self.config.bind_addr);

        // Retry at least every 100ms
        let mut backoff = ExponentialBackoff {
            max_interval: Duration::from_millis(100),
            max_elapsed_time: None,
            ..Default::default()
        };

        // Wait for connections and shutdown signals
        loop {
            tokio::select! {
                // Wait for incoming control and data channels
                ret = self.transport.accept(&l) => {
                    match ret {
                        Err(err) => {
                            // Detects whether it's an IO error
                            if let Some(err) = err.downcast_ref::<io::Error>() {
                                // If it is an IO error, then it's possibly an
                                // EMFILE. So sleep for a while and retry
                                // TODO: Only sleep for EMFILE, ENFILE, ENOMEM, ENOBUFS
                                if let Some(d) = backoff.next_backoff() {
                                    error!("Failed to accept: {:#}. Retry in {:?}...", err, d);
                                    time::sleep(d).await;
                                } else {
                                    // This branch will never be executed according to the current retry policy
                                    error!("Too many retries. Aborting...");
                                    break;
                                }
                            }
                            // If it's not an IO error, then it comes from
                            // the transport layer, so just ignore it
                        }
                        Ok((conn, addr)) => {
                            backoff.reset();

                            let transport = self.transport.clone();
                            let services = self.services.clone();
                            let control_channels = self.control_channels.clone();
                            let server_config = self.config.clone();

                            tokio::spawn(async move {
                                // Do transport handshake with a timeout. Keep this out of the
                                // accept loop so slow or bogus handshakes cannot block other
                                // clients from connecting.
                                match time::timeout(
                                    Duration::from_secs(HANDSHAKE_TIMEOUT),
                                    transport.handshake(conn),
                                )
                                .await
                                {
                                    Ok(conn) => match conn
                                        .with_context(|| "Failed to do transport handshake")
                                    {
                                        Ok(conn) => match time::timeout(
                                            Duration::from_secs(HANDSHAKE_TIMEOUT),
                                            handle_connection(
                                                conn,
                                                services,
                                                control_channels,
                                                server_config,
                                            ),
                                        )
                                        .await
                                        {
                                            Ok(Ok(())) => {}
                                            Ok(Err(err)) => {
                                                error!("{:#}", err);
                                            }
                                            Err(e) => {
                                                error!("Protocol handshake timeout: {}", e);
                                            }
                                        },
                                        Err(e) => {
                                            error!("{:#}", e);
                                        }
                                    },
                                    Err(e) => {
                                        error!("Transport handshake timeout: {}", e);
                                    }
                                }
                            }.instrument(info_span!("connection", %addr)));
                        }
                    }
                },
                // Wait for the shutdown signal
                _ = shutdown_rx.recv() => {
                    info!("Shuting down gracefully...");
                    break;
                },
                e = update_rx.recv() => {
                    if let Some(e) = e {
                        self.handle_hot_reload(e).await;
                    }
                }
            }
        }

        info!("Shutdown");

        Ok(())
    }

    async fn handle_hot_reload(&mut self, e: ConfigChange) {
        match e {
            ConfigChange::ServerChange(server_change) => match server_change {
                ServerServiceChange::Add(cfg) => {
                    let hash = protocol::digest(cfg.name.as_bytes());
                    let mut wg = self.services.write().await;
                    let _ = wg.insert(hash, cfg);

                    let mut wg = self.control_channels.write().await;
                    let _ = wg.remove1(&hash);
                }
                ServerServiceChange::Delete(s) => {
                    let hash = protocol::digest(s.as_bytes());
                    let _ = self.services.write().await.remove(&hash);

                    let mut wg = self.control_channels.write().await;
                    let _ = wg.remove1(&hash);
                }
            },
            ignored => warn!("Ignored {:?} since running as a server", ignored),
        }
    }
}

// Handle connections to `server.bind_addr`
async fn handle_connection<T: 'static + Transport>(
    mut conn: T::Stream,
    services: Arc<RwLock<HashMap<ServiceDigest, ServerServiceConfig>>>,
    control_channels: Arc<RwLock<ControlChannelMap<T>>>,
    server_config: Arc<ServerConfig>,
) -> Result<()> {
    // Read hello
    let hello = read_hello(&mut conn).await?;
    match hello {
        ControlChannelHello(_, service_digest) => {
            do_control_channel_handshake(
                conn,
                services,
                control_channels,
                service_digest,
                server_config,
            )
            .await?;
        }
        DataChannelHello(_, nonce) => {
            do_data_channel_handshake(conn, control_channels, nonce).await?;
        }
    }
    Ok(())
}

async fn do_control_channel_handshake<T: 'static + Transport>(
    mut conn: T::Stream,
    services: Arc<RwLock<HashMap<ServiceDigest, ServerServiceConfig>>>,
    control_channels: Arc<RwLock<ControlChannelMap<T>>>,
    service_digest: ServiceDigest,
    server_config: Arc<ServerConfig>,
) -> Result<()> {
    info!("Try to handshake a control channel");

    T::hint(&conn, SocketOpts::for_control_channel());

    // Generate a nonce
    let mut nonce = vec![0u8; HASH_WIDTH_IN_BYTES];
    rand::rng().fill(&mut nonce);

    // Send hello
    let hello_send = Hello::ControlChannelHello(
        protocol::CURRENT_PROTO_VERSION,
        nonce.clone().try_into().unwrap(),
    );
    conn.write_all(&protocol::encode(&hello_send).unwrap())
        .await?;
    conn.flush().await?;

    // Lookup the service
    let service_config = match services.read().await.get(&service_digest) {
        Some(v) => v,
        None => {
            conn.write_all(&protocol::encode(&Ack::ServiceNotExist).unwrap())
                .await?;
            bail!("No such a service {}", hex::encode(service_digest));
        }
    }
    .to_owned();

    let service_name = &service_config.name;

    // Calculate the checksum
    let mut concat = Vec::from(service_config.token.as_ref().unwrap().as_bytes());
    concat.append(&mut nonce);

    // Read auth
    let protocol::Auth(d) = read_auth(&mut conn).await?;

    // Validate
    let session_key = protocol::digest(&concat);
    if session_key != d {
        conn.write_all(&protocol::encode(&Ack::AuthFailed).unwrap())
            .await?;
        debug!(
            "Expect {}, but got {}",
            hex::encode(session_key),
            hex::encode(d)
        );
        bail!("Service {} failed the authentication", service_name);
    } else {
        let mut h = control_channels.write().await;

        // If there's already a control channel for the service, then drop the old one.
        // This handles reconnects that arrive before the previous control channel's
        // cleanup task has removed its handle from the map.
        if h.remove1(&service_digest).is_some() {
            warn!(
                "Dropping previous control channel for service {}",
                service_name
            );
        }

        // Send ack
        conn.write_all(&protocol::encode(&Ack::Ok).unwrap()).await?;
        conn.flush().await?;

        info!(service = %service_config.name, "Control channel established");
        let handle = ControlChannelHandle::new(
            conn,
            service_config,
            server_config.heartbeat_interval,
            server_config.post_half_close_idle_timeout.as_duration(),
            service_digest,
            session_key,
            Arc::downgrade(&control_channels),
        );

        // Insert the new handle
        let _ = h.insert(service_digest, session_key, handle);
    }

    Ok(())
}

async fn do_data_channel_handshake<T: 'static + Transport>(
    conn: T::Stream,
    control_channels: Arc<RwLock<ControlChannelMap<T>>>,
    nonce: Nonce,
) -> Result<()> {
    debug!("Try to handshake a data channel");

    // Validate
    let control_channels_guard = control_channels.read().await;
    match control_channels_guard.get2(&nonce) {
        Some(handle) => {
            T::hint(&conn, SocketOpts::from_server_cfg(&handle.service));

            // Send the data channel to the corresponding control channel
            handle
                .data_ch_tx
                .send(conn)
                .await
                .with_context(|| "Data channel for a stale control channel")?;
        }
        None => {
            warn!("Data channel has incorrect nonce");
        }
    }
    Ok(())
}

pub struct ControlChannelHandle<T: Transport> {
    // Shutdown the control channel by dropping it
    _shutdown_tx: broadcast::Sender<bool>,
    data_ch_tx: mpsc::Sender<T::Stream>,
    service: ServerServiceConfig,
}

impl<T> ControlChannelHandle<T>
where
    T: 'static + Transport,
{
    // Create a control channel handle, where the control channel handling task
    // and the connection pool task are created.
    #[instrument(name = "handle", skip_all, fields(service = %service.name))]
    fn new(
        conn: T::Stream,
        service: ServerServiceConfig,
        heartbeat_interval: u64,
        post_half_close_idle_timeout: Option<Duration>,
        service_digest: ServiceDigest,
        session_key: Nonce,
        control_channels: Weak<RwLock<ControlChannelMap<T>>>,
    ) -> ControlChannelHandle<T> {
        // Create a shutdown channel
        let (shutdown_tx, shutdown_rx) = broadcast::channel::<bool>(1);

        // Store data channels
        let (data_ch_tx, data_ch_rx) = mpsc::channel(CHAN_SIZE * 2);

        // Store data channel creation requests
        let (data_ch_req_tx, data_ch_req_rx) = mpsc::unbounded_channel();

        let shutdown_rx_clone = shutdown_tx.subscribe();
        let bind_addr = service.bind_addr.clone();
        match service.service_type {
            ServiceType::Tcp => tokio::spawn(
                async move {
                    if let Err(e) = run_tcp_connection_pool::<T>(
                        bind_addr,
                        post_half_close_idle_timeout,
                        data_ch_rx,
                        data_ch_req_tx,
                        TCP_POOL_SIZE,
                        shutdown_rx_clone,
                    )
                    .await
                    .with_context(|| "Failed to run TCP connection pool")
                    {
                        error!("{:#}", e);
                    }
                }
                .instrument(Span::current()),
            ),
            ServiceType::Udp => {
                for _i in 0..UDP_POOL_SIZE {
                    if let Err(e) = data_ch_req_tx.send(true) {
                        error!("Failed to request data channel {}", e);
                    };
                }

                tokio::spawn(
                    async move {
                        if let Err(e) = run_udp_connection_pool::<T>(
                            bind_addr,
                            data_ch_rx,
                            data_ch_req_tx,
                            shutdown_rx_clone,
                        )
                        .await
                        .with_context(|| "Failed to run TCP connection pool")
                        {
                            error!("{:#}", e);
                        }
                    }
                    .instrument(Span::current()),
                )
            }
        };

        // Create the control channel
        let ch = ControlChannel::<T> {
            conn,
            shutdown_rx,
            data_ch_req_rx,
            heartbeat_interval,
        };

        // Run the control channel
        let service_name = service.name.clone();
        tokio::spawn(
            async move {
                if let Err(err) = ch.run().await {
                    error!("{:#}", err);
                }

                if let Some(control_channels) = control_channels.upgrade() {
                    if control_channels
                        .write()
                        .await
                        .remove2(&session_key)
                        .is_some()
                    {
                        debug!(
                            service = %service_name,
                            digest = %hex::encode(service_digest),
                            "Removed stale control channel"
                        );
                    }
                }
            }
            .instrument(Span::current()),
        );

        ControlChannelHandle {
            _shutdown_tx: shutdown_tx,
            data_ch_tx,
            service,
        }
    }
}

// Control channel, using T as the transport layer. P is TcpStream or UdpTraffic
struct ControlChannel<T: Transport> {
    conn: T::Stream,                               // The connection of control channel
    shutdown_rx: broadcast::Receiver<bool>,        // Receives the shutdown signal
    data_ch_req_rx: mpsc::UnboundedReceiver<bool>, // Receives visitor connections
    heartbeat_interval: u64,                       // Application-layer heartbeat interval in secs
}

impl<T: Transport> ControlChannel<T> {
    async fn write_and_flush(&mut self, data: &[u8]) -> Result<()> {
        write_and_flush(&mut self.conn, data)
            .await
            .with_context(|| "Failed to write control cmds")?;
        Ok(())
    }
    // Run a control channel
    #[instrument(skip_all)]
    async fn run(mut self) -> Result<()> {
        let create_ch_cmd = protocol::encode(&ControlChannelCmd::CreateDataChannel).unwrap();
        let heartbeat = protocol::encode(&ControlChannelCmd::HeartBeat).unwrap();

        // Wait for data channel requests and the shutdown signal
        loop {
            tokio::select! {
                val = self.data_ch_req_rx.recv() => {
                    match val {
                        Some(_) => {
                            debug!("Sending {:?}", ControlChannelCmd::CreateDataChannel);
                            if let Err(e) = self.write_and_flush(&create_ch_cmd).await {
                                error!("{:#}", e);
                                break;
                            }
                            debug!("Sent {:?}", ControlChannelCmd::CreateDataChannel);
                        }
                        None => {
                            break;
                        }
                    }
                },
                _ = time::sleep(Duration::from_secs(self.heartbeat_interval)), if self.heartbeat_interval != 0 => {
                            debug!("Sending {:?}", ControlChannelCmd::HeartBeat);
                            if let Err(e) = self.write_and_flush(&heartbeat).await {
                                error!("{:#}", e);
                                break;
                            }
                            debug!("Sent {:?}", ControlChannelCmd::HeartBeat);
                }
                // Wait for the shutdown signal
                _ = self.shutdown_rx.recv() => {
                    break;
                }
            }
        }

        info!("Control channel shutdown");

        Ok(())
    }
}

struct PendingTcpVisitor {
    stream: TcpStream,
    accepted_at: time::Instant,
}

fn tcp_listen_and_send(
    addr: String,
    mut shutdown_rx: broadcast::Receiver<bool>,
) -> mpsc::Receiver<PendingTcpVisitor> {
    let (tx, rx) = mpsc::channel(CHAN_SIZE);

    tokio::spawn(
        async move {
            let l = retry_notify_with_deadline(
                listen_backoff(),
                || async { Ok(TcpListener::bind(&addr).await?) },
                |e, duration| {
                    error!("{:#}. Retry in {:?}", e, duration);
                },
                &mut shutdown_rx,
            )
            .await
            .with_context(|| "Failed to listen for the service");

            let l: TcpListener = match l {
                Ok(v) => v,
                Err(e) => {
                    error!("{:#}", e);
                    return;
                }
            };

            info!("Listening at {}", &addr);

            // Retry at least every 1s
            let mut backoff = ExponentialBackoff {
                max_interval: Duration::from_secs(1),
                max_elapsed_time: None,
                ..Default::default()
            };

            // Wait for visitors and the shutdown signal
            loop {
                tokio::select! {
                    val = l.accept() => {
                        match val {
                            Err(e) => {
                                // `l` is a TCP listener so this must be a IO error
                                // Possibly a EMFILE. So sleep for a while
                                error!("{}. Sleep for a while", e);
                                if let Some(d) = backoff.next_backoff() {
                                    time::sleep(d).await;
                                } else {
                                    // This branch will never be reached for current backoff policy
                                    error!("Too many retries. Aborting...");
                                    break;
                                }
                            }
                            Ok((incoming, addr)) => {
                                backoff.reset();

                                debug!("New visitor from {}", addr);

                                // Send the visitor to the connection pool
                                if tx
                                    .send(PendingTcpVisitor {
                                        stream: incoming,
                                        accepted_at: time::Instant::now(),
                                    })
                                    .await
                                    .is_err()
                                {
                                    break;
                                }
                            }
                        }
                    },
                    _ = shutdown_rx.recv() => {
                        break;
                    }
                }
            }

            info!("TCPListener shutdown");
        }
        .instrument(Span::current()),
    );

    rx
}

struct PooledTcpDataChannel<S> {
    conn: S,
    last_checked: time::Instant,
}

enum TcpPoolCheckResult<S> {
    Healthy(S),
    Unhealthy,
}

async fn check_tcp_pool_channel<S>(conn: S, heartbeat_cmd: Vec<u8>) -> Result<S>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    check_tcp_pool_channel_with_timeout(
        conn,
        heartbeat_cmd,
        Duration::from_secs(TCP_POOL_HEARTBEAT_TIMEOUT),
    )
    .await
}

async fn check_tcp_pool_channel_with_timeout<S>(
    mut conn: S,
    heartbeat_cmd: Vec<u8>,
    timeout: Duration,
) -> Result<S>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let heartbeat = async move {
        debug!("Checking idle TCP data channel");
        write_and_flush(&mut conn, &heartbeat_cmd).await?;
        match read_ack(&mut conn).await? {
            Ack::Ok => {
                debug!("Idle TCP data channel heartbeat succeeded");
                Ok(conn)
            }
            v => Err(anyhow!("Unexpected data channel heartbeat response: {}", v)),
        }
    };

    match time::timeout(timeout, heartbeat).await {
        Ok(result) => result,
        Err(_) => Err(anyhow!("Data channel heartbeat timed out")),
    }
}

async fn refresh_tcp_pool<S>(
    pool: &mut VecDeque<PooledTcpDataChannel<S>>,
    checked_ch_tx: mpsc::Sender<TcpPoolCheckResult<S>>,
    heartbeat_cmd: &[u8],
) -> usize
where
    S: 'static + AsyncRead + AsyncWrite + Unpin + Send,
{
    refresh_tcp_pool_with_timeout(
        pool,
        checked_ch_tx,
        heartbeat_cmd,
        Duration::from_secs(TCP_POOL_HEARTBEAT_TIMEOUT),
    )
    .await
}

async fn refresh_tcp_pool_with_timeout<S>(
    pool: &mut VecDeque<PooledTcpDataChannel<S>>,
    checked_ch_tx: mpsc::Sender<TcpPoolCheckResult<S>>,
    heartbeat_cmd: &[u8],
    heartbeat_timeout: Duration,
) -> usize
where
    S: 'static + AsyncRead + AsyncWrite + Unpin + Send,
{
    let now = time::Instant::now();
    let mut spawned = 0usize;

    let pool_len = pool.len();
    for _ in 0..pool_len {
        let Some(pooled) = pool.pop_front() else {
            break;
        };

        if now.duration_since(pooled.last_checked)
            < Duration::from_secs(TCP_POOL_HEARTBEAT_INTERVAL)
        {
            pool.push_back(pooled);
            continue;
        }

        spawned += 1;
        let checked_ch_tx = checked_ch_tx.clone();
        let heartbeat_cmd = heartbeat_cmd.to_vec();
        tokio::spawn(async move {
            match check_tcp_pool_channel_with_timeout(pooled.conn, heartbeat_cmd, heartbeat_timeout)
                .await
            {
                Ok(conn) => {
                    if checked_ch_tx
                        .send(TcpPoolCheckResult::Healthy(conn))
                        .await
                        .is_err()
                    {
                        debug!("Dropping healthy TCP data channel after pool shutdown");
                    }
                }
                Err(e) => {
                    debug!("Dropping unhealthy idle TCP data channel: {:#}", e);
                    let _ = checked_ch_tx.send(TcpPoolCheckResult::Unhealthy).await;
                }
            }
        });
    }

    spawned
}

#[cfg(test)]
async fn next_tcp_pool_channel<S>(
    pool: &mut VecDeque<PooledTcpDataChannel<S>>,
    data_ch_rx: &mut mpsc::Receiver<S>,
    data_ch_req_tx: &mpsc::UnboundedSender<bool>,
    heartbeat_cmd: &[u8],
) -> Option<S>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    loop {
        if let Some(pooled) = pool.pop_front() {
            if pooled.last_checked.elapsed() < Duration::from_secs(TCP_POOL_HEARTBEAT_INTERVAL) {
                return Some(pooled.conn);
            }

            match check_tcp_pool_channel(pooled.conn, heartbeat_cmd.to_vec()).await {
                Ok(conn) => return Some(conn),
                Err(e) => {
                    debug!("Dropping unhealthy idle TCP data channel: {:#}", e);
                    if data_ch_req_tx.send(true).is_err() {
                        return None;
                    }
                }
            }
        } else {
            return data_ch_rx.recv().await;
        }
    }
}

fn request_data_channels(
    data_ch_req_tx: &mpsc::UnboundedSender<bool>,
    pending_data_channels: &mut VecDeque<time::Instant>,
    count: usize,
) -> bool {
    let now = time::Instant::now();
    for _ in 0..count {
        if data_ch_req_tx.send(true).is_err() {
            return false;
        }
        pending_data_channels.push_back(now);
    }
    true
}

fn expire_pending_data_channels(
    pending_data_channels: &mut VecDeque<time::Instant>,
    request_timeout: Duration,
) {
    let now = time::Instant::now();
    let before = pending_data_channels.len();

    while matches!(
        pending_data_channels.front(),
        Some(created_at) if now.duration_since(*created_at) >= request_timeout
    ) {
        pending_data_channels.pop_front();
    }

    let expired = before - pending_data_channels.len();
    if expired > 0 {
        warn!(
            expired,
            "Expired pending TCP data channel requests before a data channel arrived"
        );
    }
}

fn ensure_tcp_data_channels(
    pool_len: usize,
    queued_visitors: usize,
    checking_pool_channels: usize,
    pool_target: usize,
    pending_data_channels: &mut VecDeque<time::Instant>,
    data_ch_req_tx: &mpsc::UnboundedSender<bool>,
) -> bool {
    let desired = pool_target.saturating_add(queued_visitors);
    let available = pool_len
        .saturating_add(checking_pool_channels)
        .saturating_add(pending_data_channels.len());

    if desired <= available {
        return true;
    }

    request_data_channels(data_ch_req_tx, pending_data_channels, desired - available)
}

fn drop_stale_tcp_visitors(
    visitor_queue: &mut VecDeque<PendingTcpVisitor>,
    visitor_wait_timeout: Duration,
) {
    let now = time::Instant::now();
    let before = visitor_queue.len();
    visitor_queue.retain(|visitor| now.duration_since(visitor.accepted_at) < visitor_wait_timeout);
    let dropped = before - visitor_queue.len();

    if dropped > 0 {
        warn!(
            dropped,
            "Dropped stale TCP visitors that waited too long for data channels"
        );
    }
}

async fn pop_tcp_pool_channel<S>(
    pool: &mut VecDeque<PooledTcpDataChannel<S>>,
    data_ch_req_tx: &mpsc::UnboundedSender<bool>,
    pending_data_channels: &mut VecDeque<time::Instant>,
    heartbeat_cmd: &[u8],
) -> Option<S>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    loop {
        let pooled = pool.pop_front()?;
        if pooled.last_checked.elapsed() < Duration::from_secs(TCP_POOL_HEARTBEAT_INTERVAL) {
            return Some(pooled.conn);
        }

        match check_tcp_pool_channel(pooled.conn, heartbeat_cmd.to_vec()).await {
            Ok(conn) => return Some(conn),
            Err(e) => {
                debug!("Dropping unhealthy idle TCP data channel: {:#}", e);
                if !request_data_channels(data_ch_req_tx, pending_data_channels, 1) {
                    return None;
                }
            }
        }
    }
}

struct TcpVisitorServeConfig<'a> {
    heartbeat_cmd: &'a [u8],
    start_forward_cmd: &'a [u8],
    post_half_close_idle_timeout: Option<Duration>,
    visitor_wait_timeout: Duration,
    data_channel_request_timeout: Duration,
}

async fn serve_ready_tcp_visitors<T: 'static + Transport>(
    visitor_queue: &mut VecDeque<PendingTcpVisitor>,
    pool: &mut VecDeque<PooledTcpDataChannel<T::Stream>>,
    data_ch_req_tx: &mpsc::UnboundedSender<bool>,
    pending_data_channels: &mut VecDeque<time::Instant>,
    config: &TcpVisitorServeConfig<'_>,
) -> bool {
    loop {
        drop_stale_tcp_visitors(visitor_queue, config.visitor_wait_timeout);

        let Some(visitor) = visitor_queue.pop_front() else {
            return true;
        };

        loop {
            let Some(mut ch) = pop_tcp_pool_channel(
                pool,
                data_ch_req_tx,
                pending_data_channels,
                config.heartbeat_cmd,
            )
            .await
            else {
                visitor_queue.push_front(visitor);
                return true;
            };

            match time::timeout(
                config.data_channel_request_timeout,
                write_and_flush(&mut ch, config.start_forward_cmd),
            )
            .await
            {
                Ok(Ok(())) => {
                    let v = visitor.stream;
                    let post_half_close_idle_timeout = config.post_half_close_idle_timeout;
                    tokio::spawn(async move {
                        if let Err(e) = T::forward_tcp(ch, v, post_half_close_idle_timeout).await {
                            log_forwarder_outcome("tcp", &e);
                        }
                    });
                    break;
                }
                Ok(Err(e)) => {
                    debug!(
                        "Dropping TCP data channel that rejected start command: {:#}",
                        e
                    );
                }
                Err(_) => {
                    debug!("Dropping TCP data channel that timed out on start command");
                }
            }

            if !request_data_channels(data_ch_req_tx, pending_data_channels, 1) {
                return false;
            }
        }
    }
}

#[instrument(skip_all)]
async fn run_tcp_connection_pool<T: 'static + Transport>(
    bind_addr: String,
    post_half_close_idle_timeout: Option<Duration>,
    mut data_ch_rx: mpsc::Receiver<T::Stream>,
    data_ch_req_tx: mpsc::UnboundedSender<bool>,
    pool_target: usize,
    shutdown_rx: broadcast::Receiver<bool>,
) -> Result<()> {
    let mut visitor_rx = tcp_listen_and_send(bind_addr, shutdown_rx);
    let cmd = protocol::encode(&DataChannelCmd::StartForwardTcp).unwrap();
    let heartbeat_cmd = protocol::encode(&DataChannelCmd::HeartBeat).unwrap();
    let mut pool: VecDeque<PooledTcpDataChannel<T::Stream>> = VecDeque::new();
    let mut visitor_queue: VecDeque<PendingTcpVisitor> = VecDeque::new();
    let mut pending_data_channels: VecDeque<time::Instant> = VecDeque::new();
    let mut checking_pool_channels = 0usize;
    let (checked_ch_tx, mut checked_ch_rx) = mpsc::channel(CHAN_SIZE);
    let mut heartbeat_interval = time::interval(Duration::from_secs(TCP_POOL_HEARTBEAT_INTERVAL));
    let mut visitor_cleanup_interval = time::interval(Duration::from_secs(1));
    let visitor_wait_timeout = Duration::from_secs(TCP_VISITOR_WAIT_TIMEOUT);
    let data_channel_request_timeout = Duration::from_secs(TCP_DATA_CHANNEL_REQUEST_TIMEOUT);
    let visitor_serve_config = TcpVisitorServeConfig {
        heartbeat_cmd: &heartbeat_cmd,
        start_forward_cmd: &cmd,
        post_half_close_idle_timeout,
        visitor_wait_timeout,
        data_channel_request_timeout,
    };

    loop {
        expire_pending_data_channels(&mut pending_data_channels, data_channel_request_timeout);

        if !serve_ready_tcp_visitors::<T>(
            &mut visitor_queue,
            &mut pool,
            &data_ch_req_tx,
            &mut pending_data_channels,
            &visitor_serve_config,
        )
        .await
        {
            break;
        }

        if !ensure_tcp_data_channels(
            pool.len(),
            visitor_queue.len(),
            checking_pool_channels,
            pool_target,
            &mut pending_data_channels,
            &data_ch_req_tx,
        ) {
            break;
        }

        tokio::select! {
            val = data_ch_rx.recv() => {
                match val {
                    Some(conn) => {
                        let _ = pending_data_channels.pop_front();
                        if visitor_queue.is_empty() && pool.len() >= pool_target {
                            debug!("Dropping extra TCP data channel after pool is full");
                        } else {
                            pool.push_back(PooledTcpDataChannel {
                                conn,
                                last_checked: time::Instant::now(),
                            });
                        }
                    },
                    None => break,
                }
            },
            val = visitor_rx.recv() => {
                let Some(visitor) = val else {
                    break;
                };
                visitor_queue.push_back(visitor);
            },
            val = checked_ch_rx.recv() => {
                let Some(result) = val else {
                    break;
                };
                checking_pool_channels = checking_pool_channels.saturating_sub(1);
                match result {
                    TcpPoolCheckResult::Healthy(conn) => {
                        if visitor_queue.is_empty() && pool.len() >= pool_target {
                            debug!("Dropping healthy TCP data channel after pool is full");
                        } else {
                            pool.push_back(PooledTcpDataChannel {
                                conn,
                                last_checked: time::Instant::now(),
                            });
                        }
                    }
                    TcpPoolCheckResult::Unhealthy => {}
                }
            },
            _ = heartbeat_interval.tick() => {
                checking_pool_channels += refresh_tcp_pool(
                    &mut pool,
                    checked_ch_tx.clone(),
                    &heartbeat_cmd,
                )
                .await;
            },
            _ = visitor_cleanup_interval.tick() => {
                expire_pending_data_channels(&mut pending_data_channels, data_channel_request_timeout);
                drop_stale_tcp_visitors(&mut visitor_queue, visitor_wait_timeout);
            }
        }
    }

    info!("Shutdown");
    Ok(())
}

#[instrument(skip_all)]
async fn run_udp_connection_pool<T: Transport>(
    bind_addr: String,
    mut data_ch_rx: mpsc::Receiver<T::Stream>,
    _data_ch_req_tx: mpsc::UnboundedSender<bool>,
    mut shutdown_rx: broadcast::Receiver<bool>,
) -> Result<()> {
    // TODO: Load balance

    let l = retry_notify_with_deadline(
        listen_backoff(),
        || async { Ok(UdpSocket::bind(&bind_addr).await?) },
        |e, duration| {
            warn!("{:#}. Retry in {:?}", e, duration);
        },
        &mut shutdown_rx,
    )
    .await
    .with_context(|| "Failed to listen for the service")?;

    info!("Listening at {}", &bind_addr);

    let cmd = protocol::encode(&DataChannelCmd::StartForwardUdp).unwrap();

    // Receive one data channel
    let mut conn = data_ch_rx
        .recv()
        .await
        .ok_or_else(|| anyhow!("No available data channels"))?;
    write_and_flush(&mut conn, &cmd).await?;

    let mut buf = [0u8; UDP_BUFFER_SIZE];
    loop {
        tokio::select! {
            // Forward inbound traffic to the client
            val = l.recv_from(&mut buf) => {
                let (n, from) = val?;
                UdpTraffic::write_slice(&mut conn, from, &buf[..n]).await?;
            },

            // Forward outbound traffic from the client to the visitor
            hdr_len = conn.read_u8() => {
                let t = UdpTraffic::read(&mut conn, hdr_len?).await?;
                l.send_to(&t.data, t.from).await?;
            }

            _ = shutdown_rx.recv() => {
                break;
            }
        }
    }

    debug!("UDP pool dropped");

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::pin::Pin;
    use std::task::{Context, Poll};
    use tokio::io::duplex;
    use tokio::io::ReadBuf;

    #[derive(Debug)]
    struct PendingHeartbeatWrite;

    impl AsyncRead for PendingHeartbeatWrite {
        fn poll_read(
            self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
            _buf: &mut ReadBuf<'_>,
        ) -> Poll<io::Result<()>> {
            Poll::Pending
        }
    }

    impl AsyncWrite for PendingHeartbeatWrite {
        fn poll_write(
            self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
            _buf: &[u8],
        ) -> Poll<io::Result<usize>> {
            Poll::Pending
        }

        fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Pending
        }

        fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Ready(Ok(()))
        }
    }

    #[tokio::test]
    async fn tcp_pool_heartbeat_accepts_ack() -> Result<()> {
        let (server_side, mut client_side) = duplex(64);
        let heartbeat_cmd = protocol::encode(&DataChannelCmd::HeartBeat).unwrap();

        let client = tokio::spawn(async move {
            assert!(matches!(
                crate::protocol::read_data_cmd(&mut client_side)
                    .await
                    .unwrap(),
                DataChannelCmd::HeartBeat
            ));
            write_and_flush(&mut client_side, &protocol::encode(&Ack::Ok).unwrap())
                .await
                .unwrap();
        });

        let _server_side = check_tcp_pool_channel(server_side, heartbeat_cmd).await?;
        client.await?;
        Ok(())
    }

    #[tokio::test]
    async fn stale_tcp_pool_channel_is_replaced_before_forwarding() -> Result<()> {
        let (stale_server_side, stale_client_side) = duplex(64);
        drop(stale_client_side);

        let (fresh_server_side, mut fresh_client_side) = duplex(64);
        let (data_ch_req_tx, mut data_ch_req_rx) = mpsc::unbounded_channel();
        let (data_ch_tx, mut data_ch_rx) = mpsc::channel(1);
        data_ch_tx.send(fresh_server_side).await.unwrap();

        let mut pool = VecDeque::new();
        pool.push_back(PooledTcpDataChannel {
            conn: stale_server_side,
            last_checked: time::Instant::now()
                - Duration::from_secs(TCP_POOL_HEARTBEAT_INTERVAL + 1),
        });

        let heartbeat_cmd = protocol::encode(&DataChannelCmd::HeartBeat).unwrap();
        let mut selected =
            next_tcp_pool_channel(&mut pool, &mut data_ch_rx, &data_ch_req_tx, &heartbeat_cmd)
                .await
                .expect("fresh data channel should replace stale pool entry");

        assert!(data_ch_req_rx.try_recv().is_ok());

        write_and_flush(
            &mut selected,
            &protocol::encode(&DataChannelCmd::StartForwardTcp).unwrap(),
        )
        .await?;
        assert!(matches!(
            crate::protocol::read_data_cmd(&mut fresh_client_side).await?,
            DataChannelCmd::StartForwardTcp
        ));
        Ok(())
    }

    #[tokio::test]
    async fn tcp_pool_heartbeat_times_out_when_write_stalls() -> Result<()> {
        let err = check_tcp_pool_channel_with_timeout(
            PendingHeartbeatWrite,
            protocol::encode(&DataChannelCmd::HeartBeat).unwrap(),
            Duration::from_millis(10),
        )
        .await
        .unwrap_err();

        assert!(
            err.to_string().contains("timed out"),
            "unexpected heartbeat error: {err:#}"
        );
        Ok(())
    }

    #[tokio::test]
    async fn expired_pending_data_channel_requests_are_reissued() -> Result<()> {
        let (data_ch_req_tx, mut data_ch_req_rx) = mpsc::unbounded_channel();
        let mut pending = VecDeque::new();
        pending.push_back(time::Instant::now() - Duration::from_secs(2));

        expire_pending_data_channels(&mut pending, Duration::from_secs(1));

        assert!(pending.is_empty());
        assert!(ensure_tcp_data_channels(
            0,
            1,
            0,
            1,
            &mut pending,
            &data_ch_req_tx,
        ));
        assert_eq!(pending.len(), 2);
        assert!(matches!(data_ch_req_rx.try_recv(), Ok(true)));
        assert!(matches!(data_ch_req_rx.try_recv(), Ok(true)));
        assert!(data_ch_req_rx.try_recv().is_err());
        Ok(())
    }

    #[tokio::test]
    async fn stale_tcp_visitors_are_closed_when_data_channels_do_not_arrive() -> Result<()> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let addr = listener.local_addr()?;
        let (mut visitor_client, (visitor_server, _)) =
            tokio::try_join!(TcpStream::connect(addr), listener.accept())?;
        let mut visitor_queue = VecDeque::new();
        visitor_queue.push_back(PendingTcpVisitor {
            stream: visitor_server,
            accepted_at: time::Instant::now() - Duration::from_secs(2),
        });

        drop_stale_tcp_visitors(&mut visitor_queue, Duration::from_secs(1));

        assert!(visitor_queue.is_empty());
        let mut buf = [0u8; 1];
        let n = time::timeout(Duration::from_secs(1), visitor_client.read(&mut buf)).await??;
        assert_eq!(n, 0);
        Ok(())
    }

    #[tokio::test]
    async fn tcp_pool_refresh_does_not_block_on_stalled_heartbeat() -> Result<()> {
        let (checked_ch_tx, mut checked_ch_rx) = mpsc::channel(1);
        let mut pool = VecDeque::new();
        pool.push_back(PooledTcpDataChannel {
            conn: PendingHeartbeatWrite,
            last_checked: time::Instant::now()
                - Duration::from_secs(TCP_POOL_HEARTBEAT_INTERVAL + 1),
        });

        time::timeout(
            Duration::from_millis(50),
            refresh_tcp_pool_with_timeout(
                &mut pool,
                checked_ch_tx,
                &protocol::encode(&DataChannelCmd::HeartBeat).unwrap(),
                Duration::from_millis(10),
            ),
        )
        .await?;

        assert!(pool.is_empty());
        assert!(matches!(
            time::timeout(Duration::from_secs(1), checked_ch_rx.recv()).await?,
            Some(TcpPoolCheckResult::Unhealthy)
        ));
        Ok(())
    }

    #[tokio::test]
    async fn tcp_pool_refresh_scans_fresh_channels_once() -> Result<()> {
        let (checked_ch_tx, mut checked_ch_rx) = mpsc::channel(1);
        let mut pool = VecDeque::new();
        let (server_side_1, _client_side_1) = duplex(64);
        let (server_side_2, _client_side_2) = duplex(64);

        pool.push_back(PooledTcpDataChannel {
            conn: server_side_1,
            last_checked: time::Instant::now(),
        });
        pool.push_back(PooledTcpDataChannel {
            conn: server_side_2,
            last_checked: time::Instant::now(),
        });

        time::timeout(
            Duration::from_millis(50),
            refresh_tcp_pool_with_timeout(
                &mut pool,
                checked_ch_tx,
                &protocol::encode(&DataChannelCmd::HeartBeat).unwrap(),
                Duration::from_millis(10),
            ),
        )
        .await?;

        assert_eq!(pool.len(), 2);
        assert!(checked_ch_rx.try_recv().is_err());
        Ok(())
    }
}
