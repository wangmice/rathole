use crate::config::{Config, ServerConfig, ServerServiceConfig, ServiceType, TransportType};
use crate::config_watcher::{ConfigChange, ServerServiceChange};
use crate::constants::{listen_backoff, UDP_BUFFER_SIZE};
use crate::helper::{retry_notify_with_deadline, write_and_flush};
use crate::multi_map::MultiMap;
use crate::protocol::Hello::{ControlChannelHello, DataChannelHello};
use crate::protocol::{
    self, read_ack, read_auth, read_hello, Ack, ControlChannelCmd, DataChannelCmd, Hello, UdpTraffic,
    HASH_WIDTH_IN_BYTES,
};
use crate::forward::forward_bidirectional_with_idle_timeout;
use crate::transport::{SocketOpts, TcpTransport, Transport};
use anyhow::{anyhow, bail, Context, Result};
use backoff::backoff::Backoff;
use backoff::ExponentialBackoff;

use rand::RngCore;
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

// Surface the outcome of a forwarder task. Only the leak-guard reaper
// (the typed `PostHalfCloseIdleTimeout` sentinel) is debug-level; generic
// `TimedOut` from a transport could indicate a real handshake / keepalive
// problem and stays at warn.
fn log_forwarder_outcome(kind: &'static str, e: &io::Error) {
    if crate::forward::is_post_half_close_idle_timeout(e) {
        debug!("Forwarder ({}) reaped by post-half-close idle timeout", kind);
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
                                        Ok(conn) => {
                                            if let Err(err) = handle_connection(
                                                conn,
                                                services,
                                                control_channels,
                                                server_config,
                                            )
                                            .await
                                            {
                                                error!("{:#}", err);
                                            }
                                        }
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
    rand::thread_rng().fill_bytes(&mut nonce);

    // Send hello
    let hello_send = Hello::ControlChannelHello(
        protocol::CURRENT_PROTO_VERSION,
        nonce.clone().try_into().unwrap(),
    );
    conn.write_all(&bincode::serialize(&hello_send).unwrap())
        .await?;
    conn.flush().await?;

    // Lookup the service
    let service_config = match services.read().await.get(&service_digest) {
        Some(v) => v,
        None => {
            conn.write_all(&bincode::serialize(&Ack::ServiceNotExist).unwrap())
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
        conn.write_all(&bincode::serialize(&Ack::AuthFailed).unwrap())
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
        conn.write_all(&bincode::serialize(&Ack::Ok).unwrap())
            .await?;
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

        // Cache some data channels for later use
        let pool_size = match service.service_type {
            ServiceType::Tcp => TCP_POOL_SIZE,
            ServiceType::Udp => UDP_POOL_SIZE,
        };

        for _i in 0..pool_size {
            if let Err(e) = data_ch_req_tx.send(true) {
                error!("Failed to request data channel {}", e);
            };
        }

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
            ServiceType::Udp => tokio::spawn(
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
            ),
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
                    if control_channels.write().await.remove2(&session_key).is_some() {
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
        let create_ch_cmd = bincode::serialize(&ControlChannelCmd::CreateDataChannel).unwrap();
        let heartbeat = bincode::serialize(&ControlChannelCmd::HeartBeat).unwrap();

        // Wait for data channel requests and the shutdown signal
        loop {
            tokio::select! {
                val = self.data_ch_req_rx.recv() => {
                    match val {
                        Some(_) => {
                            if let Err(e) = self.write_and_flush(&create_ch_cmd).await {
                                error!("{:#}", e);
                                break;
                            }
                        }
                        None => {
                            break;
                        }
                    }
                },
                _ = time::sleep(Duration::from_secs(self.heartbeat_interval)), if self.heartbeat_interval != 0 => {
                            if let Err(e) = self.write_and_flush(&heartbeat).await {
                                error!("{:#}", e);
                                break;
                            }
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

fn tcp_listen_and_send(
    addr: String,
    data_ch_req_tx: mpsc::UnboundedSender<bool>,
    mut shutdown_rx: broadcast::Receiver<bool>,
) -> mpsc::Receiver<TcpStream> {
    let (tx, rx) = mpsc::channel(CHAN_SIZE);

    tokio::spawn(async move {
        let l = retry_notify_with_deadline(listen_backoff(),  || async {
            Ok(TcpListener::bind(&addr).await?)
        }, |e, duration| {
            error!("{:#}. Retry in {:?}", e, duration);
        }, &mut shutdown_rx).await
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
                            // For every visitor, request to create a data channel
                            if data_ch_req_tx.send(true).with_context(|| "Failed to send data chan create request").is_err() {
                                // An error indicates the control channel is broken
                                // So break the loop
                                break;
                            }

                            backoff.reset();

                            debug!("New visitor from {}", addr);

                            // Send the visitor to the connection pool
                            let _ = tx.send(incoming).await;
                        }
                    }
                },
                _ = shutdown_rx.recv() => {
                    break;
                }
            }
        }

        info!("TCPListener shutdown");
    }.instrument(Span::current()));

    rx
}

struct PooledTcpDataChannel<S> {
    conn: S,
    last_checked: time::Instant,
}

async fn check_tcp_pool_channel<S>(mut conn: S, heartbeat_cmd: Vec<u8>) -> Result<S>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    write_and_flush(&mut conn, &heartbeat_cmd).await?;
    match time::timeout(
        Duration::from_secs(TCP_POOL_HEARTBEAT_TIMEOUT),
        read_ack(&mut conn),
    )
    .await
    {
        Ok(Ok(Ack::Ok)) => Ok(conn),
        Ok(Ok(v)) => Err(anyhow!("Unexpected data channel heartbeat response: {}", v)),
        Ok(Err(e)) => Err(e),
        Err(_) => Err(anyhow!("Data channel heartbeat timed out")),
    }
}

async fn refresh_tcp_pool<S>(
    pool: &mut VecDeque<PooledTcpDataChannel<S>>,
    data_ch_req_tx: &mpsc::UnboundedSender<bool>,
    heartbeat_cmd: &[u8],
) where
    S: 'static + AsyncRead + AsyncWrite + Unpin + Send,
{
    let mut pending = Vec::new();
    let now = time::Instant::now();

    while let Some(pooled) = pool.pop_front() {
        if now.duration_since(pooled.last_checked)
            < Duration::from_secs(TCP_POOL_HEARTBEAT_INTERVAL)
        {
            pool.push_back(pooled);
            continue;
        }

        pending.push(tokio::spawn(check_tcp_pool_channel(
            pooled.conn,
            heartbeat_cmd.to_vec(),
        )));
    }

    for task in pending {
        match task.await {
            Ok(Ok(conn)) => pool.push_back(PooledTcpDataChannel {
                conn,
                last_checked: time::Instant::now(),
            }),
            Ok(Err(e)) => {
                debug!("Dropping unhealthy idle TCP data channel: {:#}", e);
                let _ = data_ch_req_tx.send(true);
            }
            Err(e) => {
                warn!("TCP data channel heartbeat task failed: {}", e);
                let _ = data_ch_req_tx.send(true);
            }
        }
    }
}

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

#[instrument(skip_all)]
async fn run_tcp_connection_pool<T: 'static + Transport>(
    bind_addr: String,
    post_half_close_idle_timeout: Option<Duration>,
    mut data_ch_rx: mpsc::Receiver<T::Stream>,
    data_ch_req_tx: mpsc::UnboundedSender<bool>,
    shutdown_rx: broadcast::Receiver<bool>,
) -> Result<()> {
    let mut visitor_rx = tcp_listen_and_send(bind_addr, data_ch_req_tx.clone(), shutdown_rx);
    let cmd = bincode::serialize(&DataChannelCmd::StartForwardTcp).unwrap();
    let heartbeat_cmd = bincode::serialize(&DataChannelCmd::HeartBeat).unwrap();
    let mut pool: VecDeque<PooledTcpDataChannel<T::Stream>> = VecDeque::new();
    let mut heartbeat_interval =
        time::interval(Duration::from_secs(TCP_POOL_HEARTBEAT_INTERVAL));

    loop {
        tokio::select! {
            val = data_ch_rx.recv() => {
                match val {
                    Some(conn) => pool.push_back(PooledTcpDataChannel {
                        conn,
                        last_checked: time::Instant::now(),
                    }),
                    None => break,
                }
            },
            val = visitor_rx.recv() => {
                let Some(visitor) = val else {
                    break;
                };
                let mut visitor = Some(visitor);

                loop {
                    if let Some(mut ch) = next_tcp_pool_channel(
                        &mut pool,
                        &mut data_ch_rx,
                        &data_ch_req_tx,
                        &heartbeat_cmd,
                    ).await {
                        if write_and_flush(&mut ch, &cmd).await.is_ok() {
                            let v = visitor.take().unwrap();
                            tokio::spawn(async move {
                                if let Err(e) = forward_bidirectional_with_idle_timeout(
                                    ch,
                                    v,
                                    post_half_close_idle_timeout,
                                )
                                .await
                                {
                                    log_forwarder_outcome("tcp", &e);
                                }
                            });
                            break;
                        }

                        // Current data channel is broken. Request for a new one.
                        if data_ch_req_tx.send(true).is_err() {
                            if let Some(mut v) = visitor.take() {
                                let _ = AsyncWriteExt::shutdown(&mut v).await;
                            }
                            return Ok(());
                        }
                    } else {
                        if let Some(mut v) = visitor.take() {
                            let _ = AsyncWriteExt::shutdown(&mut v).await;
                        }
                        return Ok(());
                    }
                }
            },
            _ = heartbeat_interval.tick() => {
                refresh_tcp_pool(&mut pool, &data_ch_req_tx, &heartbeat_cmd).await;
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

    let cmd = bincode::serialize(&DataChannelCmd::StartForwardUdp).unwrap();

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
    use tokio::io::duplex;

    #[tokio::test]
    async fn tcp_pool_heartbeat_accepts_ack() -> Result<()> {
        let (server_side, mut client_side) = duplex(64);
        let heartbeat_cmd = bincode::serialize(&DataChannelCmd::HeartBeat).unwrap();

        let client = tokio::spawn(async move {
            assert!(matches!(
                crate::protocol::read_data_cmd(&mut client_side).await.unwrap(),
                DataChannelCmd::HeartBeat
            ));
            write_and_flush(
                &mut client_side,
                &bincode::serialize(&Ack::Ok).unwrap(),
            )
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

        let heartbeat_cmd = bincode::serialize(&DataChannelCmd::HeartBeat).unwrap();
        let mut selected = next_tcp_pool_channel(
            &mut pool,
            &mut data_ch_rx,
            &data_ch_req_tx,
            &heartbeat_cmd,
        )
        .await
        .expect("fresh data channel should replace stale pool entry");

        assert!(data_ch_req_rx.try_recv().is_ok());

        write_and_flush(
            &mut selected,
            &bincode::serialize(&DataChannelCmd::StartForwardTcp).unwrap(),
        )
        .await?;
        assert!(matches!(
            crate::protocol::read_data_cmd(&mut fresh_client_side).await?,
            DataChannelCmd::StartForwardTcp
        ));
        Ok(())
    }
}
