use crate::{
    config::{IoUringZcRxConfig, TcpConfig, TransportConfig},
    helper::{tcp_bind, tcp_connect_with_proxy},
    io_uring_zc_rx::MaybeZcRxTcpStream,
};

use super::{AddrMaybeCached, SocketOpts, Transport};
use anyhow::Result;
use std::future::Future;
use std::io;
use std::net::SocketAddr;
use std::time::Duration;
use tokio::net::{TcpListener, TcpStream, ToSocketAddrs};
#[cfg(target_os = "linux")]
use tracing::debug;

#[derive(Debug)]
pub struct TcpTransport {
    socket_opts: SocketOpts,
    cfg: TcpConfig,
    zc_rx: IoUringZcRxConfig,
}

impl Transport for TcpTransport {
    type Acceptor = TcpListener;
    type Stream = MaybeZcRxTcpStream;
    type RawStream = MaybeZcRxTcpStream;

    fn new(config: &TransportConfig) -> Result<Self> {
        Ok(TcpTransport {
            socket_opts: SocketOpts::from_cfg(&config.tcp),
            cfg: config.tcp.clone(),
            zc_rx: config.io_uring_zc_rx.clone(),
        })
    }

    fn hint(conn: &Self::Stream, opt: SocketOpts) {
        opt.apply(conn.tcp_stream());
    }

    async fn bind<T: ToSocketAddrs + Send + Sync>(&self, addr: T) -> Result<Self::Acceptor> {
        tcp_bind(addr, self.cfg.fast_open).await
    }

    async fn accept(&self, a: &Self::Acceptor) -> Result<(Self::RawStream, SocketAddr)> {
        let (s, addr) = a.accept().await?;
        self.socket_opts.apply(&s);
        Ok((MaybeZcRxTcpStream::new(s, &self.zc_rx), addr))
    }

    async fn handshake(&self, conn: Self::RawStream) -> Result<Self::Stream> {
        Ok(conn)
    }

    async fn connect(&self, addr: &AddrMaybeCached) -> Result<Self::Stream> {
        let s = tcp_connect_with_proxy(addr, self.cfg.proxy.as_ref(), self.cfg.fast_open).await?;
        self.socket_opts.apply(&s);
        Ok(MaybeZcRxTcpStream::new(s, &self.zc_rx))
    }

    fn forward_tcp(
        data_channel: Self::Stream,
        peer: TcpStream,
        idle: Option<Duration>,
    ) -> impl Future<Output = io::Result<()>> + Send {
        forward_tcp(data_channel, peer, idle)
    }
}

#[cfg(target_os = "linux")]
async fn forward_tcp(
    data_channel: MaybeZcRxTcpStream,
    peer: TcpStream,
    idle: Option<Duration>,
) -> io::Result<()> {
    let config = data_channel.io_uring_zc_rx_config().clone();
    let peer = MaybeZcRxTcpStream::new(peer, &config);
    if data_channel.is_zc_rx_active() || peer.is_zc_rx_active() {
        debug!("Using io_uring ZC Rx TCP forwarding");
        return crate::forward::forward_bidirectional_with_idle_timeout(data_channel, peer, idle)
            .await;
    }

    let data_channel = into_tcp_stream(data_channel)?;
    let peer = into_tcp_stream(peer)?;
    debug!("Using splice zero-copy TCP forwarding");
    crate::forward::splice_bidirectional_with_idle_timeout(data_channel, peer, idle).await
}

#[cfg(not(target_os = "linux"))]
async fn forward_tcp(
    data_channel: MaybeZcRxTcpStream,
    peer: TcpStream,
    idle: Option<Duration>,
) -> io::Result<()> {
    let config = data_channel.io_uring_zc_rx_config().clone();
    let peer = MaybeZcRxTcpStream::new(peer, &config);
    crate::forward::forward_bidirectional_with_idle_timeout(data_channel, peer, idle).await
}

#[cfg(target_os = "linux")]
fn into_tcp_stream(stream: MaybeZcRxTcpStream) -> io::Result<TcpStream> {
    stream.try_into_tcp_stream().map_err(|_| {
        io::Error::new(
            io::ErrorKind::Other,
            "io_uring ZC Rx stream cannot be converted back to TcpStream",
        )
    })
}
