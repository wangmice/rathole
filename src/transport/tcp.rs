use crate::{
    config::{TcpConfig, TransportConfig},
    helper::{tcp_bind, tcp_connect_with_proxy},
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
}

impl Transport for TcpTransport {
    type Acceptor = TcpListener;
    type Stream = TcpStream;
    type RawStream = TcpStream;

    fn new(config: &TransportConfig) -> Result<Self> {
        Ok(TcpTransport {
            socket_opts: SocketOpts::from_cfg(&config.tcp),
            cfg: config.tcp.clone(),
        })
    }

    fn hint(conn: &Self::Stream, opt: SocketOpts) {
        opt.apply(conn);
    }

    async fn bind<T: ToSocketAddrs + Send + Sync>(&self, addr: T) -> Result<Self::Acceptor> {
        tcp_bind(addr, self.cfg.fast_open).await
    }

    async fn accept(&self, a: &Self::Acceptor) -> Result<(Self::RawStream, SocketAddr)> {
        let (s, addr) = a.accept().await?;
        self.socket_opts.apply(&s);
        Ok((s, addr))
    }

    async fn handshake(&self, conn: Self::RawStream) -> Result<Self::Stream> {
        Ok(conn)
    }

    async fn connect(&self, addr: &AddrMaybeCached) -> Result<Self::Stream> {
        let s = tcp_connect_with_proxy(addr, self.cfg.proxy.as_ref(), self.cfg.fast_open).await?;
        self.socket_opts.apply(&s);
        Ok(s)
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
    data_channel: TcpStream,
    peer: TcpStream,
    idle: Option<Duration>,
) -> io::Result<()> {
    debug!("Using splice zero-copy TCP forwarding");
    crate::forward::splice_bidirectional_with_idle_timeout(data_channel, peer, idle).await
}

#[cfg(not(target_os = "linux"))]
async fn forward_tcp(
    data_channel: TcpStream,
    peer: TcpStream,
    idle: Option<Duration>,
) -> io::Result<()> {
    crate::forward::forward_bidirectional_with_idle_timeout(data_channel, peer, idle).await
}
