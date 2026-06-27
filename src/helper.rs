use anyhow::{Context, Result, anyhow, bail};
use async_http_proxy::{http_connect_tokio, http_connect_tokio_with_basic_auth};
use backoff::{backoff::Backoff, Notify};
#[cfg(target_os = "linux")]
use socket2::{Domain, Protocol, SockAddr, Socket, Type};
use socket2::{SockRef, TcpKeepalive};
#[cfg(not(target_os = "linux"))]
use std::sync::Once;
use std::{future::Future, net::SocketAddr, time::Duration};
use tokio::io::{AsyncWrite, AsyncWriteExt};
use tokio::{
    net::{lookup_host, TcpListener, TcpStream, ToSocketAddrs, UdpSocket},
    sync::broadcast,
};
use tracing::{trace, warn};
use url::Url;

use crate::transport::AddrMaybeCached;

#[cfg(target_os = "linux")]
const TCP_LISTEN_BACKLOG: i32 = 1024;
#[cfg(target_os = "linux")]
const TCP_FASTOPEN_QUEUE_LEN: i32 = 1024;

// Tokio hesitates to expose this option...So we have to do it on our own :(
// The good news is that using socket2 it can be easily done, without losing portability.
// See https://github.com/tokio-rs/tokio/issues/3082
pub fn try_set_tcp_keepalive(
    conn: &TcpStream,
    keepalive_duration: Duration,
    keepalive_interval: Duration,
) -> Result<()> {
    let s = SockRef::from(conn);
    let keepalive = TcpKeepalive::new().with_time(keepalive_duration);
    let keepalive = tcp_keepalive_with_interval(keepalive, keepalive_interval);

    trace!(
        "Set TCP keepalive {:?} {:?}",
        keepalive_duration,
        keepalive_interval
    );

    Ok(s.set_tcp_keepalive(&keepalive)?)
}

#[cfg(any(
    target_os = "android",
    target_os = "dragonfly",
    target_os = "freebsd",
    target_os = "fuchsia",
    target_os = "illumos",
    target_os = "linux",
    target_os = "netbsd",
    target_vendor = "apple",
    windows,
))]
fn tcp_keepalive_with_interval(
    keepalive: TcpKeepalive,
    keepalive_interval: Duration,
) -> TcpKeepalive {
    keepalive.with_interval(keepalive_interval)
}

#[cfg(not(any(
    target_os = "android",
    target_os = "dragonfly",
    target_os = "freebsd",
    target_os = "fuchsia",
    target_os = "illumos",
    target_os = "linux",
    target_os = "netbsd",
    target_vendor = "apple",
    windows,
)))]
fn tcp_keepalive_with_interval(
    keepalive: TcpKeepalive,
    _keepalive_interval: Duration,
) -> TcpKeepalive {
    keepalive
}

#[allow(dead_code)]
pub fn feature_not_compile(feature: &str) -> ! {
    panic!(
        "The feature '{}' is not compiled in this binary. Please re-compile rathole",
        feature
    )
}

#[allow(dead_code)]
pub fn feature_neither_compile(feature1: &str, feature2: &str) -> ! {
    panic!(
        "Neither of the feature '{}' or '{}' is compiled in this binary. Please re-compile rathole",
        feature1, feature2
    )
}

pub async fn to_socket_addr<A: ToSocketAddrs>(addr: A) -> Result<SocketAddr> {
    lookup_host(addr)
        .await?
        .next()
        .ok_or_else(|| anyhow!("Failed to lookup the host"))
}

/// Split `domain:port`. For IP literals use [`host_port_pair`].
pub fn parse_domain_host_port(addr: &str) -> Result<(&str, u16)> {
    let semi = addr
        .rfind(':')
        .ok_or_else(|| anyhow!("missing port in address {addr}"))?;
    let host = &addr[..semi];
    let port: u16 = addr[semi + 1..]
        .parse()
        .with_context(|| format!("invalid port in address {addr}"))?;

    if host.is_empty() {
        return Err(anyhow!("empty host in address {addr}"));
    }
    if host.contains(':') {
        return Err(anyhow!(
            "invalid address {addr}: use bracket form for IPv6, e.g. [::1]:2333"
        ));
    }
    Ok((host, port))
}

pub fn host_port_pair(s: &str) -> Result<(&str, u16)> {
    if let Ok(sa) = s.parse::<SocketAddr>() {
        if let Some(rest) = s.strip_prefix('[') {
            let (inner, after) = rest
                .split_once(']')
                .ok_or_else(|| anyhow!("missing ] in address {s}"))?;
            if !after.starts_with(':') {
                bail!("missing port after IPv6 address in {s}");
            }
            return Ok((inner, sa.port()));
        }
        let semi = s
            .rfind(':')
            .ok_or_else(|| anyhow!("missing port in address {s}"))?;
        return Ok((&s[..semi], sa.port()));
    }
    parse_domain_host_port(s)
}

/// Create a UDP socket and connect to `addr`
pub async fn udp_connect<A: ToSocketAddrs>(addr: A, prefer_ipv6: bool) -> Result<UdpSocket> {
    let (socket_addr, bind_addr);

    match prefer_ipv6 {
        false => {
            socket_addr = to_socket_addr(addr).await?;

            bind_addr = match socket_addr {
                SocketAddr::V4(_) => "0.0.0.0:0",
                SocketAddr::V6(_) => ":::0",
            };
        }
        true => {
            let all_host_addresses: Vec<SocketAddr> = lookup_host(addr).await?.collect();

            // Try to find an IPv6 address
            match all_host_addresses.clone().iter().find(|x| x.is_ipv6()) {
                Some(socket_addr_ipv6) => {
                    socket_addr = *socket_addr_ipv6;
                    bind_addr = ":::0";
                }
                None => {
                    let socket_addr_ipv4 = all_host_addresses.iter().find(|x| x.is_ipv4());
                    match socket_addr_ipv4 {
                        None => return Err(anyhow!("Failed to lookup the host")),
                        // fallback to IPv4
                        Some(socket_addr_ipv4) => {
                            socket_addr = *socket_addr_ipv4;
                            bind_addr = "0.0.0.0:0";
                        }
                    }
                }
            }
        }
    };
    let s = UdpSocket::bind(bind_addr).await?;
    s.connect(socket_addr).await?;
    s.connect(socket_addr).await?;
    Ok(s)
}

pub async fn tcp_bind<A: ToSocketAddrs>(addr: A, fast_open: bool) -> Result<TcpListener> {
    if !fast_open {
        return Ok(TcpListener::bind(addr).await?);
    }

    #[cfg(not(target_os = "linux"))]
    {
        warn_tcp_fast_open_unsupported();
        Ok(TcpListener::bind(addr).await?)
    }

    #[cfg(target_os = "linux")]
    {
        let addr = to_socket_addr(addr).await?;
        tcp_bind_socket(addr)
    }
}

#[cfg(target_os = "linux")]
fn tcp_bind_socket(addr: SocketAddr) -> Result<TcpListener> {
    let socket = tcp_socket(addr)?;
    socket.set_reuse_address(true)?;
    try_set_tcp_fast_open_listener(&socket);

    socket.bind(&SockAddr::from(addr))?;
    socket.listen(TCP_LISTEN_BACKLOG)?;
    socket.set_nonblocking(true)?;

    Ok(TcpListener::from_std(socket.into())?)
}

async fn tcp_connect<A: ToSocketAddrs>(addr: A, fast_open: bool) -> Result<TcpStream> {
    if !fast_open {
        return Ok(TcpStream::connect(addr).await?);
    }

    #[cfg(not(target_os = "linux"))]
    {
        warn_tcp_fast_open_unsupported();
        Ok(TcpStream::connect(addr).await?)
    }

    #[cfg(target_os = "linux")]
    {
        let addr = to_socket_addr(addr).await?;
        tcp_connect_socket(addr).await
    }
}

#[cfg(target_os = "linux")]
async fn tcp_connect_socket(addr: SocketAddr) -> Result<TcpStream> {
    let socket = tcp_socket(addr)?;
    try_set_tcp_fast_open_connect(&socket);

    socket.set_nonblocking(true)?;
    match socket.connect(&SockAddr::from(addr)) {
        Ok(()) => Ok(TcpStream::from_std(socket.into())?),
        Err(e) if is_connect_in_progress(&e) => {
            let stream = TcpStream::from_std(socket.into())?;
            stream.writable().await?;
            if let Some(e) = stream.take_error()? {
                return Err(e.into());
            }
            Ok(stream)
        }
        Err(e) => Err(e.into()),
    }
}

#[cfg(target_os = "linux")]
fn tcp_socket(addr: SocketAddr) -> Result<Socket> {
    let domain = match addr {
        SocketAddr::V4(_) => Domain::IPV4,
        SocketAddr::V6(_) => Domain::IPV6,
    };
    Ok(Socket::new(domain, Type::STREAM, Some(Protocol::TCP))?)
}

#[cfg(target_os = "linux")]
fn is_connect_in_progress(e: &std::io::Error) -> bool {
    if e.kind() == std::io::ErrorKind::WouldBlock {
        return true;
    }

    e.raw_os_error() == Some(libc::EINPROGRESS)
}

#[cfg(target_os = "linux")]
fn try_set_tcp_fast_open_listener(socket: &Socket) {
    if let Err(e) = set_tcp_fast_open(socket, libc::TCP_FASTOPEN, TCP_FASTOPEN_QUEUE_LEN) {
        warn!("Failed to enable TCP Fast Open on listener: {}", e);
    }
}

#[cfg(target_os = "linux")]
fn try_set_tcp_fast_open_connect(socket: &Socket) {
    if let Err(e) = set_tcp_fast_open(socket, libc::TCP_FASTOPEN_CONNECT, 1) {
        warn!("Failed to enable TCP Fast Open on connector: {}", e);
    }
}

#[cfg(target_os = "linux")]
fn set_tcp_fast_open(socket: &Socket, opt: libc::c_int, value: libc::c_int) -> std::io::Result<()> {
    use std::os::unix::io::AsRawFd;

    let ret = unsafe {
        libc::setsockopt(
            socket.as_raw_fd(),
            libc::IPPROTO_TCP,
            opt,
            &value as *const _ as *const libc::c_void,
            std::mem::size_of_val(&value) as libc::socklen_t,
        )
    };
    if ret == -1 {
        Err(std::io::Error::last_os_error())
    } else {
        Ok(())
    }
}

#[cfg(not(target_os = "linux"))]
fn warn_tcp_fast_open_unsupported() {
    static WARN_ONCE: Once = Once::new();
    WARN_ONCE.call_once(|| {
        warn!("TCP Fast Open is not supported by this build; using regular TCP");
    });
}

/// Create a TcpStream using a proxy
/// e.g. socks5://user:pass@127.0.0.1:1080 http://127.0.0.1:8080
pub async fn tcp_connect_with_proxy(
    addr: &AddrMaybeCached,
    proxy: Option<&Url>,
    fast_open: bool,
) -> Result<TcpStream> {
    if let Some(url) = proxy {
        let addr = &addr.addr;
        let mut s = tcp_connect(
            (
                url.host_str().expect("proxy url should have host field"),
                url.port().expect("proxy url should have port field"),
            ),
            fast_open,
        )
        .await?;
        if fast_open {
            trace!("TCP Fast Open is applied to the proxy connection");
        }

        let auth = if !url.username().is_empty() || url.password().is_some() {
            Some(async_socks5::Auth {
                username: url.username().into(),
                password: url.password().unwrap_or("").into(),
            })
        } else {
            None
        };
        match url.scheme() {
            "socks5" => {
                async_socks5::connect(&mut s, host_port_pair(addr)?, auth).await?;
            }
            "http" => {
                let (host, port) = host_port_pair(addr)?;
                match auth {
                    Some(auth) => {
                        http_connect_tokio_with_basic_auth(
                            &mut s,
                            host,
                            port,
                            &auth.username,
                            &auth.password,
                        )
                        .await?
                    }
                    None => http_connect_tokio(&mut s, host, port).await?,
                }
            }
            _ => panic!("unknown proxy scheme"),
        }
        Ok(s)
    } else {
        Ok(match addr.socket_addr {
            Some(s) => tcp_connect(s, fast_open).await?,
            None => tcp_connect(addr.addr.as_str(), fast_open).await?,
        })
    }
}

// Wrapper of retry_notify
pub async fn retry_notify_with_deadline<I, E, Fn, Fut, B, N>(
    backoff: B,
    operation: Fn,
    notify: N,
    deadline: &mut broadcast::Receiver<bool>,
) -> Result<I>
where
    E: std::error::Error + Send + Sync + 'static,
    B: Backoff,
    Fn: FnMut() -> Fut,
    Fut: Future<Output = std::result::Result<I, backoff::Error<E>>>,
    N: Notify<E>,
{
    tokio::select! {
        v = backoff::future::retry_notify(backoff, operation, notify) => {
            v.map_err(anyhow::Error::new)
        }
        _ = deadline.recv() => {
            Err(anyhow!("shutdown"))
        }
    }
}

pub async fn write_and_flush<T>(conn: &mut T, data: &[u8]) -> Result<()>
where
    T: AsyncWrite + Unpin,
{
    conn.write_all(data)
        .await
        .with_context(|| "Failed to write data")?;
    conn.flush().await.with_context(|| "Failed to flush data")?;
    Ok(())
}

#[cfg(all(test, target_os = "linux"))]
mod tests {
    use super::*;
    use std::os::unix::io::{AsRawFd, RawFd};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    fn getsockopt_int(fd: RawFd, opt: libc::c_int) -> std::io::Result<libc::c_int> {
        let mut value = 0;
        let mut len = std::mem::size_of_val(&value) as libc::socklen_t;
        let ret = unsafe {
            libc::getsockopt(
                fd,
                libc::IPPROTO_TCP,
                opt,
                &mut value as *mut _ as *mut libc::c_void,
                &mut len,
            )
        };
        if ret == -1 {
            Err(std::io::Error::last_os_error())
        } else {
            Ok(value)
        }
    }

    #[tokio::test]
    async fn tcp_fast_open_listener_option_is_enabled() -> Result<()> {
        let listener = tcp_bind("127.0.0.1:0", true).await?;
        let value = getsockopt_int(listener.as_raw_fd(), libc::TCP_FASTOPEN)?;
        assert!(value > 0, "TCP_FASTOPEN queue length should be enabled");
        Ok(())
    }

    #[tokio::test]
    async fn tcp_fast_open_connect_option_is_enabled() -> Result<()> {
        let listener = tcp_bind("127.0.0.1:0", true).await?;
        let addr = listener.local_addr()?;
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await?;
            let mut byte = [0u8; 1];
            stream.read_exact(&mut byte).await?;
            Result::<()>::Ok(())
        });

        let mut stream = tcp_connect(addr, true).await?;
        let value = getsockopt_int(stream.as_raw_fd(), libc::TCP_FASTOPEN_CONNECT)?;
        assert_eq!(value, 1, "TCP_FASTOPEN_CONNECT should be enabled");
        stream.write_all(b"x").await?;
        server.await??;
        Ok(())
    }
}

#[cfg(all(test, not(target_os = "linux")))]
mod tests {
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    #[tokio::test]
    async fn tcp_fast_open_falls_back_to_regular_tcp() -> Result<()> {
        let listener = tcp_bind("127.0.0.1:0", true).await?;
        let addr = listener.local_addr()?;
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await?;
            let mut byte = [0u8; 1];
            stream.read_exact(&mut byte).await?;
            stream.write_all(&byte).await?;
            Result::<()>::Ok(())
        });

        let mut stream = tcp_connect(addr, true).await?;
        stream.write_all(b"x").await?;
        let mut byte = [0u8; 1];
        stream.read_exact(&mut byte).await?;
        assert_eq!(byte, *b"x");
        server.await??;
        Ok(())
    }
}
