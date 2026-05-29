use std::io;
use std::sync::Once;
use tokio::net::TcpStream;
use tracing::{debug, warn};

pub(crate) struct TcpQuickAck {
    active: bool,
}

impl TcpQuickAck {
    pub(crate) fn new(enabled: bool) -> Self {
        Self {
            active: sys::init(enabled),
        }
    }

    pub(crate) fn is_active(&self) -> bool {
        self.active
    }

    pub(crate) fn rearm(&mut self, stream: &TcpStream) {
        if !self.active {
            return;
        }

        if let Err(e) = sys::set_tcp_quickack(stream) {
            warn_tcp_quickack_fallback(&e);
            self.active = false;
        }
    }
}

fn warn_tcp_quickack_fallback(e: &io::Error) {
    static WARNED: Once = Once::new();
    WARNED.call_once(|| {
        warn!(
            "TCP_QUICKACK is unavailable; falling back to regular delayed ACK behavior: {}",
            e
        );
    });
    debug!("TCP_QUICKACK unavailable for this connection: {}", e);
}

#[cfg(not(target_os = "linux"))]
mod sys {
    use super::*;

    pub(super) fn init(enabled: bool) -> bool {
        if enabled {
            warn_tcp_quickack_fallback(&io::Error::new(
                io::ErrorKind::Unsupported,
                "TCP_QUICKACK is only supported on Linux",
            ));
        }
        false
    }

    pub(super) fn set_tcp_quickack(_stream: &TcpStream) -> io::Result<()> {
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "TCP_QUICKACK is only supported on Linux",
        ))
    }
}

#[cfg(target_os = "linux")]
mod sys {
    use super::*;
    use std::mem;
    use std::os::fd::AsRawFd;

    pub(super) fn init(enabled: bool) -> bool {
        enabled
    }

    pub(super) fn set_tcp_quickack(stream: &TcpStream) -> io::Result<()> {
        let one: libc::c_int = 1;
        let ret = unsafe {
            libc::setsockopt(
                stream.as_raw_fd(),
                libc::IPPROTO_TCP,
                libc::TCP_QUICKACK,
                (&one as *const libc::c_int).cast(),
                mem::size_of_val(&one) as libc::socklen_t,
            )
        };
        if ret == -1 {
            Err(io::Error::last_os_error())
        } else {
            Ok(())
        }
    }
}

#[cfg(all(test, target_os = "linux"))]
mod tests {
    use super::TcpQuickAck;
    use tokio::net::{TcpListener, TcpStream};

    #[tokio::test]
    async fn quickack_can_be_rearmed_on_tcp_socket() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let accept = tokio::spawn(async move { listener.accept().await.unwrap().0 });

        let stream = TcpStream::connect(addr).await.unwrap();
        let _server_stream = accept.await.unwrap();

        let mut quickack = TcpQuickAck::new(true);
        quickack.rearm(&stream);
        assert!(quickack.is_active());
    }
}
