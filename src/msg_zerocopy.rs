use std::io;
use std::sync::Once;
use std::task::{Context, Poll};
use tokio::net::TcpStream;
use tracing::{debug, warn};

#[cfg(target_os = "linux")]
pub(crate) use linux::MsgZeroCopyTx;

#[cfg(not(target_os = "linux"))]
pub(crate) use fallback::MsgZeroCopyTx;

fn warn_msg_zerocopy_fallback(e: &io::Error) {
    static WARNED: Once = Once::new();
    WARNED.call_once(|| {
        warn!(
            "MSG_ZEROCOPY is unavailable; falling back to regular TCP writes: {}",
            e
        );
    });
    debug!("MSG_ZEROCOPY unavailable for this connection: {}", e);
}

#[cfg(not(target_os = "linux"))]
mod fallback {
    use super::*;

    pub(crate) struct MsgZeroCopyTx;

    impl MsgZeroCopyTx {
        pub(crate) fn new(_stream: &TcpStream, enabled: bool) -> Option<Self> {
            if enabled {
                warn_msg_zerocopy_fallback(&io::Error::new(
                    io::ErrorKind::Unsupported,
                    "MSG_ZEROCOPY is only supported on Linux",
                ));
            }
            None
        }

        pub(crate) fn poll_write(
            &mut self,
            _stream: &TcpStream,
            _cx: &mut Context<'_>,
            _buf: &[u8],
        ) -> Poll<io::Result<usize>> {
            Poll::Ready(Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "MSG_ZEROCOPY is only supported on Linux",
            )))
        }

        pub(crate) fn poll_flush(&mut self, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Ready(Ok(()))
        }
    }
}

#[cfg(target_os = "linux")]
mod linux {
    use super::*;
    use std::collections::VecDeque;
    use std::mem;
    use std::os::fd::{AsRawFd, FromRawFd, OwnedFd, RawFd};
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::{Arc, Mutex};
    use std::task::ready;
    use std::thread;
    use std::time::{Duration, Instant};
    use tokio::io::Interest;

    const MAX_ZEROCOPY_WRITE_LEN: usize = 256 * 1024;
    const MAX_PENDING_ZEROCOPY_BYTES: usize = 16 * 1024 * 1024;
    const ERRQUEUE_POLL_TIMEOUT_MS: libc::c_int = 100;
    const SHUTDOWN_DRAIN_TIMEOUT: Duration = Duration::from_secs(30);
    const SO_EE_ORIGIN_ZEROCOPY: u8 = 5;
    const SO_EE_CODE_ZEROCOPY_COPIED: u8 = 1;

    const ZEROCOPY_SEND_FLAGS: libc::c_int =
        libc::MSG_ZEROCOPY | libc::MSG_DONTWAIT | libc::MSG_NOSIGNAL;
    const COPY_SEND_FLAGS: libc::c_int = libc::MSG_DONTWAIT | libc::MSG_NOSIGNAL;

    pub(crate) struct MsgZeroCopyTx {
        fd: RawFd,
        next_id: u32,
        shared: Arc<SharedTxState>,
        drainer: Option<ErrqueueDrainer>,
        fallback: bool,
    }

    impl MsgZeroCopyTx {
        pub(crate) fn new(stream: &TcpStream, enabled: bool) -> Option<Self> {
            if !enabled {
                return None;
            }

            match Self::start(stream) {
                Ok(tx) => {
                    debug!("Using MSG_ZEROCOPY TCP send path");
                    Some(tx)
                }
                Err(e) => {
                    warn_msg_zerocopy_fallback(&e);
                    None
                }
            }
        }

        fn start(stream: &TcpStream) -> io::Result<Self> {
            set_sock_zerocopy(stream.as_raw_fd())?;

            let shared = Arc::new(SharedTxState::default());
            Ok(Self {
                fd: stream.as_raw_fd(),
                next_id: 0,
                shared,
                drainer: None,
                fallback: false,
            })
        }

        pub(crate) fn poll_write(
            &mut self,
            stream: &TcpStream,
            cx: &mut Context<'_>,
            buf: &[u8],
        ) -> Poll<io::Result<usize>> {
            if buf.is_empty() {
                return Poll::Ready(Ok(0));
            }

            if self.shared.pending_bytes()? >= MAX_PENDING_ZEROCOPY_BYTES {
                return self.poll_copy_write(stream, cx, buf);
            }
            if self.fallback {
                return self.poll_copy_write(stream, cx, buf);
            }
            if let Err(e) = self.ensure_drainer() {
                warn_msg_zerocopy_fallback(&e);
                self.fallback = true;
                return self.poll_copy_write(stream, cx, buf);
            }

            ready!(stream.poll_write_ready(cx))?;

            let len = buf.len().min(MAX_ZEROCOPY_WRITE_LEN);
            let id = self.next_id;
            let pending = PendingSend::new(id, &buf[..len]);
            let ptr = pending.as_ptr();
            let pending_len = pending.len();
            self.shared.insert(pending)?;

            let send = stream.try_io(Interest::WRITABLE, || {
                match send_raw(self.fd, ptr, pending_len, ZEROCOPY_SEND_FLAGS) {
                    Ok(n) => Ok(SendOutcome::Zerocopy(n)),
                    Err(e) if is_copy_fallback_error(&e) => {
                        send_raw(self.fd, buf.as_ptr(), len, COPY_SEND_FLAGS)
                            .map(SendOutcome::Copied)
                    }
                    Err(e) => Err(e),
                }
            });

            match send {
                Ok(SendOutcome::Zerocopy(0)) => {
                    let _ = self.shared.remove(id);
                    Poll::Ready(Ok(0))
                }
                Ok(SendOutcome::Zerocopy(n)) => {
                    self.next_id = self.next_id.wrapping_add(1);
                    self.shared.adjust_after_send(id, n, pending_len)?;
                    Poll::Ready(Ok(n))
                }
                Ok(SendOutcome::Copied(n)) => {
                    let _ = self.shared.remove(id);
                    Poll::Ready(Ok(n))
                }
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
                    let _ = self.shared.remove(id);
                    Poll::Pending
                }
                Err(e) => {
                    let _ = self.shared.remove(id);
                    Poll::Ready(Err(e))
                }
            }
        }

        fn poll_copy_write(
            &self,
            stream: &TcpStream,
            cx: &mut Context<'_>,
            buf: &[u8],
        ) -> Poll<io::Result<usize>> {
            ready!(stream.poll_write_ready(cx))?;
            let len = buf.len().min(MAX_ZEROCOPY_WRITE_LEN);
            match stream.try_io(Interest::WRITABLE, || {
                send_raw(self.fd, buf.as_ptr(), len, COPY_SEND_FLAGS)
            }) {
                Ok(n) => Poll::Ready(Ok(n)),
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => Poll::Pending,
                Err(e) => Poll::Ready(Err(e)),
            }
        }

        pub(crate) fn poll_flush(&mut self, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Ready(Ok(()))
        }

        fn ensure_drainer(&mut self) -> io::Result<()> {
            if self.drainer.is_none() {
                self.drainer = Some(ErrqueueDrainer::start(self.fd, self.shared.clone())?);
            }
            Ok(())
        }
    }

    struct ErrqueueDrainer {
        stop: Arc<AtomicBool>,
    }

    impl ErrqueueDrainer {
        fn start(fd: RawFd, shared: Arc<SharedTxState>) -> io::Result<Self> {
            let errqueue_fd = duplicate_fd(fd)?;
            let stop = Arc::new(AtomicBool::new(false));
            let worker_stop = stop.clone();
            thread::Builder::new()
                .name("rathole-msgzc".to_string())
                .spawn(move || drain_errqueue_worker(errqueue_fd, shared, worker_stop))?;
            Ok(Self { stop })
        }
    }

    impl Drop for ErrqueueDrainer {
        fn drop(&mut self) {
            self.stop.store(true, Ordering::Release);
        }
    }

    enum SendOutcome {
        Zerocopy(usize),
        Copied(usize),
    }

    struct PendingSend {
        id: u32,
        data: Box<[u8]>,
    }

    impl PendingSend {
        fn new(id: u32, data: &[u8]) -> Self {
            Self {
                id,
                data: data.to_vec().into_boxed_slice(),
            }
        }

        fn as_ptr(&self) -> *const u8 {
            self.data.as_ptr()
        }

        fn len(&self) -> usize {
            self.data.len()
        }

        fn truncate(&mut self, len: usize) -> usize {
            if len >= self.data.len() {
                return 0;
            }

            let mut data = Vec::from(mem::take(&mut self.data));
            let removed = data.len() - len;
            data.truncate(len);
            self.data = data.into_boxed_slice();
            removed
        }
    }

    #[derive(Default)]
    struct SharedTxState {
        pending: Mutex<PendingState>,
    }

    #[derive(Default)]
    struct PendingState {
        sends: VecDeque<PendingSend>,
        bytes: usize,
    }

    impl SharedTxState {
        fn pending_bytes(&self) -> io::Result<usize> {
            Ok(self.lock()?.bytes)
        }

        fn insert(&self, send: PendingSend) -> io::Result<()> {
            let mut state = self.lock()?;
            state.bytes = state.bytes.saturating_add(send.len());
            state.sends.push_back(send);
            Ok(())
        }

        fn remove(&self, id: u32) -> io::Result<bool> {
            let mut state = self.lock()?;
            let Some(index) = state.sends.iter().position(|send| send.id == id) else {
                return Ok(false);
            };
            let send = state.sends.remove(index).expect("pending send disappeared");
            state.bytes = state.bytes.saturating_sub(send.len());
            Ok(true)
        }

        fn adjust_after_send(&self, id: u32, sent: usize, original_len: usize) -> io::Result<()> {
            let mut state = self.lock()?;
            let Some(send) = state.sends.iter_mut().find(|send| send.id == id) else {
                return Ok(());
            };
            let removed = send.truncate(sent);
            state.bytes = state.bytes.saturating_sub(removed);
            debug_assert!(sent <= original_len);
            Ok(())
        }

        fn complete_range(&self, start: u32, end: u32) -> io::Result<usize> {
            let mut state = self.lock()?;
            let mut released = 0usize;
            let mut index = 0usize;
            while index < state.sends.len() {
                if id_in_range(state.sends[index].id, start, end) {
                    let send = state.sends.remove(index).expect("pending send disappeared");
                    state.bytes = state.bytes.saturating_sub(send.len());
                    released += 1;
                } else {
                    index += 1;
                }
            }
            Ok(released)
        }

        fn leak_pending(&self) -> io::Result<usize> {
            let mut state = self.lock()?;
            let bytes = state.bytes;
            while let Some(send) = state.sends.pop_front() {
                mem::forget(send);
            }
            state.bytes = 0;
            Ok(bytes)
        }

        fn lock(&self) -> io::Result<std::sync::MutexGuard<'_, PendingState>> {
            self.pending
                .lock()
                .map_err(|_| io::Error::new(io::ErrorKind::Other, "MSG_ZEROCOPY state poisoned"))
        }
    }

    fn drain_errqueue_worker(fd: OwnedFd, shared: Arc<SharedTxState>, stop: Arc<AtomicBool>) {
        let mut shutdown_started = None;

        loop {
            drain_errqueue(fd.as_raw_fd(), &shared);

            let pending = match shared.pending_bytes() {
                Ok(bytes) => bytes,
                Err(e) => {
                    debug!("MSG_ZEROCOPY pending state unavailable: {}", e);
                    break;
                }
            };
            let stopping = stop.load(Ordering::Acquire);
            if stopping && pending == 0 {
                break;
            }

            if stopping {
                let started = *shutdown_started.get_or_insert_with(Instant::now);
                if started.elapsed() >= SHUTDOWN_DRAIN_TIMEOUT {
                    match shared.leak_pending() {
                        Ok(bytes) if bytes > 0 => warn!(
                            "MSG_ZEROCOPY completions did not drain before shutdown; leaking {} bytes of send buffers to keep in-flight data immutable",
                            bytes
                        ),
                        Ok(_) => {}
                        Err(e) => debug!("MSG_ZEROCOPY pending state unavailable: {}", e),
                    }
                    break;
                }
            }

            let mut pfd = libc::pollfd {
                fd: fd.as_raw_fd(),
                events: libc::POLLERR,
                revents: 0,
            };
            let ret = unsafe { libc::poll(&mut pfd, 1, ERRQUEUE_POLL_TIMEOUT_MS) };
            if ret < 0 {
                let e = io::Error::last_os_error();
                if e.kind() != io::ErrorKind::Interrupted {
                    debug!("MSG_ZEROCOPY errqueue poll failed: {}", e);
                    if stopping {
                        break;
                    }
                }
            }
        }
    }

    fn drain_errqueue(fd: RawFd, shared: &SharedTxState) {
        loop {
            match recv_zerocopy_completion(fd) {
                Ok(Some(completion)) => {
                    if let Err(e) = shared.complete_range(completion.start, completion.end) {
                        debug!("MSG_ZEROCOPY completion release failed: {}", e);
                        return;
                    }
                    if completion.copied {
                        debug!("MSG_ZEROCOPY completion reported a deferred copy");
                    }
                }
                Ok(None) => return,
                Err(e) => {
                    debug!("MSG_ZEROCOPY errqueue recv failed: {}", e);
                    return;
                }
            }
        }
    }

    struct Completion {
        start: u32,
        end: u32,
        copied: bool,
    }

    fn recv_zerocopy_completion(fd: RawFd) -> io::Result<Option<Completion>> {
        let mut data = [0u8; 1];
        let mut iov = libc::iovec {
            iov_base: data.as_mut_ptr().cast(),
            iov_len: data.len(),
        };
        let mut control = [0u8; 128];
        let mut msg: libc::msghdr = unsafe { mem::zeroed() };
        msg.msg_iov = &mut iov;
        msg.msg_iovlen = 1;
        msg.msg_control = control.as_mut_ptr().cast();
        msg.msg_controllen = control.len() as _;

        let ret = loop {
            let ret =
                unsafe { libc::recvmsg(fd, &mut msg, libc::MSG_ERRQUEUE | libc::MSG_DONTWAIT) };
            if ret >= 0 {
                break ret;
            }

            let e = io::Error::last_os_error();
            if e.kind() == io::ErrorKind::Interrupted {
                continue;
            }
            if e.kind() == io::ErrorKind::WouldBlock {
                return Ok(None);
            }
            return Err(e);
        };

        if ret < 0 {
            return Err(io::Error::last_os_error());
        }
        if msg.msg_flags & libc::MSG_CTRUNC != 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "MSG_ZEROCOPY completion control message was truncated",
            ));
        }

        let mut cmsg = unsafe { libc::CMSG_FIRSTHDR(&msg) };
        while !cmsg.is_null() {
            let header = unsafe { &*cmsg };
            if is_recv_error_cmsg(header) {
                let data_len = (header.cmsg_len as usize)
                    .saturating_sub(unsafe { libc::CMSG_LEN(0) } as usize);
                if data_len < mem::size_of::<libc::sock_extended_err>() {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "MSG_ZEROCOPY completion is too short",
                    ));
                }

                let serr = unsafe { &*(libc::CMSG_DATA(cmsg).cast::<libc::sock_extended_err>()) };
                if serr.ee_errno == 0 && serr.ee_origin == SO_EE_ORIGIN_ZEROCOPY {
                    return Ok(Some(Completion {
                        start: serr.ee_info,
                        end: serr.ee_data,
                        copied: serr.ee_code & SO_EE_CODE_ZEROCOPY_COPIED != 0,
                    }));
                }
            }
            cmsg = unsafe { libc::CMSG_NXTHDR(&msg, cmsg) };
        }

        Ok(None)
    }

    fn is_recv_error_cmsg(header: &libc::cmsghdr) -> bool {
        (header.cmsg_level == libc::SOL_IP && header.cmsg_type == libc::IP_RECVERR)
            || (header.cmsg_level == libc::SOL_IPV6 && header.cmsg_type == libc::IPV6_RECVERR)
    }

    fn set_sock_zerocopy(fd: RawFd) -> io::Result<()> {
        let one: libc::c_int = 1;
        cvt(unsafe {
            libc::setsockopt(
                fd,
                libc::SOL_SOCKET,
                libc::SO_ZEROCOPY,
                (&one as *const libc::c_int).cast(),
                mem::size_of_val(&one) as libc::socklen_t,
            )
        })?;
        Ok(())
    }

    fn send_raw(fd: RawFd, ptr: *const u8, len: usize, flags: libc::c_int) -> io::Result<usize> {
        loop {
            let ret = unsafe { libc::send(fd, ptr.cast(), len, flags) };
            if ret >= 0 {
                return Ok(ret as usize);
            }

            let e = io::Error::last_os_error();
            if e.kind() != io::ErrorKind::Interrupted {
                return Err(e);
            }
        }
    }

    fn duplicate_fd(fd: RawFd) -> io::Result<OwnedFd> {
        let fd = unsafe { libc::dup(fd) };
        if fd < 0 {
            Err(io::Error::last_os_error())
        } else {
            Ok(unsafe { OwnedFd::from_raw_fd(fd) })
        }
    }

    fn cvt(r: libc::c_int) -> io::Result<libc::c_int> {
        if r == -1 {
            Err(io::Error::last_os_error())
        } else {
            Ok(r)
        }
    }

    fn is_copy_fallback_error(e: &io::Error) -> bool {
        e.raw_os_error() == Some(libc::ENOBUFS)
    }

    fn id_in_range(id: u32, start: u32, end: u32) -> bool {
        if start <= end {
            id >= start && id <= end
        } else {
            id >= start || id <= end
        }
    }
}
