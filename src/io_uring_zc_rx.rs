use crate::config::IoUringZcRxConfig;
use std::fmt::{self, Debug};
use std::io;
use std::pin::Pin;
use std::sync::Once;
use std::task::{Context, Poll};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::net::TcpStream;
use tracing::{debug, warn};

use self::sys::{start_zc_rx, ZcRxReadHalf};

pub struct MaybeZcRxTcpStream {
    rx: Option<ZcRxReadHalf>,
    inner: TcpStream,
    config: IoUringZcRxConfig,
}

impl MaybeZcRxTcpStream {
    pub(crate) fn new(inner: TcpStream, config: &IoUringZcRxConfig) -> Self {
        let rx = if config.enabled {
            match start_zc_rx(&inner, config) {
                Ok(rx) => {
                    debug!("Using io_uring ZC Rx receive path");
                    Some(rx)
                }
                Err(e) => {
                    warn_zc_rx_fallback(&e);
                    None
                }
            }
        } else {
            None
        };

        Self {
            rx,
            inner,
            config: config.clone(),
        }
    }

    pub(crate) fn tcp_stream(&self) -> &TcpStream {
        &self.inner
    }

    pub(crate) fn io_uring_zc_rx_config(&self) -> &IoUringZcRxConfig {
        &self.config
    }

    #[cfg(target_os = "linux")]
    pub(crate) fn is_zc_rx_active(&self) -> bool {
        self.rx.is_some()
    }

    #[cfg(target_os = "linux")]
    pub(crate) fn try_into_tcp_stream(self) -> Result<TcpStream, Self> {
        if self.rx.is_none() {
            Ok(self.inner)
        } else {
            Err(self)
        }
    }
}

impl Debug for MaybeZcRxTcpStream {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MaybeZcRxTcpStream")
            .field("inner", &self.inner)
            .field("zc_rx_active", &self.rx.is_some())
            .finish()
    }
}

impl AsyncRead for MaybeZcRxTcpStream {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        if let Some(rx) = self.rx.as_mut() {
            rx.poll_read(cx, buf)
        } else {
            Pin::new(&mut self.inner).poll_read(cx, buf)
        }
    }
}

impl AsyncWrite for MaybeZcRxTcpStream {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<Result<usize, io::Error>> {
        Pin::new(&mut self.inner).poll_write(cx, buf)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), io::Error>> {
        Pin::new(&mut self.inner).poll_flush(cx)
    }

    fn poll_shutdown(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), io::Error>> {
        Pin::new(&mut self.inner).poll_shutdown(cx)
    }
}

fn warn_zc_rx_fallback(e: &io::Error) {
    static WARNED: Once = Once::new();
    WARNED.call_once(|| {
        warn!(
            "io_uring ZC Rx is unavailable; falling back to regular TCP reads: {}",
            e
        );
    });
    debug!("io_uring ZC Rx unavailable for this connection: {}", e);
}

#[cfg(not(target_os = "linux"))]
mod sys {
    use crate::config::IoUringZcRxConfig;
    use std::io;
    use std::task::{Context, Poll};
    use tokio::io::ReadBuf;
    use tokio::net::TcpStream;

    pub(super) struct ZcRxReadHalf;

    pub(super) fn start_zc_rx(
        _stream: &TcpStream,
        _config: &IoUringZcRxConfig,
    ) -> io::Result<ZcRxReadHalf> {
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "io_uring ZC Rx is only supported on Linux",
        ))
    }

    impl ZcRxReadHalf {
        pub(super) fn poll_read(
            &mut self,
            _cx: &mut Context<'_>,
            _buf: &mut ReadBuf<'_>,
        ) -> Poll<io::Result<()>> {
            Poll::Ready(Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "io_uring ZC Rx is only supported on Linux",
            )))
        }
    }
}

#[cfg(target_os = "linux")]
mod sys {
    use crate::config::IoUringZcRxConfig;
    use io_uring::{cqueue, opcode, squeue, types, IoUring, Probe};
    use std::convert::TryFrom;
    use std::ffi::{CStr, CString};
    use std::io;
    use std::mem;
    use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
    use std::os::fd::{AsRawFd, FromRawFd, OwnedFd, RawFd};
    use std::pin::Pin;
    use std::ptr::{self, NonNull};
    use std::slice;
    use std::sync::Mutex;
    use std::task::{Context, Poll};
    use std::thread;
    use tokio::io::ReadBuf;
    use tokio::net::TcpStream;
    use tokio::sync::mpsc;

    const RECV_USER_DATA: u64 = 0x7261_7468_6f6c_655a;
    const ZCRX_DATA_OFFSET_MASK: u64 = (1u64 << types::IORING_ZCRX_AREA_SHIFT) - 1;

    pub(super) struct ZcRxReadHalf {
        state: Mutex<ZcRxReadState>,
        shutdown_fd: RawFd,
    }

    struct ZcRxReadState {
        chunks: mpsc::Receiver<io::Result<Vec<u8>>>,
        current: Option<Chunk>,
    }

    struct Chunk {
        data: Vec<u8>,
        pos: usize,
    }

    impl Drop for ZcRxReadHalf {
        fn drop(&mut self) {
            unsafe {
                libc::shutdown(self.shutdown_fd, libc::SHUT_RD);
            }
        }
    }

    impl ZcRxReadHalf {
        pub(super) fn poll_read(
            &mut self,
            cx: &mut Context<'_>,
            buf: &mut ReadBuf<'_>,
        ) -> Poll<io::Result<()>> {
            if buf.remaining() == 0 {
                return Poll::Ready(Ok(()));
            }

            let mut state = match self.state.lock() {
                Ok(state) => state,
                Err(_) => {
                    return Poll::Ready(Err(io::Error::new(
                        io::ErrorKind::Other,
                        "io_uring ZC Rx reader state is poisoned",
                    )));
                }
            };

            loop {
                if let Some(chunk) = state.current.as_mut() {
                    let available = chunk.data.len().saturating_sub(chunk.pos);
                    if available > 0 {
                        let n = available.min(buf.remaining());
                        buf.put_slice(&chunk.data[chunk.pos..chunk.pos + n]);
                        chunk.pos += n;
                        if chunk.pos == chunk.data.len() {
                            state.current = None;
                        }
                        return Poll::Ready(Ok(()));
                    }
                    state.current = None;
                }

                match Pin::new(&mut state.chunks).poll_recv(cx) {
                    Poll::Pending => return Poll::Pending,
                    Poll::Ready(None) => return Poll::Ready(Ok(())),
                    Poll::Ready(Some(Err(e))) => return Poll::Ready(Err(e)),
                    Poll::Ready(Some(Ok(data))) => {
                        if !data.is_empty() {
                            state.current = Some(Chunk { data, pos: 0 });
                        }
                    }
                }
            }
        }
    }

    pub(super) fn start_zc_rx(
        stream: &TcpStream,
        config: &IoUringZcRxConfig,
    ) -> io::Result<ZcRxReadHalf> {
        if config.ring_entries == 0 || !config.ring_entries.is_power_of_two() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "io_uring ZC Rx ring_entries must be a non-zero power of two",
            ));
        }
        if config.recv_len == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "io_uring ZC Rx recv_len must be non-zero",
            ));
        }

        let if_idx = resolve_if_index(stream, config)?;
        let fd = duplicate_fd(stream.as_raw_fd())?;
        let worker = ZcRxWorker::new(fd, config, if_idx)?;

        let capacity = usize::try_from(config.ring_entries.min(1024)).unwrap_or(1024);
        let (tx, chunks) = mpsc::channel(capacity);
        let _handle = thread::Builder::new()
            .name("rathole-zcrx".to_string())
            .spawn(move || worker.run(tx))?;

        Ok(ZcRxReadHalf {
            state: Mutex::new(ZcRxReadState {
                chunks,
                current: None,
            }),
            shutdown_fd: stream.as_raw_fd(),
        })
    }

    struct ZcRxWorker {
        ring: IoUring<squeue::Entry, cqueue::Entry32>,
        fd: OwnedFd,
        area: MmapRegion,
        refill_ring: RefillRing,
        _refill_memory: MmapRegion,
        area_token: u64,
        ifq_id: u32,
        recv_len: u32,
    }

    impl ZcRxWorker {
        fn new(fd: OwnedFd, config: &IoUringZcRxConfig, if_idx: u32) -> io::Result<Self> {
            let ring = IoUring::<squeue::Entry, cqueue::Entry32>::builder()
                .setup_single_issuer()
                .setup_defer_taskrun()
                .build(config.ring_entries)?;

            let mut probe = Probe::new();
            ring.submitter().register_probe(&mut probe)?;
            if !probe.is_supported(opcode::RecvZc::CODE) {
                return Err(io::Error::new(
                    io::ErrorKind::Unsupported,
                    "IORING_OP_RECV_ZC is not supported by this kernel",
                ));
            }

            let area_len = usize::try_from(config.area_size).map_err(|_| {
                io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "io_uring ZC Rx area_size does not fit in usize",
                )
            })?;
            if area_len < usize::try_from(config.recv_len).unwrap_or(usize::MAX) {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "io_uring ZC Rx area_size must be at least recv_len",
                ));
            }

            let area = MmapRegion::new(area_len)?;
            let refill_len = refill_ring_len(config.ring_entries)?;
            let refill_memory = MmapRegion::new(refill_len)?;

            let mut area_reg = types::io_uring_zcrx_area_reg {
                addr: area.as_ptr() as u64,
                len: area.len() as u64,
                ..Default::default()
            };
            let mut region_reg = types::io_uring_region_desc {
                user_addr: refill_memory.as_ptr() as u64,
                size: refill_memory.len() as u64,
                flags: types::IORING_MEM_REGION_TYPE_USER,
                ..Default::default()
            };
            let ifq_reg = types::io_uring_zcrx_ifq_reg {
                if_idx,
                if_rxq: config.rx_queue,
                rq_entries: config.ring_entries,
                area_ptr: (&mut area_reg as *mut _) as u64,
                region_ptr: (&mut region_reg as *mut _) as u64,
                ..Default::default()
            };
            ring.submitter().register_ifq(&ifq_reg)?;

            let area_token = unsafe { ptr::read_volatile(&area_reg.rq_area_token) };
            let offsets = unsafe { ptr::read_volatile(&ifq_reg.offsets) };
            let ifq_id = unsafe { ptr::read_volatile(&ifq_reg.zcrx_id) };
            let refill_ring = unsafe {
                RefillRing::new(
                    &refill_memory,
                    offsets.head,
                    offsets.tail,
                    offsets.rqes,
                    config.ring_entries,
                )?
            };

            Ok(Self {
                ring,
                fd,
                area,
                refill_ring,
                _refill_memory: refill_memory,
                area_token,
                ifq_id,
                recv_len: config.recv_len,
            })
        }

        fn run(mut self, tx: mpsc::Sender<io::Result<Vec<u8>>>) {
            if let Err(e) = self.run_inner(&tx) {
                let _ = tx.blocking_send(Err(e));
            }
        }

        fn run_inner(&mut self, tx: &mpsc::Sender<io::Result<Vec<u8>>>) -> io::Result<()> {
            self.submit_recv()?;

            loop {
                self.ring.submit_and_wait(1)?;

                let mut resubmit = false;
                {
                    let mut cq = self.ring.completion();
                    for cqe in &mut cq {
                        if cqe.user_data() != RECV_USER_DATA {
                            continue;
                        }

                        let result = cqe.result();
                        if result < 0 {
                            return Err(io::Error::from_raw_os_error(-result));
                        }
                        if result == 0 {
                            return Ok(());
                        }

                        let len = result as u32;
                        let offset = cqe.big_cqe()[0];
                        let data = self.area.copy_at(offset, len)?;
                        self.refill_ring.recycle(offset, len, self.area_token)?;

                        if tx.blocking_send(Ok(data)).is_err() {
                            return Ok(());
                        }

                        if !cqueue::more(cqe.flags()) {
                            resubmit = true;
                        }
                    }
                }

                if resubmit {
                    self.submit_recv()?;
                }
            }
        }

        fn submit_recv(&mut self) -> io::Result<()> {
            let entry = opcode::RecvZc::new(types::Fd(self.fd.as_raw_fd()), self.recv_len)
                .ifq(self.ifq_id)
                .build()
                .user_data(RECV_USER_DATA);

            let mut sq = self.ring.submission();
            unsafe {
                sq.push(&entry).map_err(|_| {
                    io::Error::new(
                        io::ErrorKind::WouldBlock,
                        "io_uring submission queue is full",
                    )
                })?;
            }
            drop(sq);
            self.ring.submit().map(drop)
        }
    }

    struct RefillRing {
        head: *const u32,
        tail: *mut u32,
        rqes: *mut types::io_uring_zcrx_rqe,
        entries: u32,
        local_tail: u32,
    }

    unsafe impl Send for RefillRing {}

    impl RefillRing {
        unsafe fn new(
            memory: &MmapRegion,
            head_offset: u32,
            tail_offset: u32,
            rqes_offset: u32,
            entries: u32,
        ) -> io::Result<Self> {
            Ok(Self {
                head: checked_offset::<u32>(memory, head_offset, 1)? as *const u32,
                tail: checked_offset::<u32>(memory, tail_offset, 1)?,
                rqes: checked_offset::<types::io_uring_zcrx_rqe>(
                    memory,
                    rqes_offset,
                    entries as usize,
                )?,
                entries,
                local_tail: ptr::read_volatile(checked_offset::<u32>(memory, tail_offset, 1)?),
            })
        }

        fn recycle(&mut self, offset: u64, len: u32, area_token: u64) -> io::Result<()> {
            let head = unsafe { ptr::read_volatile(self.head) };
            if self.local_tail.wrapping_sub(head) >= self.entries {
                return Err(io::Error::new(
                    io::ErrorKind::WouldBlock,
                    "io_uring ZC Rx refill ring is full",
                ));
            }

            let slot = self.local_tail & (self.entries - 1);
            unsafe {
                let rqe = self.rqes.add(slot as usize);
                ptr::write(
                    rqe,
                    types::io_uring_zcrx_rqe {
                        off: (offset & ZCRX_DATA_OFFSET_MASK) | area_token,
                        len,
                        __pad: 0,
                    },
                );
                self.local_tail = self.local_tail.wrapping_add(1);
                ptr::write_volatile(self.tail, self.local_tail);
            }

            Ok(())
        }
    }

    unsafe fn checked_offset<T>(
        memory: &MmapRegion,
        offset: u32,
        count: usize,
    ) -> io::Result<*mut T> {
        let offset = offset as usize;
        let bytes = count.checked_mul(mem::size_of::<T>()).ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidData, "io_uring ZC Rx offset overflow")
        })?;
        let end = offset.checked_add(bytes).ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidData, "io_uring ZC Rx offset overflow")
        })?;
        if end > memory.len() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "io_uring ZC Rx refill ring offsets are out of bounds",
            ));
        }
        Ok(memory.as_ptr().add(offset).cast())
    }

    struct MmapRegion {
        ptr: NonNull<u8>,
        len: usize,
    }

    unsafe impl Send for MmapRegion {}

    impl MmapRegion {
        fn new(len: usize) -> io::Result<Self> {
            if len == 0 {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "mmap length must be non-zero",
                ));
            }

            let ptr = unsafe {
                libc::mmap(
                    ptr::null_mut(),
                    len,
                    libc::PROT_READ | libc::PROT_WRITE,
                    libc::MAP_PRIVATE | libc::MAP_ANONYMOUS,
                    -1,
                    0,
                )
            };
            if ptr == libc::MAP_FAILED {
                return Err(io::Error::last_os_error());
            }

            Ok(Self {
                ptr: NonNull::new(ptr.cast()).ok_or_else(|| {
                    io::Error::new(io::ErrorKind::Other, "mmap returned a null pointer")
                })?,
                len,
            })
        }

        fn as_ptr(&self) -> *mut u8 {
            self.ptr.as_ptr()
        }

        fn len(&self) -> usize {
            self.len
        }

        fn copy_at(&self, offset: u64, len: u32) -> io::Result<Vec<u8>> {
            let start = usize::try_from(offset & ZCRX_DATA_OFFSET_MASK).map_err(|_| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    "io_uring ZC Rx offset is too large",
                )
            })?;
            let len = len as usize;
            let end = start.checked_add(len).ok_or_else(|| {
                io::Error::new(io::ErrorKind::InvalidData, "io_uring ZC Rx length overflow")
            })?;
            if end > self.len {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "io_uring ZC Rx completion points outside the receive area",
                ));
            }

            let data = unsafe { slice::from_raw_parts(self.ptr.as_ptr().add(start), len) };
            Ok(data.to_vec())
        }
    }

    impl Drop for MmapRegion {
        fn drop(&mut self) {
            unsafe {
                libc::munmap(self.ptr.as_ptr().cast(), self.len);
            }
        }
    }

    fn refill_ring_len(entries: u32) -> io::Result<usize> {
        let page_size = page_size()?;
        let rqes = (entries as usize)
            .checked_mul(mem::size_of::<types::io_uring_zcrx_rqe>())
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "io_uring ZC Rx ring is too large",
                )
            })?;
        align_up(page_size.checked_add(rqes).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "io_uring ZC Rx ring is too large",
            )
        })?)
    }

    fn align_up(len: usize) -> io::Result<usize> {
        let page_size = page_size()?;
        len.checked_add(page_size - 1)
            .map(|v| v & !(page_size - 1))
            .ok_or_else(|| {
                io::Error::new(io::ErrorKind::InvalidInput, "io_uring ZC Rx size overflow")
            })
    }

    fn page_size() -> io::Result<usize> {
        let size = unsafe { libc::sysconf(libc::_SC_PAGESIZE) };
        if size <= 0 {
            Err(io::Error::last_os_error())
        } else {
            Ok(size as usize)
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

    fn resolve_if_index(stream: &TcpStream, config: &IoUringZcRxConfig) -> io::Result<u32> {
        if let Some(index) = config.interface_index {
            if index == 0 {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "io_uring ZC Rx interface_index must be non-zero",
                ));
            }
            return Ok(index);
        }

        if let Some(name) = config.interface.as_deref() {
            return if_nametoindex(name);
        }

        infer_if_index(stream.local_addr()?)
    }

    fn if_nametoindex(name: &str) -> io::Result<u32> {
        let name = CString::new(name).map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "io_uring ZC Rx interface name contains a NUL byte",
            )
        })?;
        let index = unsafe { libc::if_nametoindex(name.as_ptr()) };
        if index == 0 {
            Err(io::Error::last_os_error())
        } else {
            Ok(index)
        }
    }

    fn infer_if_index(addr: SocketAddr) -> io::Result<u32> {
        let ip = addr.ip();
        if ip.is_unspecified() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "cannot infer io_uring ZC Rx interface from an unspecified local address",
            ));
        }

        let mut ifaddrs = ptr::null_mut();
        if unsafe { libc::getifaddrs(&mut ifaddrs) } != 0 {
            return Err(io::Error::last_os_error());
        }
        let _guard = IfAddrs(ifaddrs);

        let mut cur = ifaddrs;
        while !cur.is_null() {
            let ifaddr = unsafe { &*cur };
            if !ifaddr.ifa_addr.is_null() && sockaddr_matches(ifaddr.ifa_addr, ip) {
                let name = unsafe { CStr::from_ptr(ifaddr.ifa_name) }
                    .to_str()
                    .map_err(|_| {
                        io::Error::new(
                            io::ErrorKind::InvalidData,
                            "interface name is not valid UTF-8",
                        )
                    })?;
                return if_nametoindex(name);
            }
            cur = ifaddr.ifa_next;
        }

        Err(io::Error::new(
            io::ErrorKind::NotFound,
            format!("cannot infer io_uring ZC Rx interface for {}", addr),
        ))
    }

    struct IfAddrs(*mut libc::ifaddrs);

    impl Drop for IfAddrs {
        fn drop(&mut self) {
            unsafe {
                libc::freeifaddrs(self.0);
            }
        }
    }

    fn sockaddr_matches(sockaddr: *const libc::sockaddr, ip: IpAddr) -> bool {
        match (unsafe { (*sockaddr).sa_family as i32 }, ip) {
            (libc::AF_INET, IpAddr::V4(expected)) => {
                let sockaddr = unsafe { &*(sockaddr.cast::<libc::sockaddr_in>()) };
                let actual = Ipv4Addr::from(u32::from_be(sockaddr.sin_addr.s_addr));
                actual == expected
            }
            (libc::AF_INET6, IpAddr::V6(expected)) => {
                let sockaddr = unsafe { &*(sockaddr.cast::<libc::sockaddr_in6>()) };
                let actual = Ipv6Addr::from(sockaddr.sin6_addr.s6_addr);
                actual == expected
            }
            _ => false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::MaybeZcRxTcpStream;
    use crate::config::IoUringZcRxConfig;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::{TcpListener, TcpStream};

    #[tokio::test]
    async fn wrapper_forwards_data_when_zc_rx_is_unavailable() {
        let config = IoUringZcRxConfig {
            enabled: true,
            ring_entries: 64,
            area_size: 1024 * 1024,
            recv_len: 4096,
            ..Default::default()
        };

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server_config = config.clone();
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut stream = MaybeZcRxTcpStream::new(stream, &server_config);
            let mut buf = [0; 4];
            stream.read_exact(&mut buf).await.unwrap();
            assert_eq!(&buf, b"ping");
            stream.write_all(b"pong").await.unwrap();
        });

        let stream = TcpStream::connect(addr).await.unwrap();
        let mut stream = MaybeZcRxTcpStream::new(stream, &config);
        stream.write_all(b"ping").await.unwrap();
        let mut buf = [0; 4];
        stream.read_exact(&mut buf).await.unwrap();
        assert_eq!(&buf, b"pong");

        server.await.unwrap();
    }
}
