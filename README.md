# rathole

![rathole-logo](./docs/img/rathole-logo.png)

[![GitHub stars](https://img.shields.io/github/stars/verofess/rathole-ng)](https://github.com/verofess/rathole-ng/stargazers)
[![GitHub release (latest SemVer)](https://img.shields.io/github/v/release/verofess/rathole-ng)](https://github.com/verofess/rathole-ng/releases)
![GitHub Workflow Status (branch)](https://img.shields.io/github/actions/workflow/status/verofess/rathole-ng/rust.yml?branch=main)
[![GitHub all releases](https://img.shields.io/github/downloads/verofess/rathole-ng/total)](https://github.com/verofess/rathole-ng/releases)
[![GHCR image](https://img.shields.io/badge/ghcr.io-verofess%2Frathole--ng-blue)](https://github.com/verofess/rathole-ng/pkgs/container/rathole-ng)

[English](README.md) | [简体中文](README-zh.md)

A secure, stable and high-performance reverse proxy for NAT traversal, written in Rust

rathole, like [frp](https://github.com/fatedier/frp) and [ngrok](https://github.com/inconshreveable/ngrok), can help to expose the service on the device behind the NAT to the Internet, via a server with a public IP.

<!-- TOC -->

- [rathole](#rathole)
  - [Features](#features)
  - [Quickstart](#quickstart)
  - [Configuration](#configuration)
    - [Logging](#logging)
    - [Tuning](#tuning)
  - [Benchmark](#benchmark)
  - [Planning](#planning)

<!-- /TOC -->

## Features

- **High Performance** Much higher throughput can be achieved than frp, and more stable when handling a large volume of connections. See [Benchmark](#benchmark)
- **Low Resource Consumption** Consumes much fewer memory than similar tools. See [Benchmark](#benchmark). [The binary can be](docs/build-guide.md) **as small as ~500KiB** to fit the constraints of devices, like embedded devices as routers.
- **Security** Tokens of services are mandatory and service-wise. The server and clients are responsible for their own configs. With the optional Noise Protocol, encryption can be configured at ease. No need to create a self-signed certificate! TLS is also supported.
- **Hot Reload** Services can be added or removed dynamically by hot-reloading the configuration file. HTTP API is WIP.

## Quickstart

A full-powered `rathole` can be obtained from the [release](https://github.com/verofess/rathole-ng/releases) page. Or [build from source](docs/build-guide.md) **for other platforms and minimizing the binary**. A Docker image is also available from GHCR:

```sh
docker pull ghcr.io/verofess/rathole-ng:latest
```

The usage of `rathole` is very similar to frp. If you have experience with the latter, then the configuration is very easy for you. The only difference is that configuration of a service is split into the client side and the server side, and a token is mandatory.

To use `rathole`, you need a server with a public IP, and a device behind the NAT, where some services that need to be exposed to the Internet.

Assuming you have a NAS at home behind the NAT, and want to expose its ssh service to the Internet:

1. On the server which has a public IP

Create `server.toml` with the following content and accommodate it to your needs.

```toml
# server.toml
[server]
bind_addr = "0.0.0.0:2333" # `2333` specifies the port that rathole listens for clients

[server.services.my_nas_ssh]
token = "use_a_secret_that_only_you_know" # Token that is used to authenticate the client for the service. Change to an arbitrary value.
bind_addr = "0.0.0.0:5202" # `5202` specifies the port that exposes `my_nas_ssh` to the Internet
```

Then run:

```bash
./rathole server.toml
```

2. On the host which is behind the NAT (your NAS)

Create `client.toml` with the following content and accommodate it to your needs.

```toml
# client.toml
[client]
remote_addr = "myserver.com:2333" # The address of the server. The port must be the same with the port in `server.bind_addr`

[client.services.my_nas_ssh]
token = "use_a_secret_that_only_you_know" # Must be the same with the server to pass the validation
local_addr = "127.0.0.1:22" # The address of the service that needs to be forwarded
```

Then run:

```bash
./rathole client.toml
```

3. Now the client will try to connect to the server `myserver.com` on port `2333`, and any traffic to `myserver.com:5202` will be forwarded to the client's port `22`.

So you can `ssh myserver.com:5202` to ssh to your NAS.

### STCP/SUDP-like hidden services

For services that should not expose a public port on the server, use the native `stcp` or `sudp` mode. The server only relays authenticated visitor connections; users connect to a visitor's local port instead.

```toml
# server.toml
[server]
bind_addr = "0.0.0.0:2333"
default_token = "use_a_secret_that_only_you_know"

[server.services.my_nas_ssh]
mode = "stcp"
```

```toml
# provider.toml, on the host that owns the hidden service
[client]
remote_addr = "myserver.com:2333"
default_token = "use_a_secret_that_only_you_know"

[client.services.my_nas_ssh]
local_addr = "127.0.0.1:22"
```

```toml
# visitor.toml, on the host that wants to access the hidden service
[client]
remote_addr = "myserver.com:2333"
default_token = "use_a_secret_that_only_you_know"

[client.visitors.my_nas_ssh]
bind_addr = "127.0.0.1:5202"
```

Then connect to `127.0.0.1:5202` on the visitor host. UDP services use the same shape with `type = "udp"` and `mode = "sudp"`; see [examples/stcp](./examples/stcp) and [examples/sudp](./examples/sudp) for complete examples.

To run `rathole` run as a background service on Linux, checkout the [systemd examples](./examples/systemd).

## Configuration

`rathole` can automatically determine to run in the server mode or the client mode, according to the content of the configuration file, if only one of `[server]` and `[client]` block is present, like the example in [Quickstart](#quickstart).

But the `[client]` and `[server]` block can also be put in one file. Then on the server side, run `rathole --server config.toml` and on the client side, run `rathole --client config.toml` to explicitly tell `rathole` the running mode.

Before heading to the full configuration specification, it's recommend to skim [the configuration examples](./examples) to get a feeling of the configuration format.

See [Transport](./docs/transport.md) for more details about encryption and the `transport` block.

Here is the full configuration specification:

```toml
[client]
remote_addr = "example.com:2333" # Necessary. The address of the server
default_token = "default_token_if_not_specify" # Optional. The default token of services, if they don't define their own ones
heartbeat_timeout = 40 # Optional. Set to 0 to disable the application-layer heartbeat test. The value must be greater than `server.heartbeat_interval`. Default: 40 seconds
retry_interval = 1 # Optional. The interval between retry to connect to the server. Default: 1 second
post_half_close_idle_timeout = 120 # Optional. Idle deadline applied to a forwarder once one peer has half-closed; see the dedicated section below. Use `"off"` to disable (legacy behavior). Default: 120

[client.transport] # The whole block is optional. Specify which transport to use
type = "tcp" # Optional. Possible values: ["tcp", "tls", "noise"]. Default: "tcp"

[client.transport.tcp] # Optional. Also affects `noise` and `tls`
proxy = "socks5://user:passwd@127.0.0.1:1080" # Optional. The proxy used to connect to the server. `http` and `socks5` is supported.
nodelay = true # Optional. Determine whether to enable TCP_NODELAY, if applicable, to improve the latency but decrease the bandwidth. Default: true
keepalive_secs = 20 # Optional. Specify `tcp_keepalive_time` in `tcp(7)`, if applicable. Default: 20 seconds
keepalive_interval = 8 # Optional. Specify `tcp_keepalive_intvl` in `tcp(7)`, if applicable. Default: 8 seconds
fast_open = false # Optional. Enable TCP Fast Open on supported platforms. Default: false
quickack = false # Optional. Re-arm Linux TCP_QUICKACK after successful TCP reads. Default: false
msg_zerocopy = false # Optional. Linux MSG_ZEROCOPY send path for TCP writes that must pass through userspace. Default: false

[client.transport.io_uring_zc_rx] # Optional. Experimental Linux io_uring zero-copy receive path. Also affects `tcp`, `tls`, `noise`, and `websocket`.
enabled = false # Optional. Try io_uring ZC Rx where possible, falling back to regular TCP reads or splice when unavailable. Default: false
interface = "eth0" # Optional. Network interface name. If omitted, rathole tries to infer it from the socket's local address.
# interface_index = 2 # Optional alternative to `interface`.
rx_queue = 0 # Optional. RX queue to register. Default: 0
ring_entries = 4096 # Optional. Must be a non-zero power of two. Default: 4096
area_size = 16777216 # Optional. Size of the ZC Rx area in bytes. Default: 16 MiB
recv_len = 65536 # Optional. Maximum bytes requested per receive completion. Default: 64 KiB

[client.transport.tls] # Necessary if `type` is "tls"
trusted_root = "ca.pem" # Necessary. The certificate of CA that signed the server's certificate
hostname = "example.com" # Optional. The hostname that the client uses to validate the certificate. If not set, fallback to `client.remote_addr`

[client.transport.noise] # Noise protocol. See `docs/transport.md` for further explanation
pattern = "Noise_NK_25519_ChaChaPoly_BLAKE2s" # Optional. Default value as shown
local_private_key = "key_encoded_in_base64" # Optional
remote_public_key = "key_encoded_in_base64" # Optional

[client.transport.websocket] # Necessary if `type` is "websocket"
tls = true # If `true` then it will use settings in `client.transport.tls`

[client.services.service1] # A service that needs forwarding. The name `service1` can change arbitrarily, as long as identical to the name in the server's configuration
type = "tcp" # Optional. The protocol that needs forwarding. Possible values: ["tcp", "udp"]. Default: "tcp"
token = "whatever" # Necessary if `client.default_token` not set
local_addr = "127.0.0.1:1081" # Necessary. The address of the service that needs to be forwarded
nodelay = true # Optional. Override the `client.transport.nodelay` per service
retry_interval = 1 # Optional. The interval between retry to connect to the server. Default: inherits the global config

[client.services.service2] # Multiple services can be defined
local_addr = "127.0.0.1:1082"

[client.visitors.service3] # A private visitor for a server service with mode = "stcp" or "sudp"
type = "tcp" # Optional. Possible values: ["tcp", "udp"]. Default: "tcp"
token = "whatever" # Necessary if `client.default_token` not set
bind_addr = "127.0.0.1:1083" # Local address where visitor users connect
nodelay = true # Optional. Same as services
retry_interval = 1 # Optional. The interval between retries to listen locally/connect to the server

[server]
bind_addr = "0.0.0.0:2333" # Necessary. The address that the server listens for clients. Generally only the port needs to be change.
default_token = "default_token_if_not_specify" # Optional
heartbeat_interval = 30 # Optional. The interval between two application-layer heartbeat. Set to 0 to disable sending heartbeat. Default: 30 seconds
post_half_close_idle_timeout = 120 # Optional. Idle deadline applied to a forwarder once one peer has half-closed; see the dedicated section below. Use `"off"` to disable (legacy behavior). Default: 120

[server.transport] # Same as `[client.transport]`
type = "tcp"

[server.transport.tcp] # Same as the client
nodelay = true
keepalive_secs = 20
keepalive_interval = 8
fast_open = false
quickack = false
msg_zerocopy = false

[server.transport.io_uring_zc_rx] # Same as the client
enabled = false
interface = "eth0"
# interface_index = 2
rx_queue = 0
ring_entries = 4096
area_size = 16777216
recv_len = 65536

[server.transport.tls] # Necessary if `type` is "tls"
pkcs12 = "identify.pfx" # Necessary. pkcs12 file of server's certificate and private key
pkcs12_password = "password" # Necessary. Password of the pkcs12 file

[server.transport.noise] # Same as `[client.transport.noise]`
pattern = "Noise_NK_25519_ChaChaPoly_BLAKE2s"
local_private_key = "key_encoded_in_base64"
remote_public_key = "key_encoded_in_base64"

[server.transport.websocket] # Necessary if `type` is "websocket"
tls = true # If `true` then it will use settings in `server.transport.tls`

[server.services.service1] # The service name must be identical to the client side
type = "tcp" # Optional. Same as the client `[client.services.X.type]
mode = "public" # Optional. Possible values: ["public", "stcp", "sudp"]. Default: "public"
token = "whatever" # Necessary if `server.default_token` not set
bind_addr = "0.0.0.0:8081" # Necessary for public services. stcp/sudp services do not expose a public service port.
nodelay = true # Optional. Same as the client

[server.services.service2]
bind_addr = "0.0.0.1:8082"

[server.services.service3]
mode = "stcp"

[server.services.service4]
type = "udp"
mode = "sudp"
```

### Logging

`rathole`, like many other Rust programs, use environment variables to control the logging level. `info`, `warn`, `error`, `debug`, `trace` are available.

```shell
RUST_LOG=error ./rathole config.toml
```

will run `rathole` with only error level logging.

If `RUST_LOG` is not present, the default logging level is `info`.

### Tuning

From v0.4.7, rathole enables TCP_NODELAY by default, which should benefit the latency and interactive applications like rdp, Minecraft servers. However, it slightly decreases the bandwidth.

If the bandwidth is more important, TCP_NODELAY can be opted out with `nodelay = false`.

### TCP Fast Open

`fast_open = true` under `[client.transport.tcp]` and `[server.transport.tcp]` enables TCP Fast Open where the platform supports it. On Linux, `rathole` sets `TCP_FASTOPEN_CONNECT` for outbound TCP connections and `TCP_FASTOPEN` for listeners. Platforms or kernels that do not support these socket options log a warning and continue with regular TCP.

The option only applies to the underlying TCP sockets, so it also affects TLS, Noise, and WebSocket transports. It does not replace application-layer authentication, and the operating system may require its own TCP Fast Open sysctl or policy settings before the feature is actually used on the wire.

### `quickack`

`quickack = true` under `[client.transport.tcp]` and `[server.transport.tcp]` enables Linux `TCP_QUICKACK` for the transport TCP sockets. `TCP_QUICKACK` is a one-shot hint rather than a persistent mode, so `rathole` re-arms it after each successful TCP read that yields bytes. Platforms that do not support `TCP_QUICKACK` log a warning and continue with normal delayed ACK behavior.

This only affects the tunnel TCP sockets owned by the transport. Plain TCP forwarding still keeps its `splice` data path when possible; when that path is active, `rathole` re-arms `TCP_QUICKACK` on the tunnel side after successful splice reads.

### `msg_zerocopy`

`msg_zerocopy = true` under `[client.transport.tcp]` and `[server.transport.tcp]` enables the Linux `SO_ZEROCOPY` / `MSG_ZEROCOPY` send path for TCP writes that still have to pass through userspace, such as TLS, Noise, WebSocket, control-channel writes, and plain TCP streams when `io_uring_zc_rx` prevents the splice path. Plain TCP forwarding keeps using Linux `splice` whenever possible.

Linux reports `MSG_ZEROCOPY` completion through the socket error queue. `rathole` drains `MSG_ERRQUEUE` and keeps the owned send buffers alive until the kernel reports the inclusive completion range in `sock_extended_err.ee_info..=ee_data`. If the platform rejects `SO_ZEROCOPY` or a send hits `ENOBUFS`, `rathole` falls back to regular TCP writes.

This option is useful only for some large-write workloads. The kernel may still copy data internally and report that with `SO_EE_CODE_ZEROCOPY_COPIED`; small writes can be slower due to page pinning and completion overhead.

### `io_uring_zc_rx`

`[client.transport.io_uring_zc_rx]` and `[server.transport.io_uring_zc_rx]` enable an experimental Linux receive-side zero-copy path based on `IORING_OP_RECV_ZC`. When enabled, `rathole` tries to register io_uring ZC Rx for TCP streams and falls back to the existing TCP path if the kernel, NIC, queue, or platform does not support it. Plain TCP forwarding still uses Linux `splice` when ZC Rx is unavailable.

This option requires Linux with io_uring ZC Rx support and a NIC/driver configured for the kernel requirements, including header/data split, flow steering, and RSS. The `interface` or `interface_index` and `rx_queue` settings identify the RX queue to register; if neither interface setting is provided, `rathole` tries to infer the interface from the socket's local address.

### `post_half_close_idle_timeout`

When a forwarded peer sends `FIN` (half-closes its write side) but never closes its own read side, the underlying socket can sit indefinitely in `CLOSE-WAIT` / `FIN-WAIT-2`. Over time these orphaned sockets accumulate. The `post_half_close_idle_timeout` option is the leak guard for that pattern.

**How it arms.** While both directions of a forwarder are still carrying bytes, no timeout applies — long-lived idle full-duplex connections (SSH, MQTT keepalive, long-poll) are unaffected. The deadline only arms once one direction has reached EOF and that EOF has been propagated as `shutdown(SHUT_WR)` to the peer's write side. From that point on, the surviving direction must read at least one byte (or hit EOF / error) within the configured window, otherwise the forwarder is reaped and both ends are torn down.

**Per-step deadline.** The deadline is checked at every read, write and flush — not just reads — so a peer that half-closes and then stops draining its read side cannot wedge the forwarder inside `write_all` via TCP backpressure.

**Configuration.**
- Set on `[client]` and `[server]` blocks at top level. Hot-reload of these top-level fields restarts the running instance; service-level edits remain incremental.
- Accepts a non-negative integer (seconds) or the string `"off"` (disables the timeout entirely; legacy `copy_bidirectional` behavior).
- Default is `120` seconds.
- `0` is allowed and means "tear down immediately on half-close" — useful for protocols that never half-close legitimately.

**Per-transport behavior.**
- `tcp`, `tls`, `noise`: full half-close-then-respond preserved. The leak guard only arms the timeout after the surviving direction enters its post-EOF idle phase.
- `socket_stream` (Unix domain sockets): same as TCP.
- `websocket`, `websocket` (TLS): WebSocket cannot carry a half-close-then-respond pattern — RFC 6455 requires the receiver of a Close frame to reply with its own Close, which closes its sending side too. The timeout still bounds cleanup but cannot change the protocol's full-close semantics.

**Observability.** Every reap is logged at debug level with the message `Forwarder (...) reaped by post-half-close idle timeout`. To verify the leak guard is firing in production, run with `RUST_LOG=rathole=debug` and grep for that line.

## Benchmark

rathole has similar latency to [frp](https://github.com/fatedier/frp), but can handle a more connections, provide larger bandwidth, with less memory usage.

For more details, see the separate page [Benchmark](./docs/benchmark.md).

**However, don't take it from here that `rathole` can magically make your forwarded service faster several times than before.** The benchmark is done on local loopback, indicating the performance when the task is cpu-bounded. One can gain quite a improvement if the network is not the bottleneck. Unfortunately, that's not true for many users. In that case, the main benefit is lower resource consumption, while the bandwidth and the latency may not improved significantly.

![http_throughput](./docs/img/http_throughput.svg)
![tcp_bitrate](./docs/img/tcp_bitrate.svg)
![udp_bitrate](./docs/img/udp_bitrate.svg)
![mem](./docs/img/mem-graph.png)

## Planning

- [ ] HTTP APIs for configuration

[Out of Scope](./docs/out-of-scope.md) lists features that are not planned to be implemented and why.
