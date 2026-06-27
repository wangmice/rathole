# rathole

![rathole-logo](./docs/img/rathole-logo.png)

[![GitHub stars](https://img.shields.io/github/stars/verofess/rathole-ng)](https://github.com/verofess/rathole-ng/stargazers)
[![GitHub release (latest SemVer)](https://img.shields.io/github/v/release/verofess/rathole-ng)](https://github.com/verofess/rathole-ng/releases)
![GitHub Workflow Status (branch)](https://img.shields.io/github/actions/workflow/status/verofess/rathole-ng/rust.yml?branch=main)
[![GitHub all releases](https://img.shields.io/github/downloads/verofess/rathole-ng/total)](https://github.com/verofess/rathole-ng/releases)
[![GHCR image](https://img.shields.io/badge/ghcr.io-verofess%2Frathole--ng-blue)](https://github.com/verofess/rathole-ng/pkgs/container/rathole-ng)

[English](README.md) | [简体中文](README-zh.md)

安全、稳定、高性能的内网穿透工具，用 Rust 语言编写

rathole，类似于 [frp](https://github.com/fatedier/frp) 和 [ngrok](https://github.com/inconshreveable/ngrok)，可以让 NAT 后的设备上的服务通过具有公网 IP 的服务器暴露在公网上。

<!-- TOC -->

- [rathole](#rathole)
  - [Features](#features)
  - [Quickstart](#quickstart)
  - [Configuration](#configuration)
    - [Logging](#logging)
    - [Tuning](#tuning)
  - [Benchmark](#benchmark)
  - [Development Status](#development-status)

<!-- /TOC -->

## Features

- **高性能** 具有更高的吞吐量，高并发下更稳定。见[Benchmark](#benchmark)
- **低资源消耗** 内存占用远低于同类工具。见[Benchmark](#benchmark)。[二进制文件最小](docs/build-guide.md)可以到 **~500KiB**，可以部署在嵌入式设备如路由器上。
- **安全性** 每个服务单独强制鉴权。Server 和 Client 负责各自的配置。使用 Noise Protocol 可以简单地配置传输加密，而不需要自签证书。同时也支持 TLS。
- **热重载** 支持配置文件热重载，动态修改端口转发服务。HTTP API 正在开发中。

## Quickstart

一个全功能的 `rathole` 可以从 [release](https://github.com/verofess/rathole-ng/releases) 页面下载。或者 [从源码编译](docs/build-guide.md) **获取其他平台和最小化的二进制文件**。也可以从 GHCR 拉取 Docker image：

```sh
docker pull ghcr.io/verofess/rathole-ng:latest
```

`rathole` 的使用和 frp 非常类似，如果你有后者的使用经验，那配置对你来说非常简单，区别只是转发服务的配置分离到了服务端和客户端，并且必须要设置 token。

使用 rathole 需要一个有公网 IP 的服务器，和一个在 NAT 或防火墙后的设备，其中有些服务需要暴露在互联网上。

假设你在家里的 NAT 后面有一个 NAS，并且想把它的 ssh 服务暴露在公网上：

1. 在有一个公网 IP 的服务器上

创建 `server.toml`，内容如下，并根据你的需要调整。

```toml
# server.toml
[server]
bind_addr = "0.0.0.0:2333" # `2333` 配置了服务端监听客户端连接的端口

[server.services.my_nas_ssh]
token = "use_a_secret_that_only_you_know" # 用于验证的 token
bind_addr = "0.0.0.0:5202" # `5202` 配置了将 `my_nas_ssh` 暴露给互联网的端口
```

然后运行:

```bash
./rathole server.toml
```

2. 在 NAT 后面的主机（你的 NAS）上

创建 `client.toml`，内容如下，并根据你的需要进行调整。

```toml
# client.toml
[client]
remote_addr = "myserver.com:2333" # 服务器的地址。端口必须与 `server.bind_addr` 中的端口相同。
[client.services.my_nas_ssh]
token = "use_a_secret_that_only_you_know" # 必须与服务器相同以通过验证
local_addr = "127.0.0.1:22" # 需要被转发的服务的地址
```

然后运行：

```bash
./rathole client.toml
```

3. 现在 `rathole` 客户端会连接运行在 `myserver.com:2333`的 `rathole` 服务器，任何到 `myserver.com:5202` 的流量将被转发到客户端所在主机的 `22` 端口。

所以你可以 `ssh myserver.com:5202` 来 ssh 到你的 NAS。

### STCP/SUDP 隐藏服务

若不想在服务端暴露公网端口，可使用原生 `stcp` 或 `sudp` 模式。服务端只转发经认证的 visitor 连接；用户访问 visitor 主机上的本地端口即可。

```toml
# server.toml
[server]
bind_addr = "0.0.0.0:2333"
default_token = "use_a_secret_that_only_you_know"

[server.services.my_nas_ssh]
mode = "stcp"
```

```toml
# provider.toml，运行隐藏服务的主机
[client]
remote_addr = "myserver.com:2333"
default_token = "use_a_secret_that_only_you_know"

[client.services.my_nas_ssh]
local_addr = "127.0.0.1:22"
```

```toml
# visitor.toml，需要访问隐藏服务的主机
[client]
remote_addr = "myserver.com:2333"
default_token = "use_a_secret_that_only_you_know"

[client.visitors.my_nas_ssh]
bind_addr = "127.0.0.1:5202"
```

在 visitor 主机上连接 `127.0.0.1:5202` 即可。UDP 服务使用相同结构，设 `type = "udp"` 与 `mode = "sudp"`；完整示例见 [examples/stcp](./examples/stcp) 与 [examples/sudp](./examples/sudp)。

### 动态服务端地址（DDNS / TXT / IP4P）

当 `remote_addr` 使用**域名**时，客户端按顺序解析：**TXT**（base64 编码的 `ip:port`）→ **IP4P**（前缀 `2001::/80` 的 AAAA）→ **标准 A/AAAA**。IP 字面量跳过此链路。

支持的 `remote_addr` 形式：`example.com:2333`、`127.0.0.1:2333`、`[::1]:2333`（IPv6 推荐加方括号）或 `::1:2333`。

端口来自 TXT/IP4P 时用 `remote_addr = "example.com:0"`；走普通 DNS 且端口固定时用 `remote_addr = "example.com:2333"`。

控制通道保持连接期间会复用首次解析的 IP，直到断线重连。修改配置中的 `remote_addr` 或 `dns` 会触发**整 client 重启**（增量热重载仅增删 service/visitor）。

可选自定义 DNS 上游（省略则使用系统 DNS）：

```toml
[client]
remote_addr = "example.com:0"
dns = ["114.114.114.114", "8.8.8.8:53"] # 单独写 "system" 可强制使用系统解析器
```

TXT 示例：存储 `203.0.113.10:2333` 的 base64（`MjAzLjAuMTEzLjEwOjIzMzM=`）。

最小示例见 [examples/ddns](./examples/ddns)。

在 Linux 上以后台服务运行，参见 [systemd 示例](./examples/systemd)。

## Configuration

如果只有一个 `[server]` 和 `[client]` 块存在的话，`rathole` 可以根据配置文件的内容自动决定在服务器模式或客户端模式下运行，就像 [Quickstart](#quickstart) 中的例子。

但 `[client]` 和 `[server]` 块也可以放在一个文件中。然后在服务器端，运行 `rathole --server config.toml`。在客户端，运行 `rathole --client config.toml` 来明确告诉 `rathole` 运行模式。

**推荐首先查看 [examples](./examples) 中的配置示例来快速理解配置格式**，如果有不清楚的地方再查阅完整配置格式。

关于如何配置 Noise Protocol 和 TLS 来进行加密传输，参见 [Transport](./docs/transport.md)。

下面是完整的配置格式。

```toml
[client]
remote_addr = "example.com:2333" # 必填。服务端地址：域名:端口、IPv4（127.0.0.1:2333）或 IPv6（[::1]:2333）。IP 字面量不走 DDNS 解析
# dns = ["114.114.114.114", "8.8.8.8:53"] # 可选。`remote_addr` 为域名时使用的 DNS 上游。省略则用系统 DNS。每项可为 IP、`ip:port` 或 `dns://ip:port`；仅写 "system" 则强制系统解析器
default_token = "default_token_if_not_specify" # 可选。各 service 未单独设置 token 时的默认值
heartbeat_timeout = 40 # 可选。应用层心跳超时，0 为禁用；须大于 `server.heartbeat_interval`。默认 40 秒
retry_interval = 1 # 可选。控制通道重连的指数退避上限（秒）。首次约 500ms，逐步增至该值；无限重试。默认 1
post_half_close_idle_timeout = 120 # 可选。对端半关闭后的空闲超时，"off" 禁用。默认 120

[client.transport] # 整块可选
type = "tcp" # 可选。tcp / tls / noise。默认 tcp

[client.transport.tcp] # 可选。也作用于 noise 与 tls
proxy = "socks5://user:passwd@127.0.0.1:1080" # 可选。连接服务端时使用的代理，支持 http 与 socks5
nodelay = true # 可选。TCP_NODELAY。默认 true
keepalive_secs = 20 # 可选。默认 20 秒
keepalive_interval = 8 # 可选。默认 8 秒
fast_open = false # 可选。TCP Fast Open。默认 false
quickack = false # 可选。Linux TCP_QUICKACK。默认 false
msg_zerocopy = false # 可选。Linux MSG_ZEROCOPY。默认 false

[client.transport.io_uring_zc_rx] # 可选。实验性 io_uring 零拷贝接收
enabled = false
interface = "eth0"
# interface_index = 2
rx_queue = 0
ring_entries = 4096
area_size = 16777216
recv_len = 65536

[client.transport.tls] # type = "tls" 时必填
trusted_root = "ca.pem"
hostname = "example.com" # 可选。未设置则回退到 `remote_addr` 中的主机名

[client.transport.noise] # 见 docs/transport.md
pattern = "Noise_NK_25519_ChaChaPoly_BLAKE2s"
local_private_key = "key_encoded_in_base64"
remote_public_key = "key_encoded_in_base64"

[client.transport.websocket]
tls = true

[client.services.service1]
type = "tcp" # 可选。tcp 或 udp。默认 tcp
token = "whatever" # 未设置 default_token 时必填
local_addr = "127.0.0.1:1081"
nodelay = true
retry_interval = 1 # 可选。继承 `[client] retry_interval`

[client.services.service2]
local_addr = "127.0.0.1:1082"

[client.visitors.service3] # mode = stcp/sudp 的隐藏服务 visitor
type = "tcp"
token = "whatever"
bind_addr = "127.0.0.1:1083"
retry_interval = 1

[server]
bind_addr = "0.0.0.0:2333"
default_token = "default_token_if_not_specify"
heartbeat_interval = 30 # 默认 30 秒
post_half_close_idle_timeout = 120

[server.transport]
type = "tcp"

[server.transport.tcp]
nodelay = true
keepalive_secs = 20
keepalive_interval = 8
fast_open = false
quickack = false
msg_zerocopy = false

[server.transport.io_uring_zc_rx]
enabled = false
interface = "eth0"
rx_queue = 0
ring_entries = 4096
area_size = 16777216
recv_len = 65536

[server.transport.tls]
pkcs12 = "identify.pfx"
pkcs12_password = "password"

[server.transport.noise]
pattern = "Noise_NK_25519_ChaChaPoly_BLAKE2s"
local_private_key = "key_encoded_in_base64"
remote_public_key = "key_encoded_in_base64"

[server.transport.websocket]
tls = true

[server.services.service1]
type = "tcp"
mode = "public" # 可选。public / stcp / sudp。默认 public
token = "whatever"
bind_addr = "0.0.0.0:8081" # public 模式必填；stcp/sudp 不在服务端暴露业务端口
nodelay = true

[server.services.service2]
bind_addr = "0.0.0.1:8082"

[server.services.service3]
mode = "stcp"

[server.services.service4]
type = "udp"
mode = "sudp"
```

### Logging

`rathole`，像许多其他 Rust 程序一样，使用环境变量来控制日志级别。

支持的 Logging Level 有 `info`, `warn`, `error`, `debug`, `trace`

比如将日志级别设置为 `error`:

```shell
RUST_LOG=error ./rathole config.toml
```

如果 `RUST_LOG` 不存在，默认的日志级别是 `info`。

### Tuning

从 v0.4.7 开始, rathole 默认启用 TCP_NODELAY。这能够减少延迟并使交互式应用受益，比如 RDP，Minecraft 服务器。但它会减少一些带宽。

如果带宽更重要，比如网盘类应用，TCP_NODELAY 仍然可以通过配置 `nodelay = false` 关闭。

### TCP Fast Open

`[client.transport.tcp]` 和 `[server.transport.tcp]` 下的 `fast_open = true` 会在平台支持时启用 TCP Fast Open。在 Linux 上，`rathole` 会对出站 TCP 连接设置 `TCP_FASTOPEN_CONNECT`，对监听 socket 设置 `TCP_FASTOPEN`。如果当前平台或内核不支持这些 socket option，会记录 warning 并继续使用普通 TCP。

这个选项只作用于底层 TCP socket，因此也会影响 TLS、Noise 和 WebSocket transport。它不会替代应用层认证；操作系统可能还需要额外的 TCP Fast Open sysctl 或策略设置，才能真正在线路上使用 TFO。

### `quickack`

`[client.transport.tcp]` 和 `[server.transport.tcp]` 下的 `quickack = true` 会在 Linux 上为 transport 持有的 TCP socket 启用 `TCP_QUICKACK`。`TCP_QUICKACK` 不是持久模式，而是一次性提示；因此 `rathole` 会在每次成功读到 TCP 数据后重新设置它。当前平台不支持 `TCP_QUICKACK` 时，会记录 warning 并继续使用普通 delayed ACK 行为。

这个选项只作用于 transport 底层的 tunnel TCP socket。明文 TCP 转发只要能使用 `splice`，仍会保留 `splice` 数据路径；在这条路径上，`rathole` 会在 tunnel 侧成功完成 splice 读取后重新设置 `TCP_QUICKACK`。

### `msg_zerocopy`

`[client.transport.tcp]` 和 `[server.transport.tcp]` 下的 `msg_zerocopy = true` 会在 Linux 上为仍然必须经过用户态的 TCP 写入启用 `SO_ZEROCOPY` / `MSG_ZEROCOPY`，包括 TLS、Noise、WebSocket、控制通道写入，以及因为 `io_uring_zc_rx` 激活而无法使用 `splice` 的明文 TCP 流。普通明文 TCP 转发只要能拿回原始 `TcpStream`，仍然优先使用 Linux `splice`。

Linux 通过 socket error queue 报告 `MSG_ZEROCOPY` 完成通知。`rathole` 会 drain `MSG_ERRQUEUE`，并把发送缓冲区一直保留到内核通过 `sock_extended_err.ee_info..=ee_data` 报告对应的完成范围。如果当前平台拒绝 `SO_ZEROCOPY`，或发送时遇到 `ENOBUFS`，会回退到普通 TCP 写入。

这个选项只适合部分大写入场景。内核仍可能在内部复制数据，并通过 `SO_EE_CODE_ZEROCOPY_COPIED` 标记；小写入可能因为 page pinning 和完成通知开销变慢。

### `io_uring_zc_rx`

`[client.transport.io_uring_zc_rx]` 和 `[server.transport.io_uring_zc_rx]` 会启用实验性的 Linux 接收侧 zero-copy 路径，底层使用 `IORING_OP_RECV_ZC`。启用后，`rathole` 会尽可能为 TCP 流注册 io_uring ZC Rx；如果内核、网卡、RX 队列或当前平台不支持，会回退到现有 TCP 路径。明文 TCP 转发在 ZC Rx 不可用时仍会使用 Linux `splice`。

这个选项需要支持 io_uring ZC Rx 的 Linux 内核，以及满足内核要求的网卡/驱动配置，包括 header/data split、flow steering 和 RSS。`interface` 或 `interface_index` 与 `rx_queue` 用于指定要注册的 RX 队列；如果没有指定接口，`rathole` 会尝试根据 socket 的本地地址推断接口。

## Benchmark

rathole 的延迟与 [frp](https://github.com/fatedier/frp) 相近，在高并发情况下表现更好，能提供更大的带宽，内存占用更少。

关于测试进行的更多细节，参见单独页面 [Benchmark](./docs/benchmark.md)。

**但是，不要从这里得出结论，`rathole` 能让内网转发出来的服务快上数倍。** Benchmark 是在本地回环上进行的，其结果说明了任务受 CPU 限制时的结果。当用户的网络不是瓶颈时，用户能得到很大的提升。但是，对很多用户来说并不是这样。在这种情况下，`rathole` 能带来的主要好处是更少的资源占用，而带宽和延迟不一定有显著的改善。

![http_throughput](./docs/img/http_throughput.svg)
![tcp_bitrate](./docs/img/tcp_bitrate.svg)
![udp_bitrate](./docs/img/udp_bitrate.svg)
![mem](./docs/img/mem-graph.png)

## Development Status

`rathole` 正在积极开发中

- [x] 支持 TLS
- [x] 支持 UDP
- [x] 热重载
- [ ] 用于配置的 HTTP APIs

[Out of Scope](./docs/out-of-scope.md) 列举了没有计划开发的特性并说明了原因。
