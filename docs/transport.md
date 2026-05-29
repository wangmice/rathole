# Security

By default, `rathole` forwards traffic as it is. Different options can be enabled to secure the traffic.

## TCP Fast Open

TCP Fast Open can be enabled with `fast_open = true` in `[client.transport.tcp]` and `[server.transport.tcp]`:

```toml
[client.transport.tcp]
fast_open = true

[server.transport.tcp]
fast_open = true
```

The setting is shared by all transports that use TCP underneath, including TCP, TLS, Noise, and WebSocket. On Linux, `rathole` sets `TCP_FASTOPEN_CONNECT` for outbound sockets and `TCP_FASTOPEN` for listeners. If the socket option is unavailable, `rathole` logs a warning and falls back to regular TCP.

The operating system can still gate TCP Fast Open with sysctl or network policy, so enabling this option in `rathole` only requests TFO from the socket layer.

## MSG_ZEROCOPY

Linux `MSG_ZEROCOPY` can be enabled with `msg_zerocopy = true` in `[client.transport.tcp]` and `[server.transport.tcp]`:

```toml
[client.transport.tcp]
msg_zerocopy = true

[server.transport.tcp]
msg_zerocopy = true
```

This requests `SO_ZEROCOPY` on the underlying TCP sockets and uses `MSG_ZEROCOPY` for TCP writes that still pass through userspace. That includes TLS, Noise, WebSocket, control-channel writes, and plain TCP streams only when another enabled feature such as io_uring ZC Rx prevents the splice forwarding path. Plain TCP forwarding continues to use Linux `splice` whenever possible.

`MSG_ZEROCOPY` is not fire-and-forget. Linux queues completion notifications on the socket error queue and reports inclusive send-call ranges through `sock_extended_err.ee_info..=ee_data`. `rathole` drains `MSG_ERRQUEUE` in the background and keeps owned send buffers alive until those completions arrive, so user buffers are not reused while the kernel may still reference them.

If `SO_ZEROCOPY` is unavailable, or a zerocopy send fails with `ENOBUFS`, `rathole` falls back to regular TCP writes. The kernel can also complete a request after copying internally and mark it with `SO_EE_CODE_ZEROCOPY_COPIED`, so this option is mainly useful for large-write workloads where page pinning and completion overhead are worth it.

## io_uring ZC Rx

`rathole` can optionally try Linux io_uring zero-copy receive through `[client.transport.io_uring_zc_rx]` and `[server.transport.io_uring_zc_rx]`. The option is global for the transport block and applies to TCP, TLS, Noise, and WebSocket because all of them sit on top of TCP sockets.

```toml
[client.transport.io_uring_zc_rx]
enabled = true
interface = "eth0"
rx_queue = 0
ring_entries = 4096
area_size = 16777216
recv_len = 65536
```

The implementation probes and registers `IORING_OP_RECV_ZC` per TCP stream. If the platform is not Linux, the kernel does not support the opcode, or the selected NIC/RX queue cannot be registered, `rathole` logs the fallback and uses the normal TCP read path. Plain TCP forwarding keeps using Linux `splice` when ZC Rx is unavailable.

ZC Rx is not enabled by the kernel alone. The selected NIC and driver must also satisfy the kernel requirements, including header/data split, flow steering, and RSS, and those features may need out-of-band setup with tools such as `ethtool`. `interface_index` can be used instead of `interface`; if neither is set, `rathole` tries to infer the interface from the socket's local address.

## TLS

Checkout the [example](../examples/tls)

### Client

Normally, a self-signed certificate is used. In this case, the client needs to trust the CA. `trusted_root` is the path to the root CA's certificate PEM file.
`hostname` is the hostname that the client used to validate aginst the certificate that the server presents. Note that it does not have to be the same with the `remote_addr` in `[client]`.

```toml
[client.transport.tls]
trusted_root = "example/tls/rootCA.crt"
hostname = "localhost"
```

### Server

PKCS#12 archives are needed to run the server.

It can be created using openssl like:

```sh
openssl pkcs12 -export -out identity.pfx -inkey server.key -in server.crt -certfile ca_chain_certs.crt
```

Aruguments are:

- `-inkey`: Server Private Key
- `-in`: Server Certificate
- `-certfile`: CA Certificate

Creating self-signed certificate with one's own CA is a non-trival task. However, a script is provided under tls example folder for reference.

### Rustls Support

`rathole` provides optional `rustls` support. [Build Guide](build-guide.md) demostrated this.

One difference is that, the crate we use for loading PKCS#12 archives can only handle limited types of PBE algorithms. We only support PKCS#12 archives that they (crate `p12`) support. So we need to specify the legacy format (openssl 1.x format) when creating the PKCS#12 archive.

In short, the command used with openssl 3 to create the PKCS#12 archive with `rustls` support is:

```sh
openssl pkcs12 -export -out identity.pfx -inkey server.key -in server.crt -certfile ca_chain_certs.crt -legacy
```

## Noise Protocol

### Quickstart for the Noise Protocl

In one word, the [Noise Protocol](http://noiseprotocol.org/noise.html) is a lightweigt, easy to configure and drop-in replacement of TLS. No need to create a self-sign certificate to secure the connection.

`rathole` comes with a reasonable default configuration for noise protocol. You can a glimpse of the minimal [example](../examples/noise_nk) for how it will look like.

The default noise protocol that `rathole` uses, which is `Noise_NK_25519_ChaChaPoly_BLAKE2s`, providing the authentication of the server, just like TLS with properly configured certificates. So MITM is no more a problem.

To use it, a X25519 keypair is needed.

#### Generate a Keypair

1. Run `rathole --genkey`, which will generate a keypair using the default X25519 algorithm.

It emits:

```sh
$ rathole --genkey
Private Key:
cQ/vwIqNPJZmuM/OikglzBo/+jlYGrOt9i0k5h5vn1Q=

Public Key:
GQYTKSbWLBUSZiGfdWPSgek9yoOuaiwGD/GIX8Z1kkE=
```

(WARNING: Don't use the keypair from the Internet, including this one)

2. The server should keep the private key to identify itself. And the client should keep the public key, which is used to verify whether the peer is the authentic server.

So relevant snippets of configuration are:

```toml
# Client Side Configuration
[client.transport]
type = "noise"
[client.transport.noise]
remote_public_key = "GQYTKSbWLBUSZiGfdWPSgek9yoOuaiwGD/GIX8Z1kkE="

# Server Side Configuration
[server.transport]
type = "noise"
[server.transport.noise]
local_private_key = "cQ/vwIqNPJZmuM/OikglzBo/+jlYGrOt9i0k5h5vn1Q="
```

Then `rathole` will run under the protection of the Noise Protocol.

## Specifying the Pattern of Noise Protocol

The default configuration of Noise Protocol that comes with `rathole` satifies most use cases, which is described above. But there're other patterns that can be useful.

### No Authentication

This configuration provides encryption of the traffic but provides no authentication, which means it's vulnerable to MITM attack, but is resistent to the sniffing and replay attack. If MITM attack is not one of the concerns, this is more convenient to use.

```toml
# Server Side Configuration
[server.transport.noise]
pattern = "Noise_XX_25519_ChaChaPoly_BLAKE2s"

# Client Side Configuration
[client.transport.noise]
pattern = "Noise_XX_25519_ChaChaPoly_BLAKE2s"
```

### Bidirectional Authentication

```toml
# Server Side Configuration
[server.transport.noise]
pattern = "Noise_KK_25519_ChaChaPoly_BLAKE2s"
local_private_key = "server-priv-key-here"
remote_public_key = "client-pub-key-here"

# Client Side Configuration
[client.transport.noise]
pattern = "Noise_KK_25519_ChaChaPoly_BLAKE2s"
local_private_key = "client-priv-key-here"
remote_public_key = "server-pub-key-here"
```

### Other Patterns

To find out which pattern to use, refer to:

- [7.5. Interactive handshake patterns (fundamental)](https://noiseprotocol.org/noise.html#interactive-handshake-patterns-fundamental)
- [8. Protocol names and modifiers](https://noiseprotocol.org/noise.html#protocol-names-and-modifiers)

Note that PSKs are not supported currently. Free to open an issue if you need it.
