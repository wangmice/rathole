use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr, SocketAddrV4, SocketAddrV6};
use std::sync::{Arc, RwLock};
use std::time::Duration;

use anyhow::{anyhow, Context, Result};
use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64;
use hickory_resolver::config::{
    ConnectionConfig, LookupIpStrategy, NameServerConfig, ResolverConfig, ResolverOpts,
};
use hickory_resolver::net::runtime::TokioRuntimeProvider;
use hickory_resolver::proto::rr::RData;
use hickory_resolver::TokioResolver;
use tokio::net::lookup_host;
use tracing::debug;
use url::Url;

use crate::helper::to_socket_addr;

static RESOLVER: RwLock<Option<Arc<TokioResolver>>> = RwLock::new(None);

/// Initialize the client DNS resolver from `[client] dns` config.
/// Empty list uses system DNS; explicit entries use those upstream servers.
pub fn init(dns_servers: &[String]) -> Result<()> {
    let resolver: Arc<hickory_resolver::Resolver<TokioRuntimeProvider>> = Arc::new(build_resolver(dns_servers)?);
    *RESOLVER
        .write()
        .map_err(|_| anyhow!("DNS resolver lock poisoned"))? = Some(resolver);
    Ok(())
}

fn get_resolver() -> Result<Arc<TokioResolver>> {
    RESOLVER
        .read()
        .map_err(|_| anyhow!("DNS resolver lock poisoned"))?
        .clone()
        .ok_or_else(|| anyhow!("DNS resolver is not initialized"))
}

fn default_opts() -> ResolverOpts {
    let mut opts = ResolverOpts::default();
    opts.attempts = 3;
    opts.timeout = Duration::from_secs(4);
    opts.cache_size = 4096;
    opts.ip_strategy = LookupIpStrategy::Ipv4thenIpv6;
    opts.num_concurrent_reqs = 2;
    opts.try_tcp_on_error = true;
    opts
}

fn build_resolver(dns_servers: &[String]) -> Result<TokioResolver> {
    let opts = default_opts();

    if dns_servers.is_empty() || dns_servers.iter().any(|s| s == "system") {
        return TokioResolver::builder_tokio()
            .map_err(|e| anyhow!("failed to read system DNS config: {e}"))?
            .with_options(opts)
            .build()
            .map_err(|e| anyhow!("failed to build system DNS resolver: {e}"));
    }

    let mut config = ResolverConfig::from_parts(None, vec![], vec![]);
    for server in dns_servers {
        let socket_addr = parse_dns_server(server)
            .with_context(|| format!("invalid DNS upstream {server:?}"))?;
        let ip = socket_addr.ip();
        let port = socket_addr.port();
        let mut connection = ConnectionConfig::udp();
        connection.port = port;
        config.add_name_server(NameServerConfig::new(ip, true, vec![connection]));
    }

    TokioResolver::builder_with_config(config, TokioRuntimeProvider::default())
        .with_options(opts)
        .build()
        .map_err(|e| anyhow!("failed to build custom DNS resolver: {e}"))
}

fn parse_dns_server(server: &str) -> Result<SocketAddr> {
    if let Ok(url) = Url::parse(server) {
        match url.scheme() {
            "dns" | "udp" => {
                let host = url
                    .host_str()
                    .ok_or_else(|| anyhow!("missing host in DNS URL {server}"))?;
                let port = url.port().unwrap_or(53);
                return parse_host_port(host, port);
            }
            "system" => return Err(anyhow!("system DNS must be configured alone")),
            other => return Err(anyhow!("unsupported DNS URL scheme {other}")),
        }
    }

    if let Some((host, port)) = server.rsplit_once(':') {
        if !host.contains(':') {
            let port: u16 = port.parse().with_context(|| format!("invalid port in {server}"))?;
            return parse_host_port(host, port);
        }
    }

    parse_host_port(server, 53)
}

/// Validate a `[client] dns` entry during config parsing.
pub fn validate_dns_upstream(server: &str) -> Result<()> {
    if server == "system" {
        return Ok(());
    }
    parse_dns_server(server).map(|_| ())
}

fn parse_host_port(host: &str, port: u16) -> Result<SocketAddr> {
    if let Ok(ip) = host.parse::<IpAddr>() {
        return Ok(SocketAddr::new(ip, port));
    }
    Err(anyhow!("DNS upstream must be an IP address, got {host}"))
}

/// Resolve `client.remote_addr` for domain names with TXT → IP4P → standard DNS fallback.
pub async fn resolve_client_remote_addr(addr: &str) -> Result<SocketAddr> {
    let (host, config_port) = parse_remote_host_port(addr)?;

    if is_ip_literal(host) {
        return to_socket_addr(addr).await;
    }

    let resolver = get_resolver()?;
    let addrs = lookup_host_with_fallback(&resolver, host, config_port).await?;

    addrs
        .into_iter()
        .next()
        .ok_or_else(|| anyhow!("no DNS records for {host}"))
}

fn parse_remote_host_port(addr: &str) -> Result<(&str, u16)> {
    let semi = addr
        .rfind(':')
        .ok_or_else(|| anyhow!("missing port in remote_addr {addr}"))?;
    let port = addr[semi + 1..]
        .parse()
        .with_context(|| format!("invalid port in remote_addr {addr}"))?;
    Ok((&addr[..semi], port))
}

fn is_ip_literal(host: &str) -> bool {
    host.parse::<IpAddr>().is_ok()
}

async fn lookup_host_with_fallback(
    resolver: &TokioResolver,
    domain: &str,
    config_port: u16,
) -> Result<Vec<SocketAddr>> {
    if let Ok(addrs) = try_resolve_txt(resolver, domain).await {
        debug!("Resolved {domain} via TXT to {addrs:?}");
        return Ok(addrs);
    }

    if let Ok(addrs) = try_resolve_ip4p(resolver, domain, config_port).await {
        debug!("Resolved {domain} via IP4P to {addrs:?}");
        return Ok(addrs);
    }

    debug!("Resolved {domain} via standard DNS lookup");
    try_resolve_standard(resolver, domain, config_port).await
}

async fn try_resolve_txt(resolver: &TokioResolver, domain: &str) -> Result<Vec<SocketAddr>> {
    let lookup = resolver.txt_lookup(domain).await?;
    for record in lookup.answers() {
        let RData::TXT(txt) = &record.data else {
            continue;
        };
        for chunk in txt.txt_data.iter() {
            if let Ok(addrs) = decode_txt_record(chunk) {
                return Ok(addrs);
            }
        }
    }
    Err(anyhow!("no valid TXT record"))
}

/// TXT payload is base64-encoded `ipv4:port` or comma-separated entries.
fn decode_txt_record(txt: &[u8]) -> Result<Vec<SocketAddr>> {
    let encoded = std::str::from_utf8(txt)
        .map_err(|e| anyhow!("TXT record is not UTF-8: {e}"))?
        .trim()
        .trim_matches('"');

    let sanitized: String = encoded
        .chars()
        .filter(|c| c.is_ascii_alphanumeric() || *c == '+' || *c == '/' || *c == '=')
        .collect();

    let decoded_bytes = BASE64
        .decode(sanitized)
        .map_err(|e| anyhow!("TXT base64 decode failed: {e}"))?;
    let decoded_str = std::str::from_utf8(&decoded_bytes)
        .map_err(|e| anyhow!("TXT decoded payload is not UTF-8: {e}"))?
        .trim();

    if decoded_str.is_empty() {
        return Err(anyhow!("empty TXT payload"));
    }

    let mut addrs = Vec::new();
    for entry in decoded_str.split(',') {
        let entry = entry.trim();
        if entry.is_empty() {
            continue;
        }
        addrs.push(parse_ip_port(entry)?);
    }

    if addrs.is_empty() {
        return Err(anyhow!("no addresses in TXT payload"));
    }
    Ok(addrs)
}

fn parse_ip_port(s: &str) -> Result<SocketAddr> {
    let semi = s
        .rfind(':')
        .ok_or_else(|| anyhow!("invalid address in TXT payload: {s}"))?;
    let ip: IpAddr = s[..semi]
        .trim_matches(|c| c == '[' || c == ']')
        .parse()
        .map_err(|_| anyhow!("invalid IP in TXT payload: {s}"))?;
    let port: u16 = s[semi + 1..]
        .parse()
        .map_err(|_| anyhow!("invalid port in TXT payload: {s}"))?;
    Ok(SocketAddr::new(ip, port))
}

async fn try_resolve_ip4p(
    resolver: &TokioResolver,
    domain: &str,
    _config_port: u16,
) -> Result<Vec<SocketAddr>> {
    let lookup = resolver.lookup_ip(domain).await?;
    let addrs: Vec<SocketAddr> = lookup
        .iter()
        .filter_map(|ip| match ip {
            IpAddr::V6(v6) => decode_ip4p(&v6),
            IpAddr::V4(_) => None,
        })
        .collect();

    if addrs.is_empty() {
        Err(anyhow!("no IP4P record"))
    } else {
        Ok(addrs)
    }
}

async fn try_resolve_standard(
    resolver: &TokioResolver,
    domain: &str,
    config_port: u16,
) -> Result<Vec<SocketAddr>> {
    match resolver.lookup_ip(domain).await {
        Ok(lookup) => {
            let addrs: Vec<_> = lookup
                .iter()
                .map(|ip| match ip {
                    IpAddr::V4(v4) => SocketAddr::V4(SocketAddrV4::new(v4, config_port)),
                    IpAddr::V6(v6) => SocketAddr::V6(SocketAddrV6::new(v6, config_port, 0, 0)),
                })
                .collect();
            if !addrs.is_empty() {
                return Ok(addrs);
            }
        }
        Err(e) => debug!("hickory lookup_ip failed for {domain}: {e:#}"),
    }

    Ok(lookup_host(format!("{domain}:{config_port}"))
        .await?
        .collect())
}

/// Decode NATMap/heiher-frp IP4P addresses encoded in AAAA records (prefix 2001::/80).
fn decode_ip4p(ipv6: &Ipv6Addr) -> Option<SocketAddr> {
    let segments = ipv6.segments();
    if segments[0..5] == [0x2001, 0, 0, 0, 0] {
        let port = segments[5];
        let embedded_ipv4 = parse_embedded_ipv4(segments[6], segments[7]);
        Some(SocketAddr::V4(SocketAddrV4::new(embedded_ipv4, port)))
    } else {
        None
    }
}

fn parse_embedded_ipv4(yyyy: u16, zzzz: u16) -> Ipv4Addr {
    Ipv4Addr::new(
        (yyyy >> 8) as u8,
        (yyyy & 0xff) as u8,
        (zzzz >> 8) as u8,
        (zzzz & 0xff) as u8,
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decode_ip4p_example() {
        let ip: Ipv6Addr = "2001:0000:0000:0000:0000:18cc:7018:0017".parse().unwrap();
        let addr = decode_ip4p(&ip).unwrap();
        assert_eq!(addr, "112.24.0.23:6348".parse().unwrap());
    }

    #[test]
    fn decode_ip4p_ignores_regular_ipv6() {
        let ip: Ipv6Addr = "2001:db8::1".parse().unwrap();
        assert!(decode_ip4p(&ip).is_none());
    }

    #[test]
    fn parse_txt_base64_payload() {
        let payload = "192.168.1.1:8080";
        let encoded = BASE64.encode(payload);
        let addrs = decode_txt_record(encoded.as_bytes()).unwrap();
        assert_eq!(addrs, vec!["192.168.1.1:8080".parse().unwrap()]);
    }

    #[test]
    fn parse_dns_server_ip_and_port() {
        assert_eq!(
            parse_dns_server("114.114.114.114").unwrap(),
            "114.114.114.114:53".parse().unwrap()
        );
        assert_eq!(
            parse_dns_server("8.8.8.8:5353").unwrap(),
            "8.8.8.8:5353".parse().unwrap()
        );
        assert_eq!(
            parse_dns_server("dns://1.1.1.1:53").unwrap(),
            "1.1.1.1:53".parse().unwrap()
        );
    }

    #[test]
    fn ip_literal_detection() {
        assert!(is_ip_literal("127.0.0.1"));
        assert!(is_ip_literal("::1"));
        assert!(!is_ip_literal("example.com"));
    }
}
