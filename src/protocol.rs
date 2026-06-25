pub const HASH_WIDTH_IN_BYTES: usize = 32;

use anyhow::{Context, Result, bail};
use bytes::{Bytes, BytesMut};
use lazy_static::lazy_static;
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use std::net::SocketAddr;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tracing::trace;

type ProtocolVersion = u8;
const _PROTO_V0: u8 = 0u8;
const _PROTO_V1: u8 = 1u8;
const PROTO_V2: u8 = 2u8;

pub const CURRENT_PROTO_VERSION: ProtocolVersion = PROTO_V2;

pub type Digest = [u8; HASH_WIDTH_IN_BYTES];

pub(crate) fn encode<T: Serialize>(
    value: &T,
) -> std::result::Result<Vec<u8>, bincode::error::EncodeError> {
    bincode::serde::encode_to_vec(value, bincode::config::legacy())
}

fn decode<T: DeserializeOwned>(
    bytes: &[u8],
) -> std::result::Result<T, bincode::error::DecodeError> {
    bincode::serde::decode_from_slice(bytes, bincode::config::legacy()).map(|(value, _)| value)
}

fn encoded_size<T: Serialize>(
    value: &T,
) -> std::result::Result<usize, bincode::error::EncodeError> {
    encode(value).map(|bytes| bytes.len())
}

#[derive(Deserialize, Serialize, Debug)]
pub enum Hello {
    ControlChannelHello(ProtocolVersion, Digest), // sha256sum(service name) or a nonce
    DataChannelHello(ProtocolVersion, Digest),    // token provided by CreateDataChannel
    VisitorChannelHello(ProtocolVersion, Digest), // sha256sum(service name)
}

#[derive(Deserialize, Serialize, Debug)]
pub struct Auth(pub Digest);

#[derive(Deserialize, Serialize, Debug)]
pub enum Ack {
    Ok,
    ServiceNotExist,
    AuthFailed,
    NoClient,
    Unsupported,
}

impl std::fmt::Display for Ack {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}",
            match self {
                Ack::Ok => "Ok",
                Ack::ServiceNotExist => "Service not exist",
                Ack::AuthFailed => "Incorrect token",
                Ack::NoClient => "No client available",
                Ack::Unsupported => "Unsupported",
            }
        )
    }
}

#[derive(Deserialize, Serialize, Debug)]
pub enum ControlChannelCmd {
    CreateDataChannel,
    HeartBeat,
}

#[derive(Deserialize, Serialize, Debug)]
pub enum DataChannelCmd {
    StartForwardTcp,
    StartForwardUdp,
    HeartBeat,
}

type UdpPacketLen = u16; // `u16` should be enough for any practical UDP traffic on the Internet
#[derive(Deserialize, Serialize, Debug)]
struct UdpHeader {
    from: SocketAddr,
    len: UdpPacketLen,
}

#[derive(Debug)]
pub struct UdpTraffic {
    pub from: SocketAddr,
    pub data: Bytes,
}

impl UdpTraffic {
    pub async fn write<T: AsyncWrite + Unpin>(&self, writer: &mut T) -> Result<()> {
        let hdr = UdpHeader {
            from: self.from,
            len: self.data.len() as UdpPacketLen,
        };

        let v = encode(&hdr).unwrap();

        trace!("Write {:?} of length {}", hdr, v.len());
        writer.write_u8(v.len() as u8).await?;
        writer.write_all(&v).await?;

        writer.write_all(&self.data).await?;

        Ok(())
    }

    #[allow(dead_code)]
    pub async fn write_slice<T: AsyncWrite + Unpin>(
        writer: &mut T,
        from: SocketAddr,
        data: &[u8],
    ) -> Result<()> {
        let hdr = UdpHeader {
            from,
            len: data.len() as UdpPacketLen,
        };

        let v = encode(&hdr).unwrap();

        trace!("Write {:?} of length {}", hdr, v.len());
        writer.write_u8(v.len() as u8).await?;
        writer.write_all(&v).await?;

        writer.write_all(data).await?;

        Ok(())
    }

    pub async fn read<T: AsyncRead + Unpin>(reader: &mut T, hdr_len: u8) -> Result<UdpTraffic> {
        let mut buf = vec![0; hdr_len as usize];
        reader
            .read_exact(&mut buf)
            .await
            .with_context(|| "Failed to read udp header")?;

        let hdr: UdpHeader = decode(&buf).with_context(|| "Failed to deserialize UdpHeader")?;

        trace!("hdr {:?}", hdr);

        let mut data = BytesMut::new();
        data.resize(hdr.len as usize, 0);
        reader.read_exact(&mut data).await?;

        Ok(UdpTraffic {
            from: hdr.from,
            data: data.freeze(),
        })
    }
}

pub fn digest(data: &[u8]) -> Digest {
    use sha2::{Digest, Sha256};
    let d = Sha256::new().chain_update(data).finalize();
    d.into()
}

struct PacketLength {
    hello: usize,
    ack: usize,
    auth: usize,
    c_cmd: usize,
    d_cmd: usize,
}

impl PacketLength {
    pub fn new() -> PacketLength {
        let username = "default";
        let d = digest(username.as_bytes());
        let hello = encoded_size(&Hello::ControlChannelHello(CURRENT_PROTO_VERSION, d)).unwrap();
        let c_cmd = encoded_size(&ControlChannelCmd::CreateDataChannel).unwrap();
        let d_cmd = encoded_size(&DataChannelCmd::StartForwardTcp).unwrap();
        let ack = Ack::Ok;
        let ack = encoded_size(&ack).unwrap();

        let auth = encoded_size(&Auth(d)).unwrap();
        PacketLength {
            hello,
            ack,
            auth,
            c_cmd,
            d_cmd,
        }
    }
}

lazy_static! {
    static ref PACKET_LEN: PacketLength = PacketLength::new();
}

pub async fn read_hello<T: AsyncRead + AsyncWrite + Unpin>(conn: &mut T) -> Result<Hello> {
    let mut buf = vec![0u8; PACKET_LEN.hello];
    conn.read_exact(&mut buf)
        .await
        .with_context(|| "Failed to read hello")?;
    let hello = decode(&buf).with_context(|| "Failed to deserialize hello")?;

    match hello {
        Hello::ControlChannelHello(v, _) => {
            if v != CURRENT_PROTO_VERSION {
                bail!(
                    "Protocol version mismatched. Expected {}, got {}. Please update `rathole`.",
                    CURRENT_PROTO_VERSION,
                    v
                );
            }
        }
        Hello::DataChannelHello(v, _) => {
            if v != CURRENT_PROTO_VERSION {
                bail!(
                    "Protocol version mismatched. Expected {}, got {}. Please update `rathole`.",
                    CURRENT_PROTO_VERSION,
                    v
                );
            }
        }
        Hello::VisitorChannelHello(v, _) => {
            if v != CURRENT_PROTO_VERSION {
                bail!(
                    "Protocol version mismatched. Expected {}, got {}. Please update `rathole`.",
                    CURRENT_PROTO_VERSION,
                    v
                );
            }
        }
    }

    Ok(hello)
}

pub async fn read_auth<T: AsyncRead + AsyncWrite + Unpin>(conn: &mut T) -> Result<Auth> {
    let mut buf = vec![0u8; PACKET_LEN.auth];
    conn.read_exact(&mut buf)
        .await
        .with_context(|| "Failed to read auth")?;
    decode(&buf).with_context(|| "Failed to deserialize auth")
}

pub async fn read_ack<T: AsyncRead + AsyncWrite + Unpin>(conn: &mut T) -> Result<Ack> {
    let mut bytes = vec![0u8; PACKET_LEN.ack];
    conn.read_exact(&mut bytes)
        .await
        .with_context(|| "Failed to read ack")?;
    decode(&bytes).with_context(|| "Failed to deserialize ack")
}

pub async fn read_control_cmd<T: AsyncRead + AsyncWrite + Unpin>(
    conn: &mut T,
) -> Result<ControlChannelCmd> {
    let mut bytes = vec![0u8; PACKET_LEN.c_cmd];
    conn.read_exact(&mut bytes)
        .await
        .with_context(|| "Failed to read cmd")?;
    decode(&bytes).with_context(|| "Failed to deserialize control cmd")
}

pub async fn read_data_cmd<T: AsyncRead + AsyncWrite + Unpin>(
    conn: &mut T,
) -> Result<DataChannelCmd> {
    let mut bytes = vec![0u8; PACKET_LEN.d_cmd];
    conn.read_exact(&mut bytes)
        .await
        .with_context(|| "Failed to read cmd")?;
    decode(&bytes).with_context(|| "Failed to deserialize data cmd")
}
