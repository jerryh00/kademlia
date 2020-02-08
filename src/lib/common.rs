extern crate log;

use std::io::Read;
use std::io::Write;

use std::net::{TcpStream, UdpSocket, SocketAddr};
use log::{trace};

pub const FUNCTION: &str = "function";
pub const NODE_ID: &str = "node_id";
pub const RPC_ID: &str = "rpc_id";
pub const ACK: &str = "ack";
pub const OK: &str = "ok";

pub const PING: &str = "ping";
pub const PING_ACK: &str = "ping_ack";

pub const FIND_NODE: &str = "find_node";
pub const FIND_NODE_ACK: &str = "find_node_ack";
pub const TARGET_NODE_ID: &str = "target_node_id";
pub const NODE_ENTRIES: &str = "node_entries";
pub const STORE: &str = "store";
pub const KEY: &str = "key";
pub const VALUE: &str = "value";
pub const STORE_ACK: &str = "store_ack";
pub const FIND_VALUE: &str = "find_value";
pub const FIND_VALUE_ACK: &str = "find_value_ack";

const MAX_MSG_LEN: usize = 8192;
const LEN_BYTES: usize = 2;

#[allow(dead_code)]
pub fn write_message_tcp(mut stream: &TcpStream, message: &[u8]) -> std::io::Result<()> {
    if message.len() > MAX_MSG_LEN {
        Err(std::io::Error::new(std::io::ErrorKind::Other, "message size too big"))
    } else {
        stream.write_all(&(message.len() as u16).to_le_bytes())?;
        stream.write_all(message)
    }
}

#[allow(dead_code)]
pub fn read_message_tcp(mut stream: &TcpStream) -> std::io::Result<Vec<u8>> {
    let mut buffer = [0; LEN_BYTES];
    stream.read_exact(&mut buffer)?;
    trace!("buffer={:?}", buffer);
    let len:u16 = u16::from_le_bytes(buffer);
    trace!("len={}", len);

    let mut recv = vec![0; len as usize];

    stream.read_exact(&mut recv[0..len as usize])?;
    Ok(recv[0..(len as usize)].to_vec())
}

pub fn read_message_udp(socket: &UdpSocket) -> std::io::Result<(Vec<u8>, SocketAddr)> {
    let mut buf = vec![0; MAX_MSG_LEN];
    let (recv_size, src) = socket.recv_from(&mut buf)?;

    trace!("udp recv_size: {:?}, src: {:?}", recv_size, src);

    if recv_size < LEN_BYTES {
        return Err(std::io::Error::new(std::io::ErrorKind::Other, "message too short"));
    }

    let mut len_array:[u8; LEN_BYTES] = [0; LEN_BYTES];
    len_array.copy_from_slice(&buf[0..LEN_BYTES]);
    let len:u16 = u16::from_le_bytes(len_array);
    trace!("len={}", len);

    if recv_size != LEN_BYTES + len as usize {
        return Err(std::io::Error::new(std::io::ErrorKind::Other, "message invalid"));
    }

    Ok((buf[LEN_BYTES..(LEN_BYTES + len as usize)].to_vec(), src))
}

pub fn write_message_udp(socket: &UdpSocket, message: &[u8], dest: &SocketAddr) -> std::io::Result<()> {
    if message.len() > MAX_MSG_LEN {
        Err(std::io::Error::new(std::io::ErrorKind::Other, "message size too big"))
    } else {
        let mut len_and_msg:Vec<u8> = Vec::new();
        len_and_msg.extend(&(message.len() as u16).to_le_bytes());
        len_and_msg.extend(message);
        socket.send_to(&len_and_msg, dest)?;
        Ok(())
    }
}

pub fn compare_collection<T>(v1:&[T], v2:&[T]) -> bool
    where T:std::cmp::PartialEq {
    v1.len() == v2.len() && v1.iter().zip(v2.iter()).all(|(a, b)| a == b)
}
