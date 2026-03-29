use crate::{
    pool::MessageBuf, protocol::MessageHeader, tcp_server::ReceivedMessage,
    udp_common::ReceivedDatagram,
};
use std::net::SocketAddr;

#[derive(Debug, Clone)]
pub struct Message {
    pub source_addr: Option<SocketAddr>,
    pub header: MessageHeader,
    pub payload: MessageBuf,
}

impl Message {
    #[allow(dead_code)]
    pub fn from_tcp_server(peer_addr: SocketAddr, message: ReceivedMessage) -> Self {
        Self {
            source_addr: Some(peer_addr),
            header: message.header,
            payload: message.payload,
        }
    }

    #[allow(dead_code)]
    pub fn from_udp(datagram: ReceivedDatagram) -> Self {
        Self {
            source_addr: Some(datagram.from),
            header: datagram.header,
            payload: datagram.payload,
        }
    }
}
