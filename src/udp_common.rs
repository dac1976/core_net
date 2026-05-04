// This file is part of core-net Rust crate containing useful reusable
// networking utilities.
//
// Copyright (C) 2026 to present, Duncan Crutchley
// Contact <15799155+dac1976@users.noreply.github.com>
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published
// by the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License and GNU Lesser General Public License
// for more details.
//
// You should have received a copy of the GNU General Public License
// and GNU Lesser General Public License along with this program. If
// not, see <http://www.gnu.org/licenses/>.

use crate::{
    pool::{BufferPool, MessageBuf},
    protocol::{MessageHeader, ProtocolError},
};
use std::net::SocketAddr;
use thiserror::Error;

#[derive(Debug, Clone)]
pub struct ReceivedDatagram {
    pub from: SocketAddr,
    pub header: MessageHeader,
    pub payload: MessageBuf,
}

#[derive(Debug, Error)]
pub enum UdpDatagramError {
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    #[error("protocol error: {0}")]
    Protocol(#[from] ProtocolError),

    #[error("outbound queue full")]
    OutboundQueueFull,

    #[error("outbound queue closed")]
    OutboundQueueClosed,

    #[error("receiver already taken")]
    ReceiverAlreadyTaken,

    #[error("invalid inbound datagram length: {0}")]
    InvalidInboundLength(usize),

    #[error("datagram shorter than header: {0}")]
    DatagramTooShort(usize),

    #[error("header total length {header_total} does not match datagram length {datagram_len}")]
    DatagramLengthMismatch {
        header_total: usize,
        datagram_len: usize,
    },
}

#[derive(Clone)]
pub enum UdpOutboundCommand {
    SendTo { to: SocketAddr, payload: MessageBuf },
    Close,
}

pub fn parse_datagram(
    datagram: &[u8],
    from: SocketAddr,
    expected_magic_string: &[u8; crate::protocol::MAGIC_STRING_LEN],
    recv_pool: Option<&BufferPool>,
) -> Result<ReceivedDatagram, UdpDatagramError> {
    if datagram.len() < MessageHeader::WIRE_SIZE {
        return Err(UdpDatagramError::DatagramTooShort(datagram.len()));
    }

    let header = MessageHeader::decode(&datagram[..MessageHeader::WIRE_SIZE])?;
    header.validate_against(expected_magic_string)?;

    let total_len = header.total_length as usize;
    if total_len < MessageHeader::WIRE_SIZE {
        return Err(UdpDatagramError::InvalidInboundLength(total_len));
    }

    if total_len != datagram.len() {
        return Err(UdpDatagramError::DatagramLengthMismatch {
            header_total: total_len,
            datagram_len: datagram.len(),
        });
    }

    let payload_len = header.payload_len()?;
    let payload_start = MessageHeader::WIRE_SIZE;
    let payload_end = payload_start + payload_len;

    let payload =
        MessageBuf::from_slice_with_pool(recv_pool, &datagram[payload_start..payload_end]);

    Ok(ReceivedDatagram {
        from,
        header,
        payload,
    })
}
