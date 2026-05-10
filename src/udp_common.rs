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

/// Complete core-net UDP datagram received from a peer.
///
/// UDP is message-oriented, so each received datagram should contain exactly
/// one complete core-net framed message:
///
/// ```text
/// [core-net header]
/// [payload]
/// ```
///
/// This type owns the parsed header and payload buffer. The payload is stored
/// in `MessageBuf`, which may be backed by a reusable pooled buffer.
#[derive(Debug, Clone)]
pub struct ReceivedDatagram {
    /// Source socket address of the datagram.
    pub from: SocketAddr,

    /// Parsed core-net message header.
    pub header: MessageHeader,

    /// Payload bytes following the message header.
    pub payload: MessageBuf,
}

/// Errors common to UDP datagram endpoints.
///
/// Shared by:
///
/// - UDP unicast
/// - UDP broadcast
/// - UDP multicast
#[derive(Debug, Error)]
pub enum UdpDatagramError {
    /// Underlying socket/IO failure.
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    /// Core protocol decode/validation failure.
    #[error("protocol error: {0}")]
    Protocol(#[from] ProtocolError),

    /// Non-blocking outbound send failed because the endpoint queue is full.
    #[error("outbound queue full")]
    OutboundQueueFull,

    /// Outbound queue is closed.
    #[error("outbound queue closed")]
    OutboundQueueClosed,

    /// Internal receiver was already taken.
    ///
    /// Endpoint `run()` methods are intended to be called once.
    #[error("receiver already taken")]
    ReceiverAlreadyTaken,

    /// Invalid inbound datagram length.
    #[error("invalid inbound datagram length: {0}")]
    InvalidInboundLength(usize),

    /// Datagram did not contain enough bytes for the fixed protocol header.
    #[error("datagram shorter than header: {0}")]
    DatagramTooShort(usize),

    /// Header length did not match actual UDP datagram length.
    ///
    /// For UDP, each datagram must contain exactly one full core-net message.
    /// Unlike TCP, there is no stream framing/reassembly here.
    #[error("header total length {header_total} does not match datagram length {datagram_len}")]
    DatagramLengthMismatch {
        /// Total length declared in the message header.
        header_total: usize,

        /// Actual datagram byte length received from the socket.
        datagram_len: usize,
    },
}

/// Internal outbound command used by UDP endpoints.
///
/// Endpoint handles enqueue these commands and the endpoint write loop
/// consumes them.
#[derive(Clone)]
pub enum UdpOutboundCommand {
    /// Send a complete core-net message to a UDP socket address.
    SendTo {
        /// Destination socket address.
        to: SocketAddr,

        /// Complete message bytes.
        payload: MessageBuf,
    },

    /// Close the endpoint.
    Close,
}

/// Parses and validates one received UDP datagram.
///
/// Expected datagram layout:
///
/// ```text
/// [MessageHeader::WIRE_SIZE bytes]
/// [payload bytes]
/// ```
///
/// Validation performed:
///
/// - datagram is large enough to contain the fixed header
/// - protocol header decodes successfully
/// - magic string matches the expected value
/// - total length is at least the header size
/// - total length exactly equals the received datagram length
///
/// Payload storage:
///
/// - uses `recv_pool` when provided and possible
/// - falls back to dynamic allocation otherwise
pub fn parse_datagram(
    datagram: &[u8],
    from: SocketAddr,
    expected_magic_string: &[u8; crate::protocol::MAGIC_STRING_LEN],
    recv_pool: Option<&BufferPool>,
) -> Result<ReceivedDatagram, UdpDatagramError> {
    // UDP datagram must at least contain the fixed protocol header.
    if datagram.len() < MessageHeader::WIRE_SIZE {
        return Err(UdpDatagramError::DatagramTooShort(datagram.len()));
    }

    // Decode and validate header.
    let header = MessageHeader::decode(&datagram[..MessageHeader::WIRE_SIZE])?;

    header.validate_against(expected_magic_string)?;

    let total_len = header.total_length as usize;

    if total_len < MessageHeader::WIRE_SIZE {
        return Err(UdpDatagramError::InvalidInboundLength(total_len));
    }

    // UDP preserves message boundaries. Therefore the length declared in the
    // header must exactly match the socket datagram length.
    if total_len != datagram.len() {
        return Err(UdpDatagramError::DatagramLengthMismatch {
            header_total: total_len,
            datagram_len: datagram.len(),
        });
    }

    let payload_len = header.payload_len()?;

    let payload_start = MessageHeader::WIRE_SIZE;
    let payload_end = payload_start + payload_len;

    // Copy payload into MessageBuf storage.
    //
    // If a receive pool is configured and available this avoids dynamic heap
    // allocation. Otherwise MessageBuf falls back to Arc<Vec<u8>>.
    let payload =
        MessageBuf::from_slice_with_pool(recv_pool, &datagram[payload_start..payload_end]);

    Ok(ReceivedDatagram {
        from,
        header,
        payload,
    })
}
