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
    pool::MessageBuf, protocol::MessageHeader, tcp_server::ReceivedMessage,
    udp_common::ReceivedDatagram,
};

use std::net::SocketAddr;

/// Transport-independent application message.
///
/// This type is the canonical message representation used by the dispatcher
/// layer.
///
/// Messages originating from:
///
/// - TCP
/// - UDP unicast
/// - UDP broadcast
/// - UDP multicast
///
/// are all normalised into this common structure before being passed to
/// application handlers.
///
/// This allows handlers and dispatch logic to remain transport-agnostic.
#[derive(Debug, Clone)]
pub struct Message {
    /// Source socket address if known.
    ///
    /// Examples:
    ///
    /// - TCP peer address
    /// - UDP sender address
    pub source_addr: Option<SocketAddr>,

    /// Parsed protocol message header.
    ///
    /// Contains:
    ///
    /// - message id
    /// - archive type
    /// - payload length
    /// - protocol metadata
    pub header: MessageHeader,

    /// Message payload buffer.
    ///
    /// Internally this uses the pooled MessageBuf abstraction to minimise
    /// allocation churn and unnecessary payload copies.
    pub payload: MessageBuf,
}

impl Message {
    /// Creates a transport-independent Message from a TCP server message.
    ///
    /// This converts the TCP-specific receive representation into the common
    /// dispatcher/application representation.
    ///
    /// Important:
    ///
    /// - payload ownership is moved, not copied
    /// - pooled payload buffer is retained
    #[allow(dead_code)]
    pub fn from_tcp_server(peer_addr: SocketAddr, message: ReceivedMessage) -> Self {
        Self {
            source_addr: Some(peer_addr),

            header: message.header,

            payload: message.payload,
        }
    }

    /// Creates a transport-independent Message from a UDP datagram.
    ///
    /// This converts the UDP-specific receive representation into the common
    /// dispatcher/application representation.
    ///
    /// Important:
    ///
    /// - payload ownership is moved, not copied
    /// - pooled payload buffer is retained
    #[allow(dead_code)]
    pub fn from_udp(datagram: ReceivedDatagram) -> Self {
        Self {
            source_addr: Some(datagram.from),

            header: datagram.header,

            payload: datagram.payload,
        }
    }
}
