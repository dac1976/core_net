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

use std::net::{Ipv4Addr, Ipv6Addr, SocketAddr};

/// TCP send behaviour.
///
/// This controls whether TCP_NODELAY is enabled.
///
/// `NagleOff` is usually preferred for low-latency request/reply protocols
/// with small messages.
///
/// `NagleOn` may be preferable for throughput-oriented streams where packet
/// coalescing is acceptable.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SendOption {
    /// Disable Nagle's algorithm.
    ///
    /// Lower latency for small messages.
    NagleOff,

    /// Enable/default Nagle behaviour.
    ///
    /// May reduce packet count by coalescing small writes.
    NagleOn,
}

/// TCP keepalive behaviour.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KeepAliveOption {
    /// Disable TCP keepalive.
    Off,

    /// Enable TCP keepalive.
    On,
}

/// Common TCP socket options used by both TCP client and TCP server.
#[derive(Debug, Clone)]
pub struct TcpSocketOptions {
    /// TCP send behaviour.
    pub send_option: SendOption,

    /// TCP keepalive behaviour.
    pub keep_alive: KeepAliveOption,

    /// Optional OS socket send buffer size.
    ///
    /// `None` leaves the OS default unchanged.
    pub send_buffer_size: Option<usize>,

    /// Optional OS socket receive buffer size.
    ///
    /// `None` leaves the OS default unchanged.
    pub recv_buffer_size: Option<usize>,

    /// Optional IP time-to-live / hop limit.
    pub ttl: Option<u32>,

    /// Enable SO_REUSEADDR where supported.
    pub reuse_address: bool,

    /// Enable SO_REUSEPORT on Linux/Android.
    ///
    /// Not available on all platforms, hence the cfg gate.
    #[cfg(any(target_os = "linux", target_os = "android"))]
    pub reuse_port: bool,
}

impl Default for TcpSocketOptions {
    fn default() -> Self {
        Self {
            send_option: SendOption::NagleOn,
            keep_alive: KeepAliveOption::Off,
            send_buffer_size: None,
            recv_buffer_size: None,
            ttl: None,
            reuse_address: true,

            #[cfg(any(target_os = "linux", target_os = "android"))]
            reuse_port: false,
        }
    }
}

/// Runtime configuration for a TCP server endpoint.
#[derive(Debug, Clone)]
pub struct TcpServerConfig {
    /// Minimum amount of data required before attempting message parsing.
    ///
    /// Usually the wire header size.
    pub min_amount_to_read: usize,

    /// Maximum number of unsent outbound async messages allowed.
    ///
    /// Protects the process from unbounded queue growth if peers are slow.
    pub max_allowed_unsent_async_messages: usize,

    /// Size of pooled outbound message buffers.
    ///
    /// `0` disables outbound pooling/falls back to dynamic allocation,
    /// depending on the pool implementation.
    pub send_pool_msg_size: usize,

    /// Number of pooled receive message buffers.
    ///
    /// `0` disables preallocated receive pool usage.
    pub recv_pool_msg_count: usize,

    /// Size of each pooled receive message buffer.
    pub recv_pool_msg_size: usize,

    /// Socket read chunk size.
    ///
    /// Controls how much data is read from TCP per receive operation.
    pub recv_chunk_size: usize,

    /// TCP listen backlog.
    pub listen_backlog: i32,

    /// Expected protocol magic string.
    ///
    /// Incoming messages must match this value.
    pub expected_magic_string: [u8; crate::protocol::MAGIC_STRING_LEN],

    /// Low-level TCP socket options.
    pub socket: TcpSocketOptions,
}

impl Default for TcpServerConfig {
    fn default() -> Self {
        Self {
            min_amount_to_read: crate::protocol::MessageHeader::WIRE_SIZE,
            max_allowed_unsent_async_messages: 100,
            send_pool_msg_size: 0,
            recv_pool_msg_count: 0,
            recv_pool_msg_size: 8192,
            recv_chunk_size: 64 * 1024,
            listen_backlog: 1024,
            expected_magic_string: crate::protocol::DEFAULT_MAGIC_STRING,
            socket: TcpSocketOptions::default(),
        }
    }
}

/// Runtime configuration for a TCP client endpoint.
#[derive(Debug, Clone)]
pub struct TcpClientConfig {
    /// Minimum amount of data required before attempting message parsing.
    pub min_amount_to_read: usize,

    /// Maximum number of unsent outbound async messages allowed.
    pub max_allowed_unsent_async_messages: usize,

    /// Size of pooled outbound message buffers.
    pub send_pool_msg_size: usize,

    /// Number of pooled receive message buffers.
    pub recv_pool_msg_count: usize,

    /// Size of each pooled receive message buffer.
    pub recv_pool_msg_size: usize,

    /// Socket read chunk size.
    pub recv_chunk_size: usize,

    /// Expected protocol magic string.
    pub expected_magic_string: [u8; crate::protocol::MAGIC_STRING_LEN],

    /// Low-level TCP socket options.
    pub socket: TcpSocketOptions,
}

impl Default for TcpClientConfig {
    fn default() -> Self {
        Self {
            min_amount_to_read: crate::protocol::MessageHeader::WIRE_SIZE,
            max_allowed_unsent_async_messages: 100,
            send_pool_msg_size: 0,
            recv_pool_msg_count: 0,
            recv_pool_msg_size: 8192,
            recv_chunk_size: 64 * 1024,
            expected_magic_string: crate::protocol::DEFAULT_MAGIC_STRING,
            socket: TcpSocketOptions::default(),
        }
    }
}

/// Common UDP socket options used by unicast, broadcast and multicast endpoints.
#[derive(Debug, Clone)]
pub struct UdpSocketOptions {
    /// Optional OS socket send buffer size.
    pub send_buffer_size: Option<usize>,

    /// Optional OS socket receive buffer size.
    pub recv_buffer_size: Option<usize>,

    /// Optional IP TTL / hop limit.
    pub ttl: Option<u32>,

    /// Enable SO_REUSEADDR where supported.
    pub reuse_address: bool,

    /// Enable SO_BROADCAST.
    ///
    /// Required for sending to broadcast addresses.
    pub broadcast: bool,

    /// Enable SO_REUSEPORT on Linux/Android.
    #[cfg(any(target_os = "linux", target_os = "android"))]
    pub reuse_port: bool,
}

impl Default for UdpSocketOptions {
    fn default() -> Self {
        Self {
            send_buffer_size: None,
            recv_buffer_size: None,
            ttl: None,
            reuse_address: true,
            broadcast: false,

            #[cfg(any(target_os = "linux", target_os = "android"))]
            reuse_port: false,
        }
    }
}

/// Runtime configuration for UDP unicast/broadcast endpoints.
#[derive(Debug, Clone)]
pub struct UdpConfig {
    /// Maximum datagram payload size accepted/sent by the endpoint.
    ///
    /// 65507 is the practical maximum UDP payload size for IPv4.
    pub max_datagram_size: usize,

    /// Maximum number of queued outbound async messages.
    pub max_allowed_unsent_async_messages: usize,

    /// Size of pooled outbound message buffers.
    pub send_pool_msg_size: usize,

    /// Number of pooled receive buffers.
    pub recv_pool_msg_count: usize,

    /// Size of each pooled receive buffer.
    pub recv_pool_msg_size: usize,

    /// Expected protocol magic string.
    pub expected_magic_string: [u8; crate::protocol::MAGIC_STRING_LEN],

    /// Low-level UDP socket options.
    pub socket: UdpSocketOptions,
}

impl Default for UdpConfig {
    fn default() -> Self {
        Self {
            max_datagram_size: 65507,
            max_allowed_unsent_async_messages: 100,
            send_pool_msg_size: 0,
            recv_pool_msg_count: 0,
            recv_pool_msg_size: 8192,
            expected_magic_string: crate::protocol::DEFAULT_MAGIC_STRING,
            socket: UdpSocketOptions::default(),
        }
    }
}

/// Multicast group configuration.
///
/// Supports IPv4 and IPv6 multicast addressing.
#[derive(Debug, Clone)]
pub enum MulticastGroup {
    /// IPv4 multicast group.
    V4 {
        /// Local socket bind address.
        local_bind_addr: SocketAddr,

        /// IPv4 multicast group address.
        group_addr: Ipv4Addr,

        /// Multicast group UDP port.
        group_port: u16,

        /// Local interface address used for group join/send.
        ///
        /// `Ipv4Addr::UNSPECIFIED` lets the OS choose.
        interface_addr: Ipv4Addr,
    },

    /// IPv6 multicast group.
    V6 {
        /// Local socket bind address.
        local_bind_addr: SocketAddr,

        /// IPv6 multicast group address.
        group_addr: Ipv6Addr,

        /// Multicast group UDP port.
        group_port: u16,

        /// Local interface index used for group join/send.
        interface_index: u32,
    },
}

/// Runtime configuration for UDP multicast endpoints.
#[derive(Debug, Clone)]
pub struct UdpMulticastConfig {
    /// Multicast group/bind configuration.
    pub group: MulticastGroup,

    /// Maximum datagram payload size.
    pub max_datagram_size: usize,

    /// Maximum number of queued outbound async messages.
    pub max_allowed_unsent_async_messages: usize,

    /// Size of pooled outbound message buffers.
    pub send_pool_msg_size: usize,

    /// Number of pooled receive buffers.
    pub recv_pool_msg_count: usize,

    /// Size of each pooled receive buffer.
    pub recv_pool_msg_size: usize,

    /// Expected protocol magic string.
    pub expected_magic_string: [u8; crate::protocol::MAGIC_STRING_LEN],

    /// IPv4 multicast TTL.
    ///
    /// TTL=1 keeps multicast traffic on the local subnet.
    pub send_ttl_v4: u32,

    /// Enable IPv4 multicast loopback.
    pub multicast_loop_v4: bool,

    /// Enable IPv6 multicast loopback.
    pub multicast_loop_v6: bool,

    /// Join multicast group automatically when endpoint starts.
    pub join_group_on_start: bool,

    /// Low-level UDP socket options.
    pub socket: UdpSocketOptions,
}

impl Default for UdpMulticastConfig {
    fn default() -> Self {
        Self {
            group: MulticastGroup::V4 {
                local_bind_addr: "0.0.0.0:9300".parse().unwrap(),
                group_addr: Ipv4Addr::new(239, 255, 0, 1),
                group_port: 9300,
                interface_addr: Ipv4Addr::UNSPECIFIED,
            },
            max_datagram_size: 65507,
            max_allowed_unsent_async_messages: 100,
            send_pool_msg_size: 0,
            recv_pool_msg_count: 0,
            recv_pool_msg_size: 8192,
            expected_magic_string: crate::protocol::DEFAULT_MAGIC_STRING,
            send_ttl_v4: 1,
            multicast_loop_v4: true,
            multicast_loop_v6: true,
            join_group_on_start: true,
            socket: UdpSocketOptions::default(),
        }
    }
}
