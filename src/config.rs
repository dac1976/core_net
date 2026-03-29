use std::net::{Ipv4Addr, Ipv6Addr, SocketAddr};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SendOption {
    NagleOff,
    NagleOn,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KeepAliveOption {
    Off,
    On,
}

#[derive(Debug, Clone)]
pub struct TcpSocketOptions {
    pub send_option: SendOption,
    pub keep_alive: KeepAliveOption,
    pub send_buffer_size: Option<usize>,
    pub recv_buffer_size: Option<usize>,
    pub ttl: Option<u32>,
    pub reuse_address: bool,

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

#[derive(Debug, Clone)]
pub struct TcpServerConfig {
    pub min_amount_to_read: usize,
    pub max_allowed_unsent_async_messages: usize,
    pub send_pool_msg_size: usize,
    pub recv_pool_msg_count: usize,
    pub recv_pool_msg_size: usize,
    pub recv_chunk_size: usize,
    pub listen_backlog: i32,
    pub expected_magic_string: [u8; crate::protocol::MAGIC_STRING_LEN],
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

#[derive(Debug, Clone)]
pub struct TcpClientConfig {
    pub min_amount_to_read: usize,
    pub max_allowed_unsent_async_messages: usize,
    pub send_pool_msg_size: usize,
    pub recv_pool_msg_count: usize,
    pub recv_pool_msg_size: usize,
    pub recv_chunk_size: usize,
    pub expected_magic_string: [u8; crate::protocol::MAGIC_STRING_LEN],
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

#[derive(Debug, Clone)]
pub struct UdpSocketOptions {
    pub send_buffer_size: Option<usize>,
    pub recv_buffer_size: Option<usize>,
    pub ttl: Option<u32>,
    pub reuse_address: bool,
    pub broadcast: bool,

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

#[derive(Debug, Clone)]
pub struct UdpConfig {
    pub max_datagram_size: usize,
    pub max_allowed_unsent_async_messages: usize,
    pub send_pool_msg_size: usize,
    pub recv_pool_msg_count: usize,
    pub recv_pool_msg_size: usize,
    pub expected_magic_string: [u8; crate::protocol::MAGIC_STRING_LEN],
    pub socket: UdpSocketOptions,
}

impl Default for UdpConfig {
    fn default() -> Self {
        Self {
            max_datagram_size: 64 * 1024,
            max_allowed_unsent_async_messages: 100,
            send_pool_msg_size: 0,
            recv_pool_msg_count: 0,
            recv_pool_msg_size: 8192,
            expected_magic_string: crate::protocol::DEFAULT_MAGIC_STRING,
            socket: UdpSocketOptions::default(),
        }
    }
}

#[derive(Debug, Clone)]
pub enum MulticastGroup {
    V4 {
        local_bind_addr: SocketAddr,
        group_addr: Ipv4Addr,
        group_port: u16,
        interface_addr: Ipv4Addr,
    },
    V6 {
        local_bind_addr: SocketAddr,
        group_addr: Ipv6Addr,
        group_port: u16,
        interface_index: u32,
    },
}

#[derive(Debug, Clone)]
pub struct UdpMulticastConfig {
    pub group: MulticastGroup,
    pub max_datagram_size: usize,
    pub max_allowed_unsent_async_messages: usize,
    pub send_pool_msg_size: usize,
    pub recv_pool_msg_count: usize,
    pub recv_pool_msg_size: usize,
    pub expected_magic_string: [u8; crate::protocol::MAGIC_STRING_LEN],
    pub send_ttl_v4: u32,
    pub multicast_loop_v4: bool,
    pub multicast_loop_v6: bool,
    pub join_group_on_start: bool,
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
            max_datagram_size: 64 * 1024,
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
