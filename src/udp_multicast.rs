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
    config::{MulticastGroup, UdpMulticastConfig},
    pool::{BufferPool, MessageBuf},
    udp_common::{ReceivedDatagram, UdpDatagramError, parse_datagram},
};

use socket2::{Domain, Protocol, Socket, Type};

use std::{
    net::{IpAddr, SocketAddr},
    sync::Arc,
};

use tokio::{
    net::UdpSocket,
    sync::{Mutex, mpsc},
};

use tokio_util::sync::CancellationToken;

use tracing::{error, info, instrument, warn};

/// Component name used in structured log records.
const COMPONENT: &str = "udp_multicast";

/// Application-facing events emitted by `UdpMulticastEndpoint`.
///
/// The endpoint owns the UDP socket and reports lifecycle/receive events to
/// application code through an async channel.
#[derive(Debug, Clone)]
pub enum UdpMulticastEvent {
    /// UDP socket successfully bound.
    Bound {
        /// Actual local socket address.
        local_addr: SocketAddr,
    },

    /// Multicast group joined successfully.
    Joined {
        /// Local socket address.
        local_addr: SocketAddr,

        /// Human-readable multicast group endpoint string.
        group: String,
    },

    /// Complete and validated core-net datagram received.
    DatagramReceived {
        /// Parsed UDP datagram.
        datagram: ReceivedDatagram,
    },

    /// Endpoint closed.
    Closed {
        /// Local socket address that was closed.
        local_addr: SocketAddr,
    },
}

/// Internal outbound command consumed by the multicast endpoint write loop.
#[derive(Clone)]
enum OutboundCommand {
    /// Send a complete core-net message to a specific UDP peer.
    SendTo { to: SocketAddr, payload: MessageBuf },

    /// Send a complete core-net message to the configured multicast group.
    SendToGroup { payload: MessageBuf },

    /// Close the endpoint.
    Close,
}

/// Cloneable control handle for a UDP multicast endpoint.
///
/// Application code uses this handle to:
///
/// - send to a specific peer
/// - send to the configured multicast group
/// - close the endpoint
/// - request shutdown
#[derive(Clone)]
pub struct UdpMulticastHandle {
    /// Outbound command queue consumed by the endpoint write loop.
    outbound_tx: mpsc::Sender<OutboundCommand>,

    /// Optional pooled send buffer storage.
    send_pool: Option<BufferPool>,

    /// Endpoint cancellation token.
    shutdown: CancellationToken,
}

impl UdpMulticastHandle {
    /// Sends a complete core-net message to a specific UDP peer.
    ///
    /// `full_message` must already contain:
    ///
    /// - core-net protocol header
    /// - payload
    ///
    /// This method awaits if the outbound queue is full.
    #[instrument(skip(self, full_message), fields(to = %to, len = full_message.len()))]
    pub async fn send_to_async(
        &self,
        to: SocketAddr,
        full_message: &[u8],
    ) -> Result<(), UdpDatagramError> {
        let msg = MessageBuf::from_slice_with_pool(self.send_pool.as_ref(), full_message);

        self.outbound_tx
            .send(OutboundCommand::SendTo { to, payload: msg })
            .await
            .map_err(|_| UdpDatagramError::OutboundQueueClosed)
    }

    /// Sends a complete core-net message to the configured multicast group.
    ///
    /// The destination group address and port come from `UdpMulticastConfig`.
    #[instrument(skip(self, full_message), fields(len = full_message.len()))]
    pub async fn send_to_group_async(&self, full_message: &[u8]) -> Result<(), UdpDatagramError> {
        let msg = MessageBuf::from_slice_with_pool(self.send_pool.as_ref(), full_message);

        self.outbound_tx
            .send(OutboundCommand::SendToGroup { payload: msg })
            .await
            .map_err(|_| UdpDatagramError::OutboundQueueClosed)
    }

    /// Requests graceful endpoint close.
    pub async fn close(&self) -> Result<(), UdpDatagramError> {
        self.outbound_tx
            .send(OutboundCommand::Close)
            .await
            .map_err(|_| UdpDatagramError::OutboundQueueClosed)
    }

    /// Cancels the endpoint run loop.
    ///
    /// This is a broader shutdown signal than `close()`.
    pub fn shutdown(&self) {
        self.shutdown.cancel();
    }
}

/// UDP multicast endpoint.
///
/// The endpoint:
///
/// - owns one UDP socket
/// - optionally joins a multicast group
/// - receives core-net framed UDP datagrams
/// - emits application events
/// - accepts outbound send commands through a handle
/// - optionally uses pooled buffers
pub struct UdpMulticastEndpoint {
    /// Endpoint configuration.
    config: UdpMulticastConfig,

    /// Application event sink.
    app_event_tx: mpsc::Sender<UdpMulticastEvent>,

    /// Endpoint shutdown token.
    shutdown: CancellationToken,

    /// Optional outbound send pool.
    send_pool: Option<BufferPool>,

    /// Outbound command sender.
    outbound_tx: mpsc::Sender<OutboundCommand>,

    /// Outbound command receiver.
    ///
    /// Wrapped in Option so it can be moved into `run()` exactly once.
    outbound_rx: Arc<Mutex<Option<mpsc::Receiver<OutboundCommand>>>>,
}

impl UdpMulticastEndpoint {
    /// Creates a new UDP multicast endpoint.
    ///
    /// The socket is not created until `run()` is called.
    pub fn new(config: UdpMulticastConfig, app_event_tx: mpsc::Sender<UdpMulticastEvent>) -> Self {
        let send_pool = if config.send_pool_msg_size > 0 {
            Some(BufferPool::new(
                config.send_pool_msg_size,
                config.max_allowed_unsent_async_messages.max(1),
            ))
        } else {
            None
        };

        // Bounded outbound queue.
        //
        // This prevents unbounded memory growth if sends are produced faster
        // than the socket can transmit them.
        let (outbound_tx, outbound_rx) =
            mpsc::channel::<OutboundCommand>(config.max_allowed_unsent_async_messages);

        Self {
            config,
            app_event_tx,
            shutdown: CancellationToken::new(),
            send_pool,
            outbound_tx,
            outbound_rx: Arc::new(Mutex::new(Some(outbound_rx))),
        }
    }

    /// Returns a cloneable control handle for this endpoint.
    pub fn handle(&self) -> UdpMulticastHandle {
        UdpMulticastHandle {
            outbound_tx: self.outbound_tx.clone(),
            send_pool: self.send_pool.clone(),
            shutdown: self.shutdown.clone(),
        }
    }

    /// Creates/binds the socket, optionally joins the multicast group, and
    /// runs the endpoint event loops.
    #[instrument(skip(self))]
    pub async fn run(self) -> Result<(), UdpDatagramError> {
        let std_socket = create_multicast_socket(&self.config)?;

        let socket = Arc::new(UdpSocket::from_std(std_socket)?);

        // Join multicast group before publishing the Bound/Joined events
        // when configured to do so.
        if self.config.join_group_on_start {
            apply_multicast_membership(&socket, &self.config)?;
        }

        let local_addr = socket.local_addr()?;

        let group_string = match &self.config.group {
            MulticastGroup::V4 {
                group_addr,
                group_port,
                ..
            } => {
                format!("{group_addr}:{group_port}")
            }

            MulticastGroup::V6 {
                group_addr,
                group_port,
                ..
            } => {
                format!("[{group_addr}]:{group_port}")
            }
        };

        // Notify application that the socket has bound.
        let _ = self
            .app_event_tx
            .send(UdpMulticastEvent::Bound { local_addr })
            .await;

        if self.config.join_group_on_start {
            info!(
                component = COMPONENT,
                local = %local_addr,
                group = %group_string,
                "udp multicast endpoint bound and joined"
            );

            // Notify application that group membership is active.
            let _ = self
                .app_event_tx
                .send(UdpMulticastEvent::Joined {
                    local_addr,
                    group: group_string.clone(),
                })
                .await;
        } else {
            info!(
                component = COMPONENT,
                local = %local_addr,
                group = %group_string,
                "udp multicast endpoint bound without joining group"
            );
        }

        // Optional receive pool.
        let recv_pool = if self.config.recv_pool_msg_count > 0 {
            Some(BufferPool::new(
                self.config.recv_pool_msg_size,
                self.config.recv_pool_msg_count,
            ))
        } else {
            None
        };

        // Move outbound receiver into the running endpoint.
        //
        // This prevents multiple run loops from consuming the same queue.
        let mut rx_guard = self.outbound_rx.lock().await;

        let outbound_rx = rx_guard
            .take()
            .ok_or(UdpDatagramError::ReceiverAlreadyTaken)?;

        drop(rx_guard);

        let res = run_endpoint(
            socket,
            outbound_rx,
            self.app_event_tx.clone(),
            self.shutdown.clone(),
            self.config.clone(),
            recv_pool,
        )
        .await;

        if let Err(err) = &res {
            warn!(
                component = COMPONENT,
                local = %local_addr,
                group = %group_string,
                error = %err,
                "udp multicast endpoint ended with error"
            );
        }

        // Notify application that endpoint has closed.
        let _ = self
            .app_event_tx
            .send(UdpMulticastEvent::Closed { local_addr })
            .await;

        res
    }
}

/// Runs read/write loops for the UDP multicast endpoint.
///
/// The socket is shared between:
///
/// ```text
/// read loop  -> socket to app events
/// write loop -> outbound queue to socket
/// ```
///
/// Whichever side exits first cancels the other.
#[instrument(skip(socket, outbound_rx, app_event_tx, shutdown, config, recv_pool))]
async fn run_endpoint(
    socket: Arc<UdpSocket>,
    mut outbound_rx: mpsc::Receiver<OutboundCommand>,
    app_event_tx: mpsc::Sender<UdpMulticastEvent>,
    shutdown: CancellationToken,
    config: UdpMulticastConfig,
    recv_pool: Option<BufferPool>,
) -> Result<(), UdpDatagramError> {
    let recv_socket = Arc::clone(&socket);
    let send_socket = Arc::clone(&socket);

    // Precompute multicast group target used by SendToGroup.
    let group_target = match &config.group {
        MulticastGroup::V4 {
            group_addr,
            group_port,
            ..
        } => SocketAddr::new(IpAddr::V4(*group_addr), *group_port),

        MulticastGroup::V6 {
            group_addr,
            group_port,
            ..
        } => SocketAddr::new(IpAddr::V6(*group_addr), *group_port),
    };

    // Receive loop.
    let read_task = async {
        let mut buf = vec![
            0u8;
            config
                .max_datagram_size
                .max(crate::protocol::MessageHeader::WIRE_SIZE)
        ];

        loop {
            tokio::select! {
                _ = shutdown.cancelled() => {
                    return Ok::<(), UdpDatagramError>(());
                }

                res = recv_socket.recv_from(&mut buf) => {
                    let (len, from) = res?;

                    // Parse and validate the received core-net datagram.
                    //
                    // Payload storage uses recv_pool when possible.
                    let datagram = parse_datagram(
                        &buf[..len],
                        from,
                        &config.expected_magic_string,
                        recv_pool.as_ref(),
                    )?;

                    // Forward received datagram to application.
                    app_event_tx
                        .send(UdpMulticastEvent::DatagramReceived { datagram })
                        .await
                        .map_err(|e| {
                            error!(
                                component = COMPONENT,
                                %from,
                                error = %e,
                                "failed to forward udp multicast app event"
                            );

                            std::io::Error::new(
                                std::io::ErrorKind::BrokenPipe,
                                e,
                            )
                        })?;
                }
            }
        }
    };

    // Send loop.
    let write_task = async {
        loop {
            tokio::select! {
                _ = shutdown.cancelled() => {
                    return Ok::<(), UdpDatagramError>(());
                }

                cmd = outbound_rx.recv() => {
                    match cmd {
                        Some(OutboundCommand::SendTo { to, payload }) => {
                            send_socket
                                .send_to(payload.as_slice(), to)
                                .await?;
                        }

                        Some(OutboundCommand::SendToGroup { payload }) => {
                            send_socket
                                .send_to(payload.as_slice(), group_target)
                                .await?;
                        }

                        Some(OutboundCommand::Close) | None => {
                            return Ok(());
                        }
                    }
                }
            }
        }
    };

    // Race receive/send loops.
    //
    // If either exits, cancel the whole endpoint.
    tokio::select! {
        res = read_task => {
            shutdown.cancel();

            res?;
        }

        res = write_task => {
            shutdown.cancel();

            res?;
        }
    }

    Ok(())
}

/// Creates and binds a UDP socket suitable for multicast use.
///
/// `socket2` is used so socket options can be configured before bind.
fn create_multicast_socket(config: &UdpMulticastConfig) -> std::io::Result<std::net::UdpSocket> {
    let bind_addr = match &config.group {
        MulticastGroup::V4 {
            local_bind_addr, ..
        } => *local_bind_addr,

        MulticastGroup::V6 {
            local_bind_addr, ..
        } => *local_bind_addr,
    };

    let domain = match bind_addr {
        SocketAddr::V4(_) => Domain::IPV4,
        SocketAddr::V6(_) => Domain::IPV6,
    };

    let socket = Socket::new(domain, Type::DGRAM, Some(Protocol::UDP))?;

    socket.set_reuse_address(config.socket.reuse_address)?;

    #[cfg(any(target_os = "linux", target_os = "android"))]
    socket.set_reuse_port(config.socket.reuse_port)?;

    if let Some(size) = config.socket.send_buffer_size {
        socket.set_send_buffer_size(size)?;
    }

    if let Some(size) = config.socket.recv_buffer_size {
        socket.set_recv_buffer_size(size)?;
    }

    // Tokio requires sockets to be non-blocking before conversion.
    socket.set_nonblocking(true)?;

    socket.bind(&bind_addr.into())?;

    Ok(socket.into())
}

/// Applies multicast-specific socket options and joins the configured group.
///
/// For IPv4 this configures:
///
/// - group membership
/// - multicast loopback
/// - multicast TTL
///
/// For IPv6 this configures:
///
/// - group membership
/// - multicast loopback
fn apply_multicast_membership(
    socket: &UdpSocket,
    config: &UdpMulticastConfig,
) -> std::io::Result<()> {
    match &config.group {
        MulticastGroup::V4 {
            group_addr,
            interface_addr,
            ..
        } => {
            socket.join_multicast_v4(*group_addr, *interface_addr)?;

            socket.set_multicast_loop_v4(config.multicast_loop_v4)?;

            socket.set_multicast_ttl_v4(config.send_ttl_v4)?;
        }

        MulticastGroup::V6 {
            group_addr,
            interface_index,
            ..
        } => {
            socket.join_multicast_v6(group_addr, *interface_index)?;

            socket.set_multicast_loop_v6(config.multicast_loop_v6)?;
        }
    }

    Ok(())
}
