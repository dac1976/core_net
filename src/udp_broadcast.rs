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
    config::UdpConfig,
    pool::BufferPool,
    udp_common::{ReceivedDatagram, UdpDatagramError, UdpOutboundCommand, parse_datagram},
};

use socket2::{Domain, Protocol, Socket, Type};

use std::{net::SocketAddr, sync::Arc};

use tokio::{
    net::UdpSocket,
    sync::{Mutex, mpsc},
};

use tokio_util::sync::CancellationToken;

use tracing::{error, info, instrument, warn};

/// Component name used in structured log records.
const COMPONENT: &str = "udp_broadcast";

/// Application-facing events emitted by `UdpBroadcastEndpoint`.
///
/// The endpoint owns the UDP socket and reports lifecycle/receive events to
/// application code through an async channel.
#[derive(Debug, Clone)]
pub enum UdpBroadcastEvent {
    /// UDP socket successfully bound.
    Bound {
        /// Actual local socket address.
        ///
        /// This may differ from the requested address when binding to port 0.
        local_addr: SocketAddr,
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

/// Cloneable control handle for a UDP broadcast endpoint.
///
/// Application code uses this handle to:
///
/// - send datagrams
/// - try-send without awaiting
/// - close the endpoint
/// - request shutdown
#[derive(Clone)]
pub struct UdpBroadcastHandle {
    /// Outbound command queue consumed by the endpoint write loop.
    outbound_tx: mpsc::Sender<UdpOutboundCommand>,

    /// Optional pooled send buffer storage.
    send_pool: Option<BufferPool>,

    /// Endpoint cancellation token.
    shutdown: CancellationToken,
}

impl UdpBroadcastHandle {
    /// Sends a complete core-net message to a UDP peer asynchronously.
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
        let msg =
            crate::pool::MessageBuf::from_slice_with_pool(self.send_pool.as_ref(), full_message);

        self.outbound_tx
            .send(UdpOutboundCommand::SendTo { to, payload: msg })
            .await
            .map_err(|_| UdpDatagramError::OutboundQueueClosed)
    }

    /// Attempts to enqueue a UDP send without awaiting.
    ///
    /// This is useful for low-latency paths where queue backpressure should
    /// be observed explicitly rather than by awaiting capacity.
    pub fn try_send_to(&self, to: SocketAddr, full_message: &[u8]) -> Result<(), UdpDatagramError> {
        let msg =
            crate::pool::MessageBuf::from_slice_with_pool(self.send_pool.as_ref(), full_message);

        match self
            .outbound_tx
            .try_send(UdpOutboundCommand::SendTo { to, payload: msg })
        {
            Ok(()) => Ok(()),

            Err(tokio::sync::mpsc::error::TrySendError::Full(_)) => {
                Err(UdpDatagramError::OutboundQueueFull)
            }

            Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => {
                Err(UdpDatagramError::OutboundQueueClosed)
            }
        }
    }

    /// Requests graceful endpoint close.
    pub async fn close(&self) -> Result<(), UdpDatagramError> {
        self.outbound_tx
            .send(UdpOutboundCommand::Close)
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

/// UDP broadcast endpoint.
///
/// The endpoint:
///
/// - owns one UDP socket
/// - enables broadcast socket support
/// - receives core-net framed UDP datagrams
/// - emits application events
/// - accepts outbound send commands through a handle
/// - optionally uses pooled buffers
pub struct UdpBroadcastEndpoint {
    /// Local socket bind address.
    local_addr: SocketAddr,

    /// Endpoint configuration.
    config: UdpConfig,

    /// Application event sink.
    app_event_tx: mpsc::Sender<UdpBroadcastEvent>,

    /// Endpoint shutdown token.
    shutdown: CancellationToken,

    /// Optional outbound send pool.
    send_pool: Option<BufferPool>,

    /// Outbound command sender.
    outbound_tx: mpsc::Sender<UdpOutboundCommand>,

    /// Outbound command receiver.
    ///
    /// Wrapped in Option so it can be moved into `run()` exactly once.
    outbound_rx: Arc<Mutex<Option<mpsc::Receiver<UdpOutboundCommand>>>>,
}

impl UdpBroadcastEndpoint {
    /// Creates a new UDP broadcast endpoint.
    ///
    /// Broadcast is forcibly enabled in the socket config because this type is
    /// specifically for broadcast-capable UDP operation.
    pub fn new(
        local_addr: SocketAddr,
        mut config: UdpConfig,
        app_event_tx: mpsc::Sender<UdpBroadcastEvent>,
    ) -> Self {
        // Ensure broadcast support is enabled regardless of caller input.
        config.socket.broadcast = true;

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
            mpsc::channel::<UdpOutboundCommand>(config.max_allowed_unsent_async_messages);

        Self {
            local_addr,
            config,
            app_event_tx,
            shutdown: CancellationToken::new(),
            send_pool,
            outbound_tx,
            outbound_rx: Arc::new(Mutex::new(Some(outbound_rx))),
        }
    }

    /// Returns a cloneable control handle for this endpoint.
    pub fn handle(&self) -> UdpBroadcastHandle {
        UdpBroadcastHandle {
            outbound_tx: self.outbound_tx.clone(),
            send_pool: self.send_pool.clone(),
            shutdown: self.shutdown.clone(),
        }
    }

    /// Binds the UDP socket and runs the endpoint event loops.
    #[instrument(skip(self))]
    pub async fn run(self) -> Result<(), UdpDatagramError> {
        let socket = create_udp_socket(self.local_addr, &self.config)?;

        let socket = Arc::new(UdpSocket::from_std(socket)?);

        let bound_addr = socket.local_addr()?;

        info!(
            component = COMPONENT,
            local = %bound_addr,
            "udp broadcast endpoint bound"
        );

        // Notify application that endpoint has bound successfully.
        let _ = self
            .app_event_tx
            .send(UdpBroadcastEvent::Bound {
                local_addr: bound_addr,
            })
            .await;

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
                local = %bound_addr,
                error = %err,
                "udp broadcast endpoint ended with error"
            );
        }

        // Notify application that endpoint has closed.
        let _ = self
            .app_event_tx
            .send(UdpBroadcastEvent::Closed {
                local_addr: bound_addr,
            })
            .await;

        res
    }
}

/// Runs read/write loops for the UDP broadcast endpoint.
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
    mut outbound_rx: mpsc::Receiver<UdpOutboundCommand>,
    app_event_tx: mpsc::Sender<UdpBroadcastEvent>,
    shutdown: CancellationToken,
    config: UdpConfig,
    recv_pool: Option<BufferPool>,
) -> Result<(), UdpDatagramError> {
    let recv_socket = Arc::clone(&socket);
    let send_socket = Arc::clone(&socket);

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
                        .send(UdpBroadcastEvent::DatagramReceived { datagram })
                        .await
                        .map_err(|e| {
                            error!(
                                component = COMPONENT,
                                %from,
                                error = %e,
                                "failed to forward udp broadcast app event"
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
                        Some(UdpOutboundCommand::SendTo { to, payload }) => {
                            send_socket
                                .send_to(payload.as_slice(), to)
                                .await?;
                        }

                        Some(UdpOutboundCommand::Close) | None => {
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

/// Creates and binds a UDP socket suitable for broadcast use.
///
/// `socket2` is used so options can be configured before bind.
fn create_udp_socket(
    local_addr: SocketAddr,
    config: &UdpConfig,
) -> std::io::Result<std::net::UdpSocket> {
    let domain = match local_addr {
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

    if let Some(ttl) = config.socket.ttl {
        match local_addr {
            SocketAddr::V4(_) => socket.set_ttl_v4(ttl)?,

            SocketAddr::V6(_) => socket.set_unicast_hops_v6(ttl)?,
        }
    }

    // Required for sending broadcast datagrams.
    socket.set_broadcast(true)?;

    // Tokio requires sockets to be non-blocking before conversion.
    socket.set_nonblocking(true)?;

    socket.bind(&local_addr.into())?;

    Ok(socket.into())
}
