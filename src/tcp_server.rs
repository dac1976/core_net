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
    config::{KeepAliveOption, SendOption, TcpServerConfig},
    pool::{BufferPool, MessageBuf},
    protocol::{MessageHeader, ProtocolError},
};

use socket2::{Domain, Protocol, SockRef, Socket, Type};

use std::{
    collections::HashMap,
    net::SocketAddr,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};

use thiserror::Error;

use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::{RwLock, mpsc},
};

use tokio_util::sync::CancellationToken;

use tracing::{debug, error, info, instrument, warn};

/// Component name used in structured log records.
const COMPONENT: &str = "tcp_server";

/// Unique identifier assigned to each connected TCP client.
///
/// IDs are generated monotonically by the server accept loop.
pub type ClientId = u64;

/// Complete protocol message received from a TCP client.
///
/// This is the server-side transport-specific received-message type.
/// Higher-level code can convert this into the generic dispatcher
/// `messaging::message::Message` type when required.
#[derive(Debug, Clone)]
pub struct ReceivedMessage {
    /// Parsed core-net protocol header.
    pub header: MessageHeader,

    /// Message payload bytes.
    ///
    /// Backed by `MessageBuf`, so the storage may be pooled or dynamic.
    pub payload: MessageBuf,
}

/// Application-facing events emitted by `TcpServer`.
///
/// The server owns all low-level socket work and reports connection/message
/// lifecycle events through an async channel.
#[derive(Debug, Clone)]
pub enum AppEvent {
    /// A new TCP client connected.
    ClientConnected {
        /// Server-assigned client ID.
        client_id: ClientId,

        /// Client socket address.
        peer_addr: SocketAddr,
    },

    /// A TCP client disconnected.
    ClientDisconnected {
        /// Server-assigned client ID.
        client_id: ClientId,

        /// Client socket address.
        peer_addr: SocketAddr,
    },

    /// A complete protocol message was received from a client.
    MessageReceived {
        /// Server-assigned client ID.
        client_id: ClientId,

        /// Client socket address.
        peer_addr: SocketAddr,

        /// Received message.
        message: ReceivedMessage,
    },
}

/// Errors that can occur in the TCP server.
#[derive(Debug, Error)]
pub enum ServerError {
    /// Underlying socket/IO failure.
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    /// Protocol decode/validation failure.
    #[error("protocol error: {0}")]
    Protocol(#[from] ProtocolError),

    /// Requested client ID does not exist in the connection registry.
    #[error("client not found: {0}")]
    ClientNotFound(ClientId),

    /// Non-blocking outbound send failed because the client's queue is full.
    #[error("outbound queue full for client: {0}")]
    OutboundQueueFull(ClientId),

    /// Client outbound queue is closed.
    #[error("outbound queue closed for client: {0}")]
    OutboundQueueClosed(ClientId),

    /// Invalid inbound message length detected.
    #[error("invalid inbound message length: {0}")]
    InvalidInboundLength(usize),
}

/// Internal handle for a connected client.
///
/// Stored in the server registry and used by `TcpServerHandle` to enqueue
/// outbound commands to a specific client connection task.
#[derive(Clone)]
struct ClientHandle {
    outbound_tx: mpsc::Sender<OutboundCommand>,
}

/// Internal outbound command sent to a client writer task.
#[derive(Clone)]
enum OutboundCommand {
    /// Send a complete, already-framed core-net message.
    Send(MessageBuf),

    /// Gracefully close this client connection.
    Close,
}

/// Cloneable server control handle.
///
/// Application code can use this handle to:
///
/// - send to a specific client
/// - try-send without awaiting
/// - broadcast to all clients
/// - disconnect a client
/// - query connected clients
/// - request server shutdown
#[derive(Clone)]
pub struct TcpServerHandle {
    /// Registry of active client connections.
    registry: Arc<RwLock<HashMap<ClientId, ClientHandle>>>,

    /// Optional pooled send buffer storage.
    send_pool: Option<BufferPool>,

    /// Server-wide shutdown token.
    shutdown: CancellationToken,
}

impl TcpServerHandle {
    /// Sends a complete core-net message to a specific client asynchronously.
    ///
    /// `full_message` must already contain:
    ///
    /// - protocol header
    /// - payload
    ///
    /// This method awaits if the client's outbound queue is full.
    #[instrument(skip(self, full_message), fields(client_id, len = full_message.len()))]
    pub async fn send_to_async(
        &self,
        client_id: ClientId,
        full_message: &[u8],
    ) -> Result<(), ServerError> {
        let handle = {
            let guard = self.registry.read().await;

            guard
                .get(&client_id)
                .cloned()
                .ok_or(ServerError::ClientNotFound(client_id))?
        };

        let msg = MessageBuf::from_slice_with_pool(self.send_pool.as_ref(), full_message);

        handle
            .outbound_tx
            .send(OutboundCommand::Send(msg))
            .await
            .map_err(|_| ServerError::OutboundQueueClosed(client_id))
    }

    /// Attempts to enqueue a message to a specific client without awaiting.
    ///
    /// Useful for latency-sensitive or real-time-ish paths where awaiting
    /// queue capacity is undesirable.
    pub async fn try_send_to(
        &self,
        client_id: ClientId,
        full_message: &[u8],
    ) -> Result<(), ServerError> {
        let handle = {
            let guard = self.registry.read().await;

            guard
                .get(&client_id)
                .cloned()
                .ok_or(ServerError::ClientNotFound(client_id))?
        };

        let msg = MessageBuf::from_slice_with_pool(self.send_pool.as_ref(), full_message);

        match handle.outbound_tx.try_send(OutboundCommand::Send(msg)) {
            Ok(()) => Ok(()),

            Err(tokio::sync::mpsc::error::TrySendError::Full(_)) => {
                Err(ServerError::OutboundQueueFull(client_id))
            }

            Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => {
                Err(ServerError::OutboundQueueClosed(client_id))
            }
        }
    }

    /// Attempts to broadcast a complete message to all currently-connected
    /// clients.
    ///
    /// This method intentionally uses `try_send` for each client so a slow or
    /// stalled client cannot block the entire broadcast operation.
    ///
    /// Dropped broadcasts are logged at debug level.
    pub async fn broadcast(&self, full_message: &[u8]) {
        let handles: Vec<(ClientId, ClientHandle)> = {
            let guard = self.registry.read().await;

            guard.iter().map(|(id, h)| (*id, h.clone())).collect()
        };

        for (client_id, handle) in handles {
            let msg = MessageBuf::from_slice_with_pool(self.send_pool.as_ref(), full_message);

            let _ = handle
                .outbound_tx
                .try_send(OutboundCommand::Send(msg))
                .map_err(|_| {
                    debug!(
                        component = COMPONENT,
                        client_id, "broadcast dropped because outbound queue is full or closed"
                    );
                });
        }
    }

    /// Requests graceful disconnect of a specific client.
    pub async fn disconnect(&self, client_id: ClientId) -> Result<(), ServerError> {
        let handle = {
            let guard = self.registry.read().await;

            guard
                .get(&client_id)
                .cloned()
                .ok_or(ServerError::ClientNotFound(client_id))?
        };

        handle
            .outbound_tx
            .send(OutboundCommand::Close)
            .await
            .map_err(|_| ServerError::OutboundQueueClosed(client_id))
    }

    /// Returns the current number of connected clients.
    pub async fn number_of_clients(&self) -> usize {
        self.registry.read().await.len()
    }

    /// Returns true if the given client ID is currently connected.
    pub async fn is_connected(&self, client_id: ClientId) -> bool {
        self.registry.read().await.contains_key(&client_id)
    }

    /// Requests server shutdown.
    pub fn shutdown(&self) {
        self.shutdown.cancel();
    }
}

/// Asynchronous TCP server.
///
/// The server:
///
/// - binds a listening socket
/// - accepts multiple clients
/// - assigns each client a unique ID
/// - emits application events
/// - supports per-client send queues
/// - supports optional pooled buffers
pub struct TcpServer {
    listener: TcpListener,
    config: TcpServerConfig,
    registry: Arc<RwLock<HashMap<ClientId, ClientHandle>>>,
    next_client_id: Arc<AtomicU64>,
    app_event_tx: mpsc::Sender<AppEvent>,
    shutdown: CancellationToken,
    send_pool: Option<BufferPool>,
}

impl TcpServer {
    /// Binds a new TCP server to the supplied socket address.
    ///
    /// The returned server does not start accepting clients until `run()` is
    /// awaited.
    pub async fn bind(
        addr: SocketAddr,
        config: TcpServerConfig,
        app_event_tx: mpsc::Sender<AppEvent>,
    ) -> Result<Self, ServerError> {
        let listener = create_listener(addr, &config)?;

        let send_pool = if config.send_pool_msg_size > 0 {
            Some(BufferPool::new(
                config.send_pool_msg_size,
                config.max_allowed_unsent_async_messages.max(1),
            ))
        } else {
            None
        };

        Ok(Self {
            listener,
            config,
            registry: Arc::new(RwLock::new(HashMap::new())),
            next_client_id: Arc::new(AtomicU64::new(1)),
            app_event_tx,
            shutdown: CancellationToken::new(),
            send_pool,
        })
    }

    /// Returns a cloneable server handle for application/control code.
    pub fn handle(&self) -> TcpServerHandle {
        TcpServerHandle {
            registry: Arc::clone(&self.registry),
            send_pool: self.send_pool.clone(),
            shutdown: self.shutdown.clone(),
        }
    }

    /// Runs the TCP accept loop.
    ///
    /// Each accepted client gets its own connection task.
    #[instrument(skip(self))]
    pub async fn run(self) -> Result<(), ServerError> {
        info!(component = COMPONENT, "accept loop starting");

        loop {
            tokio::select! {
                _ = self.shutdown.cancelled() => {
                    info!(component = COMPONENT, "shutdown requested");

                    break;
                }

                accept_res = self.listener.accept() => {
                    let (stream, peer_addr) = accept_res?;

                    apply_stream_options(&stream, &self.config)?;

                    let client_id =
                        self.next_client_id.fetch_add(1, Ordering::Relaxed);

                    let (outbound_tx, outbound_rx) =
                        mpsc::channel::<OutboundCommand>(
                            self.config.max_allowed_unsent_async_messages,
                        );

                    // Register client before publishing ClientConnected so
                    // application code can immediately send replies/messages.
                    {
                        let mut guard = self.registry.write().await;

                        guard.insert(client_id, ClientHandle { outbound_tx });
                    }

                    info!(
                        component = COMPONENT,
                        client_id,
                        %peer_addr,
                        "client connected"
                    );

                    let _ = self
                        .app_event_tx
                        .send(AppEvent::ClientConnected {
                            client_id,
                            peer_addr,
                        })
                        .await;

                    let registry = Arc::clone(&self.registry);
                    let app_event_tx = self.app_event_tx.clone();

                    // Child token is cancelled when server shutdown occurs.
                    let shutdown = self.shutdown.child_token();

                    let config = self.config.clone();

                    let recv_pool = if config.recv_pool_msg_count > 0 {
                        Some(BufferPool::new(
                            config.recv_pool_msg_size,
                            config.recv_pool_msg_count,
                        ))
                    } else {
                        None
                    };

                    tokio::spawn(async move {
                        let res = run_connection(
                            client_id,
                            peer_addr,
                            stream,
                            outbound_rx,
                            app_event_tx.clone(),
                            shutdown.clone(),
                            config,
                            recv_pool,
                        )
                        .await;

                        if let Err(err) = res {
                            warn!(
                                component = COMPONENT,
                                client_id,
                                %peer_addr,
                                error = %err,
                                "client connection ended with error"
                            );
                        }

                        // Remove client from registry after task ends.
                        {
                            let mut guard = registry.write().await;

                            guard.remove(&client_id);
                        }

                        info!(
                            component = COMPONENT,
                            client_id,
                            %peer_addr,
                            "client disconnected"
                        );

                        let _ = app_event_tx
                            .send(AppEvent::ClientDisconnected {
                                client_id,
                                peer_addr,
                            })
                            .await;
                    });
                }
            }
        }

        Ok(())
    }
}

/// Runs read/write loops for one connected TCP client.
///
/// The stream is split into independent halves:
///
/// ```text
/// read loop  -> socket to AppEvent::MessageReceived
/// write loop -> outbound queue to socket
/// ```
///
/// Whichever loop exits first cancels the other.
#[instrument(
    skip(stream, outbound_rx, app_event_tx, shutdown, config, recv_pool),
    fields(client_id, %peer_addr)
)]
async fn run_connection(
    client_id: ClientId,
    peer_addr: SocketAddr,
    stream: TcpStream,
    mut outbound_rx: mpsc::Receiver<OutboundCommand>,
    app_event_tx: mpsc::Sender<AppEvent>,
    shutdown: CancellationToken,
    config: TcpServerConfig,
    recv_pool: Option<BufferPool>,
) -> Result<(), ServerError> {
    let (mut reader, mut writer) = stream.into_split();

    // Reused per-connection scratch buffers.
    //
    // These avoid repeated allocation while parsing incoming messages.
    let mut header_buf = vec![0u8; config.min_amount_to_read];

    let mut chunk_buf = vec![0u8; config.recv_chunk_size.max(1)];

    let mut payload_scratch = Vec::<u8>::new();

    // Socket read loop.
    let read_task = async {
        loop {
            tokio::select! {
                _ = shutdown.cancelled() => {
                    return Ok::<(), ServerError>(());
                }

                res = read_one_message(
                    &mut reader,
                    client_id,
                    peer_addr,
                    &app_event_tx,
                    &config,
                    recv_pool.as_ref(),
                    &mut header_buf,
                    &mut chunk_buf,
                    &mut payload_scratch,
                ) => {
                    res?;
                }
            }
        }
    };

    // Socket write loop.
    let write_task = async {
        loop {
            tokio::select! {
                _ = shutdown.cancelled() => {
                    let _ = writer.shutdown().await;

                    return Ok::<(), ServerError>(());
                }

                cmd = outbound_rx.recv() => {
                    match cmd {
                        Some(OutboundCommand::Send(msg)) => {
                            writer.write_all(msg.as_slice()).await?;
                        }

                        Some(OutboundCommand::Close) | None => {
                            let _ = writer.shutdown().await;

                            return Ok(());
                        }
                    }
                }
            }
        }
    };

    // Race read/write halves.
    //
    // If either half exits or errors, cancel the connection.
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

/// Reads and validates one complete core-net message from a TCP client.
///
/// Processing:
///
/// ```text
/// read fixed header
///   -> decode header
///   -> validate magic and length
///   -> read payload
///   -> emit AppEvent::MessageReceived
/// ```
#[instrument(
    skip(reader, app_event_tx, config, recv_pool, header_buf, chunk_buf, payload_scratch),
    fields(client_id, %peer_addr)
)]
async fn read_one_message(
    reader: &mut tokio::net::tcp::OwnedReadHalf,
    client_id: ClientId,
    peer_addr: SocketAddr,
    app_event_tx: &mpsc::Sender<AppEvent>,
    config: &TcpServerConfig,
    recv_pool: Option<&BufferPool>,
    header_buf: &mut [u8],
    chunk_buf: &mut [u8],
    payload_scratch: &mut Vec<u8>,
) -> Result<(), ServerError> {
    // Read the fixed-size protocol header.
    reader.read_exact(header_buf).await?;

    let header = MessageHeader::decode(header_buf)?;

    header.validate_against(&config.expected_magic_string)?;

    let payload_len = header.payload_len()?;

    let total_len = MessageHeader::WIRE_SIZE + payload_len;

    if total_len < MessageHeader::WIRE_SIZE {
        return Err(ServerError::InvalidInboundLength(total_len));
    }

    debug!(
        component = COMPONENT,
        message_id = header.message_id,
        archive_type = ?header.archive_type,
        payload_len,
        total_len,
        "validated message header"
    );

    // Read payload into either:
    //
    // - a pooled buffer if available and sufficiently large
    // - reusable scratch buffer then MessageBuf fallback
    let payload = if let Some(pool) = recv_pool {
        if payload_len <= pool.block_size() {
            if let Some(mut pooled) = pool.try_acquire() {
                pooled.resize(payload_len, 0);

                reader.read_exact(pooled.as_mut_slice()).await?;

                MessageBuf::from_pooled(pooled)
            } else {
                read_into_scratch_and_copy(
                    reader,
                    chunk_buf,
                    payload_scratch,
                    payload_len,
                    recv_pool,
                )
                .await?
            }
        } else {
            read_into_scratch_and_copy(reader, chunk_buf, payload_scratch, payload_len, recv_pool)
                .await?
        }
    } else {
        read_into_scratch_and_copy(reader, chunk_buf, payload_scratch, payload_len, recv_pool)
            .await?
    };

    // Forward complete message to the application event channel.
    app_event_tx
        .send(AppEvent::MessageReceived {
            client_id,
            peer_addr,

            message: ReceivedMessage { header, payload },
        })
        .await
        .map_err(|e| {
            error!(
                component = COMPONENT,
                client_id,
                %peer_addr,
                error = %e,
                "failed to forward app event"
            );

            std::io::Error::new(std::io::ErrorKind::BrokenPipe, e)
        })?;

    Ok(())
}

/// Reads a payload through reusable scratch storage and returns a MessageBuf.
///
/// This path is used when:
///
/// - no receive pool exists
/// - receive pool is exhausted
/// - payload is larger than the pool block size
///
/// The socket is read in chunks to avoid large temporary allocations.
async fn read_into_scratch_and_copy(
    reader: &mut tokio::net::tcp::OwnedReadHalf,
    chunk_buf: &mut [u8],
    payload_scratch: &mut Vec<u8>,
    payload_len: usize,
    recv_pool: Option<&BufferPool>,
) -> Result<MessageBuf, std::io::Error> {
    payload_scratch.clear();

    if payload_len > payload_scratch.capacity() {
        payload_scratch.reserve(payload_len - payload_scratch.capacity());
    }

    let mut remaining = payload_len;

    while remaining > 0 {
        let to_read = remaining.min(chunk_buf.len());

        reader.read_exact(&mut chunk_buf[..to_read]).await?;

        payload_scratch.extend_from_slice(&chunk_buf[..to_read]);

        remaining -= to_read;
    }

    Ok(MessageBuf::from_slice_with_pool(recv_pool, payload_scratch))
}

/// Creates and binds a TCP listener using socket2.
///
/// This is used instead of `TcpListener::bind` directly so platform socket
/// options can be configured before binding.
fn create_listener(addr: SocketAddr, config: &TcpServerConfig) -> std::io::Result<TcpListener> {
    let domain = match addr {
        SocketAddr::V4(_) => Domain::IPV4,
        SocketAddr::V6(_) => Domain::IPV6,
    };

    let socket = Socket::new(domain, Type::STREAM, Some(Protocol::TCP))?;

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
        match addr {
            SocketAddr::V4(_) => socket.set_ttl_v4(ttl)?,
            SocketAddr::V6(_) => socket.set_unicast_hops_v6(ttl)?,
        }
    }

    socket.set_nonblocking(true)?;

    socket.bind(&addr.into())?;

    socket.listen(config.listen_backlog)?;

    let std_listener: std::net::TcpListener = socket.into();

    TcpListener::from_std(std_listener)
}

/// Applies configured socket options to an accepted TCP stream.
fn apply_stream_options(stream: &TcpStream, config: &TcpServerConfig) -> std::io::Result<()> {
    stream.set_nodelay(matches!(config.socket.send_option, SendOption::NagleOff))?;

    if let Some(ttl) = config.socket.ttl {
        stream.set_ttl(ttl)?;
    }

    let sock_ref = SockRef::from(stream);

    sock_ref.set_keepalive(matches!(config.socket.keep_alive, KeepAliveOption::On))?;

    if let Some(size) = config.socket.send_buffer_size {
        sock_ref.set_send_buffer_size(size)?;
    }

    if let Some(size) = config.socket.recv_buffer_size {
        sock_ref.set_recv_buffer_size(size)?;
    }

    Ok(())
}
