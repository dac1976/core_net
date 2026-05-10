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
    config::{KeepAliveOption, SendOption, TcpClientConfig},
    pool::{BufferPool, MessageBuf},
    protocol::{MessageHeader, ProtocolError},
};

use socket2::SockRef;

use std::{net::SocketAddr, sync::Arc};

use thiserror::Error;

use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
    sync::{Mutex, mpsc},
};

use tokio_util::sync::CancellationToken;

use tracing::{debug, error, info, instrument, warn};

/// Component name used in structured log records.
const COMPONENT: &str = "tcp_client";

/// Message received by the TCP client from the connected server.
///
/// This is the client-side transport-specific received-message type.
/// Higher-level code can convert this into a transport-independent
/// `messaging::message::Message` if required.
#[derive(Debug, Clone)]
pub struct ReceivedMessage {
    /// Parsed core-net protocol header.
    pub header: MessageHeader,

    /// Message payload bytes.
    ///
    /// Backed by `MessageBuf`, so payload storage may be pooled or dynamic.
    pub payload: MessageBuf,
}

/// Application-facing events emitted by `TcpClient`.
///
/// The client runs its socket IO internally and reports important lifecycle
/// and message events through an async channel.
#[derive(Debug, Clone)]
pub enum ClientEvent {
    /// TCP connection established successfully.
    Connected {
        /// Remote server address.
        server_addr: SocketAddr,

        /// Local socket address chosen by the OS.
        local_addr: SocketAddr,
    },

    /// TCP connection disconnected or ended.
    Disconnected {
        /// Remote server address.
        server_addr: SocketAddr,
    },

    /// Complete protocol message received from the server.
    MessageReceived {
        /// Remote server address.
        server_addr: SocketAddr,

        /// Received message.
        message: ReceivedMessage,
    },
}

/// Errors that can occur in the TCP client.
#[derive(Debug, Error)]
pub enum ClientError {
    /// Underlying socket/IO failure.
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    /// Core protocol decode/validation failure.
    #[error("protocol error: {0}")]
    Protocol(#[from] ProtocolError),

    /// Non-blocking outbound send failed because the queue is full.
    #[error("outbound queue full")]
    OutboundQueueFull,

    /// Outbound queue is closed.
    #[error("outbound queue closed")]
    OutboundQueueClosed,

    /// Internal receiver was already taken.
    ///
    /// `TcpClient::run()` is intended to be called once.
    #[error("client receiver already taken")]
    ReceiverAlreadyTaken,

    /// Invalid inbound message length detected.
    #[error("invalid inbound message length: {0}")]
    InvalidInboundLength(usize),
}

/// Internal outbound command sent from `TcpClientHandle` to the writer task.
#[derive(Clone)]
enum OutboundCommand {
    /// Send a full, already-framed core-net message.
    Send(MessageBuf),

    /// Gracefully close the TCP connection.
    Close,
}

/// Cloneable handle used by application code to interact with a running client.
///
/// The handle can:
///
/// - enqueue messages for sending
/// - request disconnect
/// - trigger shutdown
///
/// Clones share the same outbound queue and cancellation token.
#[derive(Clone)]
pub struct TcpClientHandle {
    outbound_tx: mpsc::Sender<OutboundCommand>,
    send_pool: Option<BufferPool>,
    shutdown: CancellationToken,
}

impl TcpClientHandle {
    /// Sends a complete core-net message to the server asynchronously.
    ///
    /// `full_message` must already contain:
    ///
    /// - core-net message header
    /// - payload
    ///
    /// This method may await if the outbound queue is currently full.
    #[instrument(skip(self, full_message), fields(len = full_message.len()))]
    pub async fn send_to_server_async(&self, full_message: &[u8]) -> Result<(), ClientError> {
        // Copy message bytes into owned MessageBuf storage.
        //
        // If a send pool is configured and a free buffer is available, this
        // avoids dynamic allocation on the hot path.
        let msg = MessageBuf::from_slice_with_pool(self.send_pool.as_ref(), full_message);

        self.outbound_tx
            .send(OutboundCommand::Send(msg))
            .await
            .map_err(|_| ClientError::OutboundQueueClosed)
    }

    /// Attempts to enqueue a message without awaiting.
    ///
    /// Useful for low-latency or real-time-ish paths where blocking/awaiting
    /// on queue space is not acceptable.
    pub fn try_send_to_server(&self, full_message: &[u8]) -> Result<(), ClientError> {
        let msg = MessageBuf::from_slice_with_pool(self.send_pool.as_ref(), full_message);

        match self.outbound_tx.try_send(OutboundCommand::Send(msg)) {
            Ok(()) => Ok(()),

            Err(tokio::sync::mpsc::error::TrySendError::Full(_)) => {
                Err(ClientError::OutboundQueueFull)
            }

            Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => {
                Err(ClientError::OutboundQueueClosed)
            }
        }
    }

    /// Requests a graceful disconnect.
    pub async fn disconnect(&self) -> Result<(), ClientError> {
        self.outbound_tx
            .send(OutboundCommand::Close)
            .await
            .map_err(|_| ClientError::OutboundQueueClosed)
    }

    /// Cancels the client run loop.
    ///
    /// This is a broader shutdown signal than `disconnect()` and is useful
    /// when the application wants to abort both read and write paths.
    pub fn shutdown(&self) {
        self.shutdown.cancel();
    }
}

/// Asynchronous TCP client.
///
/// The client:
///
/// - connects to one server
/// - reads framed core-net messages
/// - emits application events
/// - accepts outbound messages through a handle
/// - uses optional pooled buffers to reduce allocation churn
pub struct TcpClient {
    server_addr: SocketAddr,
    config: TcpClientConfig,
    app_event_tx: mpsc::Sender<ClientEvent>,
    shutdown: CancellationToken,
    send_pool: Option<BufferPool>,
    outbound_tx: mpsc::Sender<OutboundCommand>,

    /// Outbound receiver is wrapped in Option so it can be moved into the
    /// running connection task exactly once.
    outbound_rx: Arc<Mutex<Option<mpsc::Receiver<OutboundCommand>>>>,
}

impl TcpClient {
    /// Creates a new TCP client instance.
    ///
    /// The client does not connect until `run()` is awaited.
    pub fn new(
        server_addr: SocketAddr,
        config: TcpClientConfig,
        app_event_tx: mpsc::Sender<ClientEvent>,
    ) -> Self {
        // Optional outbound send pool.
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
        // This provides explicit backpressure and prevents unbounded memory
        // growth if the socket writer cannot keep up.
        let (outbound_tx, outbound_rx) =
            mpsc::channel::<OutboundCommand>(config.max_allowed_unsent_async_messages);

        Self {
            server_addr,
            config,
            app_event_tx,
            shutdown: CancellationToken::new(),
            send_pool,
            outbound_tx,
            outbound_rx: Arc::new(Mutex::new(Some(outbound_rx))),
        }
    }

    /// Returns a cloneable handle for sending/disconnect/shutdown operations.
    pub fn handle(&self) -> TcpClientHandle {
        TcpClientHandle {
            outbound_tx: self.outbound_tx.clone(),
            send_pool: self.send_pool.clone(),
            shutdown: self.shutdown.clone(),
        }
    }

    /// Connects to the server and runs the client event loop.
    #[instrument(skip(self))]
    pub async fn run(self) -> Result<(), ClientError> {
        info!(
            component = COMPONENT,
            server = %self.server_addr,
            "connecting"
        );

        let stream = TcpStream::connect(self.server_addr).await?;

        apply_stream_options(&stream, &self.config)?;

        let local_addr = stream.local_addr()?;

        info!(
            component = COMPONENT,
            server = %self.server_addr,
            local = %local_addr,
            "connected"
        );

        // Notify application that connection is established.
        let _ = self
            .app_event_tx
            .send(ClientEvent::Connected {
                server_addr: self.server_addr,
                local_addr,
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

        // Move outbound receiver into the running connection.
        //
        // This prevents multiple concurrent run loops from consuming the same
        // outbound queue.
        let mut rx_guard = self.outbound_rx.lock().await;

        let outbound_rx = rx_guard.take().ok_or(ClientError::ReceiverAlreadyTaken)?;

        drop(rx_guard);

        let res = run_connection(
            self.server_addr,
            stream,
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
                server = %self.server_addr,
                error = %err,
                "client connection ended with error"
            );
        }

        // Notify application that connection ended.
        let _ = self
            .app_event_tx
            .send(ClientEvent::Disconnected {
                server_addr: self.server_addr,
            })
            .await;

        res
    }
}

/// Runs the connected TCP client read/write loops.
///
/// The stream is split into independent read and write halves:
///
/// ```text
/// read task  -> socket to app events
/// write task -> outbound queue to socket
/// ```
///
/// Whichever side completes/errors first cancels the other side.
#[instrument(
    skip(stream, outbound_rx, app_event_tx, shutdown, config, recv_pool),
    fields(server = %server_addr)
)]
async fn run_connection(
    server_addr: SocketAddr,
    stream: TcpStream,
    mut outbound_rx: mpsc::Receiver<OutboundCommand>,
    app_event_tx: mpsc::Sender<ClientEvent>,
    shutdown: CancellationToken,
    config: TcpClientConfig,
    recv_pool: Option<BufferPool>,
) -> Result<(), ClientError> {
    let (mut reader, mut writer) = stream.into_split();

    // Reused per-connection scratch buffers.
    //
    // These avoid repeatedly allocating temporary read buffers while parsing
    // incoming messages.
    let mut header_buf = vec![0u8; config.min_amount_to_read];

    let mut chunk_buf = vec![0u8; config.recv_chunk_size.max(1)];

    let mut payload_scratch = Vec::<u8>::new();

    // Socket read loop.
    let read_task = async {
        loop {
            tokio::select! {
                _ = shutdown.cancelled() => {
                    return Ok::<(), ClientError>(());
                }

                res = read_one_message(
                    &mut reader,
                    server_addr,
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

                    return Ok::<(), ClientError>(());
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

    // Race read/write loops.
    //
    // If either exits, cancel the other.
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

/// Reads and validates exactly one complete core-net message from the socket.
///
/// Processing:
///
/// ```text
/// read fixed header
///   -> decode header
///   -> validate magic/length
///   -> read payload
///   -> emit ClientEvent::MessageReceived
/// ```
#[instrument(
    skip(reader, app_event_tx, config, recv_pool, header_buf, chunk_buf, payload_scratch),
    fields(server = %server_addr)
)]
async fn read_one_message(
    reader: &mut tokio::net::tcp::OwnedReadHalf,
    server_addr: SocketAddr,
    app_event_tx: &mpsc::Sender<ClientEvent>,
    config: &TcpClientConfig,
    recv_pool: Option<&BufferPool>,
    header_buf: &mut [u8],
    chunk_buf: &mut [u8],
    payload_scratch: &mut Vec<u8>,
) -> Result<(), ClientError> {
    // Read the fixed-size header.
    reader.read_exact(header_buf).await?;

    let header = MessageHeader::decode(header_buf)?;

    header.validate_against(&config.expected_magic_string)?;

    let payload_len = header.payload_len()?;

    let total_len = MessageHeader::WIRE_SIZE + payload_len;

    if total_len < MessageHeader::WIRE_SIZE {
        return Err(ClientError::InvalidInboundLength(total_len));
    }

    debug!(
        component = COMPONENT,
        server = %server_addr,
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

    // Forward complete message to application event channel.
    app_event_tx
        .send(ClientEvent::MessageReceived {
            server_addr,

            message: ReceivedMessage { header, payload },
        })
        .await
        .map_err(|e| {
            error!(
                component = COMPONENT,
                server = %server_addr,
                error = %e,
                "failed to forward client app event"
            );

            std::io::Error::new(std::io::ErrorKind::BrokenPipe, e)
        })?;

    Ok(())
}

/// Reads a payload through a reusable scratch buffer and returns a MessageBuf.
///
/// This path is used when:
///
/// - no receive pool exists
/// - receive pool is exhausted
/// - payload is larger than the pool block size
///
/// The socket is read in chunks to avoid requiring a huge temporary stack
/// allocation.
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

/// Applies configured socket options to the connected TCP stream.
fn apply_stream_options(stream: &TcpStream, config: &TcpClientConfig) -> std::io::Result<()> {
    // TCP_NODELAY.
    stream.set_nodelay(matches!(config.socket.send_option, SendOption::NagleOff))?;

    // IP TTL.
    if let Some(ttl) = config.socket.ttl {
        stream.set_ttl(ttl)?;
    }

    let sock_ref = SockRef::from(stream);

    // TCP keepalive.
    sock_ref.set_keepalive(matches!(config.socket.keep_alive, KeepAliveOption::On))?;

    // OS socket send buffer size.
    if let Some(size) = config.socket.send_buffer_size {
        sock_ref.set_send_buffer_size(size)?;
    }

    // OS socket receive buffer size.
    if let Some(size) = config.socket.recv_buffer_size {
        sock_ref.set_recv_buffer_size(size)?;
    }

    Ok(())
}
