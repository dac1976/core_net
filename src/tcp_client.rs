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
    sync::{mpsc, Mutex},
};
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, instrument, warn};

const COMPONENT: &str = "tcp_client";

#[derive(Debug, Clone)]
pub struct ReceivedMessage {
    pub header: MessageHeader,
    pub payload: MessageBuf,
}

#[derive(Debug, Clone)]
pub enum ClientEvent {
    Connected {
        server_addr: SocketAddr,
        local_addr: SocketAddr,
    },
    Disconnected {
        server_addr: SocketAddr,
    },
    MessageReceived {
        server_addr: SocketAddr,
        message: ReceivedMessage,
    },
}

#[derive(Debug, Error)]
pub enum ClientError {
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    #[error("protocol error: {0}")]
    Protocol(#[from] ProtocolError),

    #[error("outbound queue full")]
    OutboundQueueFull,

    #[error("outbound queue closed")]
    OutboundQueueClosed,

    #[error("client receiver already taken")]
    ReceiverAlreadyTaken,

    #[error("invalid inbound message length: {0}")]
    InvalidInboundLength(usize),
}

#[derive(Clone)]
enum OutboundCommand {
    Send(MessageBuf),
    Close,
}

#[derive(Clone)]
pub struct TcpClientHandle {
    outbound_tx: mpsc::Sender<OutboundCommand>,
    send_pool: Option<BufferPool>,
    shutdown: CancellationToken,
}

impl TcpClientHandle {
    #[instrument(skip(self, full_message), fields(len = full_message.len()))]
    pub async fn send_to_server_async(&self, full_message: &[u8]) -> Result<(), ClientError> {
        let msg = MessageBuf::from_slice_with_pool(self.send_pool.as_ref(), full_message);

        self.outbound_tx
            .send(OutboundCommand::Send(msg))
            .await
            .map_err(|_| ClientError::OutboundQueueClosed)
    }

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

    pub async fn disconnect(&self) -> Result<(), ClientError> {
        self.outbound_tx
            .send(OutboundCommand::Close)
            .await
            .map_err(|_| ClientError::OutboundQueueClosed)
    }

    pub fn shutdown(&self) {
        self.shutdown.cancel();
    }
}

pub struct TcpClient {
    server_addr: SocketAddr,
    config: TcpClientConfig,
    app_event_tx: mpsc::Sender<ClientEvent>,
    shutdown: CancellationToken,
    send_pool: Option<BufferPool>,
    outbound_tx: mpsc::Sender<OutboundCommand>,
    outbound_rx: Arc<Mutex<Option<mpsc::Receiver<OutboundCommand>>>>,
}

impl TcpClient {
    pub fn new(
        server_addr: SocketAddr,
        config: TcpClientConfig,
        app_event_tx: mpsc::Sender<ClientEvent>,
    ) -> Self {
        let send_pool = if config.send_pool_msg_size > 0 {
            Some(BufferPool::new(
                config.send_pool_msg_size,
                config.max_allowed_unsent_async_messages.max(1),
            ))
        } else {
            None
        };

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

    pub fn handle(&self) -> TcpClientHandle {
        TcpClientHandle {
            outbound_tx: self.outbound_tx.clone(),
            send_pool: self.send_pool.clone(),
            shutdown: self.shutdown.clone(),
        }
    }

    #[instrument(skip(self))]
    pub async fn run(self) -> Result<(), ClientError> {
        info!(component = COMPONENT, server = %self.server_addr, "connecting");

        let stream = TcpStream::connect(self.server_addr).await?;
        apply_stream_options(&stream, &self.config)?;

        let local_addr = stream.local_addr()?;

        info!(
            component = COMPONENT,
            server = %self.server_addr,
            local = %local_addr,
            "connected"
        );

        let _ = self
            .app_event_tx
            .send(ClientEvent::Connected {
                server_addr: self.server_addr,
                local_addr,
            })
            .await;

        let recv_pool = if self.config.recv_pool_msg_count > 0 {
            Some(BufferPool::new(
                self.config.recv_pool_msg_size,
                self.config.recv_pool_msg_count,
            ))
        } else {
            None
        };

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

        let _ = self
            .app_event_tx
            .send(ClientEvent::Disconnected {
                server_addr: self.server_addr,
            })
            .await;

        res
    }
}

#[instrument(skip(stream, outbound_rx, app_event_tx, shutdown, config, recv_pool), fields(server = %server_addr))]
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
    let mut header_buf = vec![0u8; config.min_amount_to_read];
    let mut chunk_buf = vec![0u8; config.recv_chunk_size.max(1)];
    let mut payload_scratch = Vec::<u8>::new();

    let read_task = async {
        loop {
            tokio::select! {
                _ = shutdown.cancelled() => return Ok::<(), ClientError>(()),
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

#[instrument(skip(reader, app_event_tx, config, recv_pool, header_buf, chunk_buf, payload_scratch), fields(server = %server_addr))]
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

fn apply_stream_options(stream: &TcpStream, config: &TcpClientConfig) -> std::io::Result<()> {
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
