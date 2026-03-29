use crate::{
    config::UdpConfig,
    pool::BufferPool,
    udp_common::{parse_datagram, ReceivedDatagram, UdpDatagramError, UdpOutboundCommand},
};
use socket2::{Domain, Protocol, Socket, Type};
use std::{net::SocketAddr, sync::Arc};
use tokio::{
    net::UdpSocket,
    sync::{mpsc, Mutex},
};
use tokio_util::sync::CancellationToken;
use tracing::{error, info, instrument, warn};

const COMPONENT: &str = "udp_unicast";

#[derive(Debug, Clone)]
pub enum UdpUnicastEvent {
    Bound { local_addr: SocketAddr },
    DatagramReceived { datagram: ReceivedDatagram },
    Closed { local_addr: SocketAddr },
}

#[derive(Clone)]
pub struct UdpUnicastHandle {
    outbound_tx: mpsc::Sender<UdpOutboundCommand>,
    send_pool: Option<BufferPool>,
    shutdown: CancellationToken,
}

impl UdpUnicastHandle {
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

    pub async fn close(&self) -> Result<(), UdpDatagramError> {
        self.outbound_tx
            .send(UdpOutboundCommand::Close)
            .await
            .map_err(|_| UdpDatagramError::OutboundQueueClosed)
    }

    pub fn shutdown(&self) {
        self.shutdown.cancel();
    }
}

pub struct UdpUnicastEndpoint {
    local_addr: SocketAddr,
    config: UdpConfig,
    app_event_tx: mpsc::Sender<UdpUnicastEvent>,
    shutdown: CancellationToken,
    send_pool: Option<BufferPool>,
    outbound_tx: mpsc::Sender<UdpOutboundCommand>,
    outbound_rx: Arc<Mutex<Option<mpsc::Receiver<UdpOutboundCommand>>>>,
}

impl UdpUnicastEndpoint {
    pub fn new(
        local_addr: SocketAddr,
        config: UdpConfig,
        app_event_tx: mpsc::Sender<UdpUnicastEvent>,
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

    pub fn handle(&self) -> UdpUnicastHandle {
        UdpUnicastHandle {
            outbound_tx: self.outbound_tx.clone(),
            send_pool: self.send_pool.clone(),
            shutdown: self.shutdown.clone(),
        }
    }

    #[instrument(skip(self))]
    pub async fn run(self) -> Result<(), UdpDatagramError> {
        let socket = create_udp_socket(self.local_addr, &self.config)?;
        let socket = Arc::new(UdpSocket::from_std(socket)?);

        let bound_addr = socket.local_addr()?;

        info!(
            component = COMPONENT,
            local = %bound_addr,
            "udp unicast endpoint bound"
        );

        let _ = self
            .app_event_tx
            .send(UdpUnicastEvent::Bound {
                local_addr: bound_addr,
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
                "udp unicast endpoint ended with error"
            );
        }

        let _ = self
            .app_event_tx
            .send(UdpUnicastEvent::Closed {
                local_addr: bound_addr,
            })
            .await;

        res
    }
}

#[instrument(skip(socket, outbound_rx, app_event_tx, shutdown, config, recv_pool))]
async fn run_endpoint(
    socket: Arc<UdpSocket>,
    mut outbound_rx: mpsc::Receiver<UdpOutboundCommand>,
    app_event_tx: mpsc::Sender<UdpUnicastEvent>,
    shutdown: CancellationToken,
    config: UdpConfig,
    recv_pool: Option<BufferPool>,
) -> Result<(), UdpDatagramError> {
    let recv_socket = Arc::clone(&socket);
    let send_socket = Arc::clone(&socket);

    let read_task = async {
        let mut buf = vec![
            0u8;
            config
                .max_datagram_size
                .max(crate::protocol::MessageHeader::WIRE_SIZE)
        ];

        loop {
            tokio::select! {
                _ = shutdown.cancelled() => return Ok::<(), UdpDatagramError>(()),
                res = recv_socket.recv_from(&mut buf) => {
                    let (len, from) = res?;
                    let datagram = parse_datagram(
                        &buf[..len],
                        from,
                        &config.expected_magic_string,
                        recv_pool.as_ref(),
                    )?;

                    app_event_tx
                        .send(UdpUnicastEvent::DatagramReceived { datagram })
                        .await
                        .map_err(|e| {
                            error!(
                                component = COMPONENT,
                                %from,
                                error = %e,
                                "failed to forward udp unicast app event"
                            );
                            std::io::Error::new(std::io::ErrorKind::BrokenPipe, e)
                        })?;
                }
            }
        }
    };

    let write_task = async {
        loop {
            tokio::select! {
                _ = shutdown.cancelled() => return Ok::<(), UdpDatagramError>(()),
                cmd = outbound_rx.recv() => {
                    match cmd {
                        Some(UdpOutboundCommand::SendTo { to, payload }) => {
                            send_socket.send_to(payload.as_slice(), to).await?;
                        }
                        Some(UdpOutboundCommand::Close) | None => {
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

    socket.set_broadcast(config.socket.broadcast)?;
    socket.set_nonblocking(true)?;
    socket.bind(&local_addr.into())?;

    Ok(socket.into())
}
