use crate::{
    config::{MulticastGroup, UdpMulticastConfig},
    pool::{BufferPool, MessageBuf},
    udp_common::{parse_datagram, ReceivedDatagram, UdpDatagramError},
};
use socket2::{Domain, Protocol, Socket, Type};
use std::{
    net::{IpAddr, SocketAddr},
    sync::Arc,
};
use tokio::{
    net::UdpSocket,
    sync::{mpsc, Mutex},
};
use tokio_util::sync::CancellationToken;
use tracing::{error, info, instrument, warn};

const COMPONENT: &str = "udp_multicast";

#[derive(Debug, Clone)]
pub enum UdpMulticastEvent {
    Bound {
        local_addr: SocketAddr,
    },
    Joined {
        local_addr: SocketAddr,
        group: String,
    },
    DatagramReceived {
        datagram: ReceivedDatagram,
    },
    Closed {
        local_addr: SocketAddr,
    },
}

#[derive(Clone)]
enum OutboundCommand {
    SendTo { to: SocketAddr, payload: MessageBuf },
    SendToGroup { payload: MessageBuf },
    Close,
}

#[derive(Clone)]
pub struct UdpMulticastHandle {
    outbound_tx: mpsc::Sender<OutboundCommand>,
    send_pool: Option<BufferPool>,
    shutdown: CancellationToken,
}

impl UdpMulticastHandle {
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

    #[instrument(skip(self, full_message), fields(len = full_message.len()))]
    pub async fn send_to_group_async(&self, full_message: &[u8]) -> Result<(), UdpDatagramError> {
        let msg = MessageBuf::from_slice_with_pool(self.send_pool.as_ref(), full_message);

        self.outbound_tx
            .send(OutboundCommand::SendToGroup { payload: msg })
            .await
            .map_err(|_| UdpDatagramError::OutboundQueueClosed)
    }

    pub async fn close(&self) -> Result<(), UdpDatagramError> {
        self.outbound_tx
            .send(OutboundCommand::Close)
            .await
            .map_err(|_| UdpDatagramError::OutboundQueueClosed)
    }

    pub fn shutdown(&self) {
        self.shutdown.cancel();
    }
}

pub struct UdpMulticastEndpoint {
    config: UdpMulticastConfig,
    app_event_tx: mpsc::Sender<UdpMulticastEvent>,
    shutdown: CancellationToken,
    send_pool: Option<BufferPool>,
    outbound_tx: mpsc::Sender<OutboundCommand>,
    outbound_rx: Arc<Mutex<Option<mpsc::Receiver<OutboundCommand>>>>,
}

impl UdpMulticastEndpoint {
    pub fn new(config: UdpMulticastConfig, app_event_tx: mpsc::Sender<UdpMulticastEvent>) -> Self {
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
            config,
            app_event_tx,
            shutdown: CancellationToken::new(),
            send_pool,
            outbound_tx,
            outbound_rx: Arc::new(Mutex::new(Some(outbound_rx))),
        }
    }

    pub fn handle(&self) -> UdpMulticastHandle {
        UdpMulticastHandle {
            outbound_tx: self.outbound_tx.clone(),
            send_pool: self.send_pool.clone(),
            shutdown: self.shutdown.clone(),
        }
    }

    #[instrument(skip(self))]
    pub async fn run(self) -> Result<(), UdpDatagramError> {
        let std_socket = create_multicast_socket(&self.config)?;
        let socket = Arc::new(UdpSocket::from_std(std_socket)?);

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
                local = %local_addr,
                group = %group_string,
                error = %err,
                "udp multicast endpoint ended with error"
            );
        }

        let _ = self
            .app_event_tx
            .send(UdpMulticastEvent::Closed { local_addr })
            .await;

        res
    }
}

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
                        .send(UdpMulticastEvent::DatagramReceived { datagram })
                        .await
                        .map_err(|e| {
                            error!(
                                component = COMPONENT,
                                %from,
                                error = %e,
                                "failed to forward udp multicast app event"
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
                        Some(OutboundCommand::SendTo { to, payload }) => {
                            send_socket.send_to(payload.as_slice(), to).await?;
                        }
                        Some(OutboundCommand::SendToGroup { payload }) => {
                            send_socket.send_to(payload.as_slice(), group_target).await?;
                        }
                        Some(OutboundCommand::Close) | None => {
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

    socket.set_nonblocking(true)?;
    socket.bind(&bind_addr.into())?;

    Ok(socket.into())
}

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
