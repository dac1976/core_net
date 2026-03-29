mod app;

use app::handlers;
use core_net::{
    config::{MulticastGroup, UdpMulticastConfig},
    logging::{init_tracing, LogTimeMode, LoggingConfig},
    messaging::dispatcher::{BoxFuture, MessageContext, MessageDispatcherBuilder, ReplyHandle},
    messaging::message::Message,
    protocol::DEFAULT_MAGIC_STRING,
    udp_multicast::{UdpMulticastEndpoint, UdpMulticastEvent},
};
use miette::{Result, WrapErr};
use std::{
    net::{Ipv4Addr, SocketAddr},
    sync::Arc,
};
use tokio::sync::mpsc;
use tracing::{error, info};

#[tokio::main]
async fn main() -> Result<()> {
    init_tracing(&LoggingConfig {
        directory: "logs",
        file_name: "demo_udp_multicast_listener.log",
        max_bytes: 5 * 1024 * 1024,
        keep_files: 10,
        level_filter: "info",
        time_mode: LogTimeMode::Utc,
        also_stderr: true,
    })
    .wrap_err("failed to initialize tracing")?;

    let (tx, mut rx) = mpsc::channel(1024);

    let mut cfg = UdpMulticastConfig::default();
    cfg.group = MulticastGroup::V4 {
        local_bind_addr: "0.0.0.0:9300".parse().unwrap(),
        group_addr: Ipv4Addr::new(239, 255, 0, 1),
        group_port: 9300,
        interface_addr: Ipv4Addr::UNSPECIFIED,
    };
    cfg.max_datagram_size = 64 * 1024;
    cfg.send_pool_msg_size = 8192;
    cfg.recv_pool_msg_count = 128;
    cfg.recv_pool_msg_size = 8192;
    cfg.expected_magic_string = DEFAULT_MAGIC_STRING;
    cfg.send_ttl_v4 = 1;
    cfg.multicast_loop_v4 = true;
    cfg.join_group_on_start = true;

    let endpoint = UdpMulticastEndpoint::new(cfg.clone(), tx);
    let handle = endpoint.handle();

    let send_fn = Arc::new(move |to: SocketAddr, bytes: Vec<u8>| {
        let handle = handle.clone();
        Box::pin(async move {
            handle
                .send_to_async(to, &bytes)
                .await
                .map_err(|e| e.to_string())
        }) as BoxFuture<Result<(), String>>
    });

    let mut dispatcher_builder = MessageDispatcherBuilder::new();
    handlers::register_handlers(&mut dispatcher_builder);
    let dispatcher = dispatcher_builder.build();

    tokio::spawn(async move {
        if let Err(err) = endpoint.run().await {
            error!(error = %err, "udp multicast listener failed");
        }
    });

    info!("udp multicast listener starting");

    while let Some(event) = rx.recv().await {
        match event {
            UdpMulticastEvent::Bound { local_addr } => {
                info!(%local_addr, "multicast listener bound");
            }

            UdpMulticastEvent::Joined { local_addr, group } => {
                info!(%local_addr, %group, "multicast listener joined group");
            }

            UdpMulticastEvent::Closed { local_addr } => {
                info!(%local_addr, "multicast listener closed");
                break;
            }

            UdpMulticastEvent::DatagramReceived { datagram } => {
                let peer = datagram.from;
                let app_message = Message::from_udp(datagram);

                let ctx = MessageContext {
                    source_addr: Some(peer),
                    expected_magic: cfg.expected_magic_string,
                    reply_handle: ReplyHandle::Udp {
                        peer_addr: peer,
                        send_fn: send_fn.clone(),
                    },
                };

                dispatcher.dispatch(ctx, app_message).await;
            }
        }
    }

    Ok(())
}
