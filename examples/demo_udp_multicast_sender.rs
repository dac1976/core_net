use core_net::{
    config::{MulticastGroup, UdpMulticastConfig},
    logging::{init_tracing, LogTimeMode, LoggingConfig},
    messaging::message_builder::build_raw_message,
    protocol::DEFAULT_MAGIC_STRING,
    udp_multicast::{UdpMulticastEndpoint, UdpMulticastEvent},
};
use miette::{Result, WrapErr};
use std::net::Ipv4Addr;
use tokio::sync::mpsc;
use tracing::{error, info};

#[tokio::main]
async fn main() -> Result<()> {
    init_tracing(&LoggingConfig {
        directory: "logs",
        file_name: "demo_udp_multicast_sender.log",
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
        local_bind_addr: "0.0.0.0:9301".parse().unwrap(),
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
    cfg.join_group_on_start = false;

    let endpoint = UdpMulticastEndpoint::new(cfg.clone(), tx);
    let handle = endpoint.handle();

    tokio::spawn(async move {
        if let Err(err) = endpoint.run().await {
            error!(error = %err, "udp multicast sender failed");
        }
    });

    let mut replies_seen = 0usize;

    while let Some(event) = rx.recv().await {
        match event {
            UdpMulticastEvent::Bound { local_addr } => {
                info!(%local_addr, "multicast sender bound");

                let msg1 =
                    build_raw_message(cfg.expected_magic_string, 1, b"multicast ping payload");

                let msg2 =
                    build_raw_message(cfg.expected_magic_string, 2, b"multicast echo me back");

                if let Err(err) = handle.send_to_group_async(&msg1).await {
                    error!(error = %err, "failed to send multicast ping");
                }

                if let Err(err) = handle.send_to_group_async(&msg2).await {
                    error!(error = %err, "failed to send multicast echo");
                }
            }

            UdpMulticastEvent::Joined { local_addr, group } => {
                info!(%local_addr, %group, "multicast sender joined group");
            }

            UdpMulticastEvent::Closed { local_addr } => {
                info!(%local_addr, "multicast sender closed");
                break;
            }

            UdpMulticastEvent::DatagramReceived { datagram } => {
                let payload_text = String::from_utf8_lossy(datagram.payload.as_slice());

                match datagram.header.message_id {
                    1 | 2 => {
                        info!(
                            from = %datagram.from,
                            message_id = datagram.header.message_id,
                            "ignoring original multicast request datagram"
                        );
                    }
                    1001 => {
                        info!(
                            from = %datagram.from,
                            payload = %payload_text,
                            "received multicast ping reply"
                        );
                        replies_seen += 1;
                    }
                    1002 => {
                        info!(
                            from = %datagram.from,
                            payload = %payload_text,
                            "received multicast echo reply"
                        );
                        replies_seen += 1;
                    }
                    other => {
                        info!(
                            from = %datagram.from,
                            message_id = other,
                            payload = %payload_text,
                            "received unexpected multicast reply"
                        );
                    }
                }

                if replies_seen >= 2 {
                    let _ = handle.close().await;
                }
            }
        }
    }

    Ok(())
}
