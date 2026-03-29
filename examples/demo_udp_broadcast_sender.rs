use core_net::{
    config::UdpConfig,
    logging::{init_tracing, LogTimeMode, LoggingConfig},
    messaging::message_builder::build_raw_message,
    protocol::DEFAULT_MAGIC_STRING,
    udp_broadcast::{UdpBroadcastEndpoint, UdpBroadcastEvent},
};
use miette::{IntoDiagnostic, Result, WrapErr};
use tokio::sync::mpsc;
use tracing::{error, info};

#[tokio::main]
async fn main() -> Result<()> {
    init_tracing(&LoggingConfig {
        directory: "logs",
        file_name: "demo_udp_broadcast_sender.log",
        max_bytes: 5 * 1024 * 1024,
        keep_files: 10,
        level_filter: "info",
        time_mode: LogTimeMode::Utc,
        also_stderr: true,
    })
    .wrap_err("failed to initialize tracing")?;

    let (tx, mut rx) = mpsc::channel(1024);

    let mut cfg = UdpConfig::default();
    cfg.max_datagram_size = 64 * 1024;
    cfg.send_pool_msg_size = 8192;
    cfg.recv_pool_msg_count = 128;
    cfg.recv_pool_msg_size = 8192;
    cfg.expected_magic_string = DEFAULT_MAGIC_STRING;
    cfg.socket.broadcast = true;

    let local_addr = "0.0.0.0:0".parse().into_diagnostic()?;
    let endpoint = UdpBroadcastEndpoint::new(local_addr, cfg.clone(), tx);
    let handle = endpoint.handle();

    tokio::spawn(async move {
        if let Err(err) = endpoint.run().await {
            error!(error = %err, "udp broadcast sender endpoint failed");
        }
    });

    // For local testing on one machine, 127.255.255.255 often works better than
    // 255.255.255.255 because it stays on loopback.
    let broadcast_addr = "127.255.255.255:9200".parse().into_diagnostic()?;

    let mut replies_seen = 0usize;

    while let Some(event) = rx.recv().await {
        match event {
            UdpBroadcastEvent::Bound { local_addr } => {
                info!(%local_addr, %broadcast_addr, "broadcast sender bound");

                let msg1 =
                    build_raw_message(cfg.expected_magic_string, 1, b"broadcast ping payload");

                let msg2 =
                    build_raw_message(cfg.expected_magic_string, 2, b"broadcast echo me back");

                handle
                    .send_to_async(broadcast_addr, &msg1)
                    .await
                    .into_diagnostic()
                    .wrap_err("failed to send broadcast ping")?;

                handle
                    .send_to_async(broadcast_addr, &msg2)
                    .await
                    .into_diagnostic()
                    .wrap_err("failed to send broadcast echo")?;
            }

            UdpBroadcastEvent::Closed { local_addr } => {
                info!(%local_addr, "broadcast sender endpoint closed");
                break;
            }

            UdpBroadcastEvent::DatagramReceived { datagram } => {
                let payload_text = String::from_utf8_lossy(datagram.payload.as_slice());

                match datagram.header.message_id {
                    1001 => {
                        info!(
                            from = %datagram.from,
                            payload = %payload_text,
                            "received broadcast ping reply"
                        );
                        replies_seen += 1;
                    }
                    1002 => {
                        info!(
                            from = %datagram.from,
                            payload = %payload_text,
                            "received broadcast echo reply"
                        );
                        replies_seen += 1;
                    }
                    other => {
                        info!(
                            from = %datagram.from,
                            message_id = other,
                            payload = %payload_text,
                            "received unexpected broadcast reply"
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
