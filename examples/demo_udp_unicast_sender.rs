use core_net::{
    config::UdpConfig,
    logging::{init_tracing, LogTimeMode, LoggingConfig},
    messaging::message_builder::build_raw_message,
    protocol::DEFAULT_MAGIC_STRING,
    udp_unicast::{UdpUnicastEndpoint, UdpUnicastEvent},
};
use miette::{IntoDiagnostic, Result, WrapErr};
use tokio::sync::mpsc;
use tracing::{error, info};

#[tokio::main]
async fn main() -> Result<()> {
    init_tracing(&LoggingConfig {
        directory: "logs",
        file_name: "demo_udp_unicast_sender.log",
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

    let local_addr = "127.0.0.1:0".parse().into_diagnostic()?;
    let remote_addr = "127.0.0.1:9100".parse().into_diagnostic()?;

    let endpoint = UdpUnicastEndpoint::new(local_addr, cfg.clone(), tx);
    let handle = endpoint.handle();

    tokio::spawn(async move {
        if let Err(err) = endpoint.run().await {
            error!(error = %err, "udp unicast sender endpoint failed");
        }
    });

    let mut replies_seen = 0usize;

    while let Some(event) = rx.recv().await {
        match event {
            UdpUnicastEvent::Bound { local_addr } => {
                info!(%local_addr, %remote_addr, "sender bound");

                let msg1 = build_raw_message(cfg.expected_magic_string, 1, b"udp ping payload");

                let msg2 = build_raw_message(cfg.expected_magic_string, 2, b"udp echo me back");

                let msg3 =
                    build_raw_message(cfg.expected_magic_string, 3, b"udp deferred work please");

                handle
                    .send_to_async(remote_addr, &msg1)
                    .await
                    .into_diagnostic()
                    .wrap_err("failed to send udp ping")?;

                handle
                    .send_to_async(remote_addr, &msg2)
                    .await
                    .into_diagnostic()
                    .wrap_err("failed to send udp echo")?;

                handle
                    .send_to_async(remote_addr, &msg3)
                    .await
                    .into_diagnostic()
                    .wrap_err("failed to send udp deferred")?;
            }

            UdpUnicastEvent::Closed { local_addr } => {
                info!(%local_addr, "sender endpoint closed");
                break;
            }

            UdpUnicastEvent::DatagramReceived { datagram } => {
                let payload_text = String::from_utf8_lossy(datagram.payload.as_slice());

                match datagram.header.message_id {
                    1001 => {
                        info!(
                            from = %datagram.from,
                            payload = %payload_text,
                            "received udp ping reply"
                        );
                        replies_seen += 1;
                    }
                    1002 => {
                        info!(
                            from = %datagram.from,
                            payload = %payload_text,
                            "received udp echo reply"
                        );
                        replies_seen += 1;
                    }
                    1003 => {
                        info!(
                            from = %datagram.from,
                            payload = %payload_text,
                            "received udp deferred reply"
                        );
                        replies_seen += 1;
                    }
                    other => {
                        info!(
                            from = %datagram.from,
                            message_id = other,
                            payload = %payload_text,
                            "received unexpected udp reply"
                        );
                    }
                }

                if replies_seen >= 3 {
                    let _ = handle.close().await;
                }
            }
        }
    }

    Ok(())
}
