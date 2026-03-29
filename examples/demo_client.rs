use core_net::{
    config::{KeepAliveOption, SendOption, TcpClientConfig},
    logging::{init_tracing, LogTimeMode, LoggingConfig},
    messaging::{
        message_builder::{build_msgpack_message, build_raw_message},
        msgpack_codec::{from_msgpack_slice, to_msgpack_vec},
    },
    protocol::{ArchiveType, DEFAULT_MAGIC_STRING},
    tcp_client::{ClientEvent, TcpClient},
};
use miette::{IntoDiagnostic, Result, WrapErr};
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;
use tracing::{error, info};

#[derive(Debug, Serialize, Deserialize)]
struct MsgPackPingRequest {
    name: String,
    count: u32,
}

#[derive(Debug, Serialize, Deserialize)]
struct MsgPackPingReply {
    ok: bool,
    message: String,
}

#[tokio::main]
async fn main() -> Result<()> {
    init_tracing(&LoggingConfig {
        directory: "logs",
        file_name: "demo_client.log",
        max_bytes: 5 * 1024 * 1024,
        keep_files: 10,
        level_filter: "info",
        time_mode: LogTimeMode::Utc,
        also_stderr: true,
    })
    .wrap_err("failed to initialize tracing")?;

    let (tx, mut rx) = mpsc::channel(1024);

    let mut cfg = TcpClientConfig::default();
    cfg.socket.send_option = SendOption::NagleOff;
    cfg.socket.keep_alive = KeepAliveOption::On;
    cfg.socket.send_buffer_size = Some(256 * 1024);
    cfg.socket.recv_buffer_size = Some(256 * 1024);
    cfg.send_pool_msg_size = 8192;
    cfg.recv_pool_msg_count = 128;
    cfg.recv_pool_msg_size = 8192;
    cfg.recv_chunk_size = 64 * 1024;
    cfg.expected_magic_string = DEFAULT_MAGIC_STRING;

    let server_addr = "127.0.0.1:9000".parse().into_diagnostic()?;
    let client = TcpClient::new(server_addr, cfg.clone(), tx);
    let handle = client.handle();

    tokio::spawn(async move {
        if let Err(err) = client.run().await {
            error!(error = %err, "client failed");
        }
    });

    let mut replies_seen = 0usize;

    while let Some(event) = rx.recv().await {
        match event {
            ClientEvent::Connected {
                server_addr,
                local_addr,
            } => {
                info!(%server_addr, %local_addr, "client connected");

                let ping = build_raw_message(cfg.expected_magic_string, 1, b"ping payload");

                let echo = build_raw_message(cfg.expected_magic_string, 2, b"echo me back");

                let deferred =
                    build_raw_message(cfg.expected_magic_string, 3, b"please do deferred work");

                let msgpack_req = MsgPackPingRequest {
                    name: "Duncan".to_string(),
                    count: 42,
                };

                let msgpack_payload = to_msgpack_vec(&msgpack_req)
                    .into_diagnostic()
                    .wrap_err("failed to encode MessagePack request")?;

                let msgpack_message =
                    build_msgpack_message(cfg.expected_magic_string, 10, &msgpack_payload);

                handle
                    .send_to_server_async(&ping)
                    .await
                    .into_diagnostic()
                    .wrap_err("failed to send ping message")?;

                handle
                    .send_to_server_async(&echo)
                    .await
                    .into_diagnostic()
                    .wrap_err("failed to send echo message")?;

                handle
                    .send_to_server_async(&deferred)
                    .await
                    .into_diagnostic()
                    .wrap_err("failed to send deferred message")?;

                handle
                    .send_to_server_async(&msgpack_message)
                    .await
                    .into_diagnostic()
                    .wrap_err("failed to send MessagePack message")?;
            }

            ClientEvent::Disconnected { server_addr } => {
                info!(%server_addr, "client disconnected");
                break;
            }

            ClientEvent::MessageReceived {
                server_addr,
                message,
            } => {
                match (message.header.message_id, message.header.archive_type) {
                    (1001, ArchiveType::Raw) => {
                        let payload_text = String::from_utf8_lossy(message.payload.as_slice());
                        info!(
                            %server_addr,
                            payload = %payload_text,
                            "received ping reply"
                        );
                        replies_seen += 1;
                    }

                    (1002, ArchiveType::Raw) => {
                        let payload_text = String::from_utf8_lossy(message.payload.as_slice());
                        info!(
                            %server_addr,
                            payload = %payload_text,
                            "received echo reply"
                        );
                        replies_seen += 1;
                    }

                    (1003, ArchiveType::Raw) => {
                        let payload_text = String::from_utf8_lossy(message.payload.as_slice());
                        info!(
                            %server_addr,
                            payload = %payload_text,
                            "received deferred reply"
                        );
                        replies_seen += 1;
                    }

                    (1010, ArchiveType::MessagePack) => {
                        let reply: MsgPackPingReply =
                            from_msgpack_slice(message.payload.as_slice())
                                .into_diagnostic()
                                .wrap_err("failed to decode MessagePack reply")?;

                        info!(
                            %server_addr,
                            ok = reply.ok,
                            message = %reply.message,
                            "received MessagePack reply"
                        );
                        replies_seen += 1;
                    }

                    (other_id, other_archive) => {
                        info!(
                            %server_addr,
                            message_id = other_id,
                            archive_type = ?other_archive,
                            payload_len = message.payload.len(),
                            "received unexpected reply"
                        );
                    }
                }

                if replies_seen >= 4 {
                    let _ = handle.disconnect().await;
                }
            }
        }
    }

    Ok(())
}
