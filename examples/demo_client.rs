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

use core_net::{
    config::{KeepAliveOption, SendOption, TcpClientConfig},
    logging::{LogTimeMode, LoggingConfig, init_tracing},
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

/// Example typed MessagePack ping request.
///
/// This mirrors the request struct handled by the server-side
/// MessagePack demo handler.
///
/// The payload is serialized with Serde + MessagePack, then wrapped inside
/// a normal core-net message with archive type `MessagePack`.
#[derive(Debug, Serialize, Deserialize)]
struct MsgPackPingRequest {
    /// Name or label to include in the request.
    name: String,

    /// Example numeric value.
    count: u32,
}

/// Example typed MessagePack ping reply.
///
/// This is decoded from the MessagePack response payload returned by the
/// server.
#[derive(Debug, Serialize, Deserialize)]
struct MsgPackPingReply {
    /// Indicates whether the server handled the request successfully.
    ok: bool,

    /// Human-readable reply message.
    message: String,
}

/// Demo TCP client entry point.
///
/// This client exercises the core_net TCP client and dispatcher examples by
/// sending a sequence of messages to the demo server:
///
/// ```text
/// 1. Raw ping
/// 2. Raw echo
/// 3. Raw deferred-work request
/// 4. MessagePack ping
/// ```
///
/// Expected replies:
///
/// ```text
/// 1001 Raw         -> PONG
/// 1002 Raw         -> echoed payload
/// 1003 Raw         -> deferred ACK
/// 1010 MessagePack -> typed MessagePack reply
/// ```
///
/// The demo exits after all expected replies are observed.
#[tokio::main]
async fn main() -> Result<()> {
    // Initialise structured logging using the shared core_net logging
    // subsystem.
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

    // Event channel used by TcpClient to report:
    //
    // - connection established
    // - disconnection
    // - received messages
    let (tx, mut rx) = mpsc::channel(1024);

    // Configure TCP client socket and core_net buffering behaviour.
    let mut cfg = TcpClientConfig::default();

    // Disable Nagle to make small demo messages send immediately.
    cfg.socket.send_option = SendOption::NagleOff;

    // Enable TCP keepalive.
    cfg.socket.keep_alive = KeepAliveOption::On;

    // Increase OS socket buffers for more realistic high-throughput testing.
    cfg.socket.send_buffer_size = Some(256 * 1024);
    cfg.socket.recv_buffer_size = Some(256 * 1024);

    // Size of pooled outbound messages used by core_net.
    cfg.send_pool_msg_size = 8192;

    // Receive pool configuration.
    //
    // Incoming messages are stored in reusable buffers to reduce allocation
    // churn.
    cfg.recv_pool_msg_count = 128;
    cfg.recv_pool_msg_size = 8192;

    // TCP read chunk size.
    cfg.recv_chunk_size = 64 * 1024;

    // Expected protocol magic string for validating received core_net
    // messages.
    cfg.expected_magic_string = DEFAULT_MAGIC_STRING;

    // Demo server endpoint.
    let server_addr = "127.0.0.1:9000".parse().into_diagnostic()?;

    let client = TcpClient::new(server_addr, cfg.clone(), tx);

    // Handle is used by this application task to send messages/disconnect.
    let handle = client.handle();

    // Run the TCP client in the background.
    tokio::spawn(async move {
        if let Err(err) = client.run().await {
            error!(error = %err, "client failed");
        }
    });

    // Track replies so the demo can terminate when all expected responses
    // have been received.
    let mut replies_seen = 0usize;

    let mut saw_ping_reply = false;
    let mut saw_echo_reply = false;
    let mut saw_deferred_reply = false;
    let mut saw_msgpack_reply = false;

    // Main client event loop.
    while let Some(event) = rx.recv().await {
        match event {
            ClientEvent::Connected {
                server_addr,
                local_addr,
            } => {
                info!(%server_addr, %local_addr, "client connected");

                // Start the demo chain by sending a raw ping message.
                //
                // Server handler:
                //
                // message id 1 -> reply id 1001
                let ping =
                    build_raw_message(cfg.expected_magic_string, 1, b"ping payload", None, None);

                handle
                    .send_to_server_async(&ping)
                    .await
                    .into_diagnostic()
                    .wrap_err("failed to send ping message")?;
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
                    // Ping response.
                    (1001, ArchiveType::Raw) => {
                        let payload_text = String::from_utf8_lossy(message.payload.as_slice());

                        info!(
                            %server_addr,
                            payload = %payload_text,
                            "received ping reply"
                        );

                        replies_seen += 1;
                        saw_ping_reply = true;

                        // Next: send echo message.
                        //
                        // Server handler:
                        //
                        // message id 2 -> reply id 1002
                        let echo = build_raw_message(
                            cfg.expected_magic_string,
                            2,
                            b"echo me back",
                            None,
                            None,
                        );

                        handle
                            .send_to_server_async(&echo)
                            .await
                            .into_diagnostic()
                            .wrap_err("failed to send echo message")?;
                    }

                    // Echo response.
                    (1002, ArchiveType::Raw) => {
                        let payload_text = String::from_utf8_lossy(message.payload.as_slice());

                        info!(
                            %server_addr,
                            payload = %payload_text,
                            "received echo reply"
                        );

                        replies_seen += 1;
                        saw_echo_reply = true;

                        // Next: send deferred-work message.
                        //
                        // Server handler:
                        //
                        // message id 3 -> reply id 1003
                        let deferred = build_raw_message(
                            cfg.expected_magic_string,
                            3,
                            b"please do deferred work",
                            None,
                            None,
                        );

                        handle
                            .send_to_server_async(&deferred)
                            .await
                            .into_diagnostic()
                            .wrap_err("failed to send deferred message")?;
                    }

                    // Deferred-work response.
                    (1003, ArchiveType::Raw) => {
                        let payload_text = String::from_utf8_lossy(message.payload.as_slice());

                        info!(
                            %server_addr,
                            payload = %payload_text,
                            "received deferred reply"
                        );

                        replies_seen += 1;
                        saw_deferred_reply = true;

                        // Next: send structured MessagePack ping.
                        //
                        // Server handler:
                        //
                        // message id 10 -> reply id 1010
                        let msgpack_req = MsgPackPingRequest {
                            name: "Duncan".to_string(),
                            count: 42,
                        };

                        let msgpack_payload = to_msgpack_vec(&msgpack_req)
                            .into_diagnostic()
                            .wrap_err("failed to encode MessagePack request")?;

                        let msgpack_message = build_msgpack_message(
                            cfg.expected_magic_string,
                            10,
                            &msgpack_payload,
                            None,
                            None,
                        );

                        handle
                            .send_to_server_async(&msgpack_message)
                            .await
                            .into_diagnostic()
                            .wrap_err("failed to send MessagePack message")?;
                    }

                    // MessagePack response.
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
                        saw_msgpack_reply = true;
                    }

                    // Any other reply is logged but not treated as fatal.
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

                // Disconnect after all expected responses have arrived.
                if replies_seen >= 4
                    || (saw_ping_reply && saw_echo_reply && saw_deferred_reply && saw_msgpack_reply)
                {
                    let _ = handle.disconnect().await;
                }
            }
        }
    }

    Ok(())
}
