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
    config::UdpConfig,
    logging::{LogTimeMode, LoggingConfig, init_tracing},
    messaging::message_builder::build_raw_message,
    protocol::DEFAULT_MAGIC_STRING,
    udp_broadcast::{UdpBroadcastEndpoint, UdpBroadcastEvent},
};

use miette::{IntoDiagnostic, Result, WrapErr};

use tokio::sync::mpsc;

use tracing::{error, info};

/// Demo UDP broadcast sender entry point.
///
/// This application demonstrates:
///
/// - creating a UDP broadcast-capable endpoint
/// - sending core-net framed messages over UDP broadcast
/// - receiving UDP replies
/// - using the same message IDs as the TCP demo handlers
///
/// High-level flow:
///
/// ```text
/// bind UDP socket
///   -> send broadcast ping
///   -> receive ping reply
///   -> send broadcast echo
///   -> receive echo reply
///   -> close endpoint
/// ```
///
/// This demo is useful for validating:
///
/// - UDP broadcast socket setup
/// - pooled UDP send/receive buffers
/// - request/reply behaviour over connectionless UDP
/// - dispatcher compatibility across TCP and UDP transports
#[tokio::main]
async fn main() -> Result<()> {
    // Initialise structured tracing/logging.
    //
    // Logs are written both:
    //
    // - to rotating log files
    // - to stderr/console
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

    // Event channel used by the UDP endpoint to publish:
    //
    // - bind events
    // - received datagrams
    // - socket closure
    let (tx, mut rx) = mpsc::channel(1024);

    // Configure UDP broadcast endpoint.
    let mut cfg = UdpConfig::default();

    // Maximum supported UDP datagram size.
    cfg.max_datagram_size = 65507;

    // Outbound pooled message buffer size.
    cfg.send_pool_msg_size = 8192;

    // Receive pool configuration.
    //
    // Incoming replies are stored in reusable buffers to reduce allocation
    // churn.
    cfg.recv_pool_msg_count = 128;
    cfg.recv_pool_msg_size = 8192;

    // Expected protocol magic string for core-net message validation.
    cfg.expected_magic_string = DEFAULT_MAGIC_STRING;

    // Enable socket broadcast support.
    //
    // This is required before sending to broadcast addresses.
    cfg.socket.broadcast = true;

    // Bind to an ephemeral local UDP port on all interfaces.
    //
    // The OS chooses the local source port.
    let local_addr = "0.0.0.0:0".parse().into_diagnostic()?;

    // Create broadcast endpoint.
    let endpoint = UdpBroadcastEndpoint::new(local_addr, cfg.clone(), tx);

    // Handle used for async send/close operations.
    let handle = endpoint.handle();

    // Run UDP endpoint in the background.
    tokio::spawn(async move {
        if let Err(err) = endpoint.run().await {
            error!(error = %err, "udp broadcast sender endpoint failed");
        }
    });

    // For local testing on one machine, 127.255.255.255 often works better
    // than 255.255.255.255 because it stays on loopback.
    //
    // On a real LAN this would normally be something like:
    //
    // - 192.168.1.255:9200
    // - or 255.255.255.255:9200 depending on routing/firewall behaviour.
    let broadcast_addr = "127.255.255.255:9200".parse().into_diagnostic()?;

    // Track replies so the demo can terminate cleanly once both expected
    // responses have arrived.
    let mut replies_seen = 0usize;

    let mut saw_ping_reply = false;
    let mut saw_echo_reply = false;

    // Main event loop for the broadcast sender endpoint.
    while let Some(event) = rx.recv().await {
        match event {
            // Socket successfully bound.
            UdpBroadcastEvent::Bound { local_addr } => {
                info!(%local_addr, %broadcast_addr, "broadcast sender bound");

                // Send initial broadcast ping.
                //
                // Server/listener handler:
                //
                // message id 1 -> reply id 1001
                let msg1 = build_raw_message(
                    cfg.expected_magic_string,
                    1,
                    b"broadcast ping payload",
                    None,
                    None,
                );

                handle
                    .send_to_async(broadcast_addr, &msg1)
                    .await
                    .into_diagnostic()
                    .wrap_err("failed to send broadcast ping")?;
            }

            // Endpoint closed.
            UdpBroadcastEvent::Closed { local_addr } => {
                info!(%local_addr, "broadcast sender endpoint closed");

                break;
            }

            // UDP datagram reply received.
            UdpBroadcastEvent::DatagramReceived { datagram } => {
                let payload_text = String::from_utf8_lossy(datagram.payload.as_slice());

                match datagram.header.message_id {
                    // Ping reply.
                    1001 => {
                        info!(
                            from = %datagram.from,
                            payload = %payload_text,
                            "received broadcast ping reply"
                        );

                        replies_seen += 1;
                        saw_ping_reply = true;

                        // Now send broadcast echo request.
                        //
                        // Server/listener handler:
                        //
                        // message id 2 -> reply id 1002
                        let msg2 = build_raw_message(
                            cfg.expected_magic_string,
                            2,
                            b"broadcast echo me back",
                            None,
                            None,
                        );

                        handle
                            .send_to_async(broadcast_addr, &msg2)
                            .await
                            .into_diagnostic()
                            .wrap_err("failed to send broadcast echo")?;
                    }

                    // Echo reply.
                    1002 => {
                        info!(
                            from = %datagram.from,
                            payload = %payload_text,
                            "received broadcast echo reply"
                        );

                        replies_seen += 1;
                        saw_echo_reply = true;
                    }

                    // Anything else is logged for diagnostics.
                    other => {
                        info!(
                            from = %datagram.from,
                            message_id = other,
                            payload = %payload_text,
                            "received unexpected broadcast reply"
                        );
                    }
                }

                // Close endpoint after the expected demo responses are seen.
                if replies_seen >= 2 || (saw_ping_reply && saw_echo_reply) {
                    let _ = handle.close().await;
                }
            }
        }
    }

    Ok(())
}
