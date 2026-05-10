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
    udp_unicast::{UdpUnicastEndpoint, UdpUnicastEvent},
};

use miette::{IntoDiagnostic, Result, WrapErr};

use tokio::sync::mpsc;

use tracing::{error, info};

/// Demo UDP unicast sender entry point.
///
/// This application demonstrates:
///
/// - creating a UDP unicast endpoint
/// - sending core-net framed messages to a specific UDP peer
/// - receiving UDP replies
/// - using pooled UDP send/receive buffers
/// - validating request/reply behaviour over connectionless UDP
///
/// High-level flow:
///
/// ```text
/// bind UDP socket
///   -> send ping
///   -> receive ping reply
///   -> send echo
///   -> receive echo reply
///   -> send deferred-work request
///   -> receive deferred reply
///   -> close endpoint
/// ```
///
/// This pairs with the UDP unicast listener demo, which dispatches incoming
/// messages through the normal core-net message dispatcher.
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
        file_name: "demo_udp_unicast_sender.log",
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
    // - endpoint closure
    let (tx, mut rx) = mpsc::channel(1024);

    // Configure UDP unicast endpoint.
    let mut cfg = UdpConfig::default();

    // Maximum supported UDP datagram size.
    cfg.max_datagram_size = 65507;

    // Outbound pooled message buffer size.
    cfg.send_pool_msg_size = 8192;

    // Receive pool configuration.
    //
    // Incoming replies are stored in reusable pooled buffers to reduce
    // allocation churn.
    cfg.recv_pool_msg_count = 128;
    cfg.recv_pool_msg_size = 8192;

    // Expected protocol magic string used by the core-net parser layer.
    cfg.expected_magic_string = DEFAULT_MAGIC_STRING;

    // Bind to an ephemeral UDP port on localhost.
    //
    // The OS selects the source port.
    let local_addr = "127.0.0.1:0".parse().into_diagnostic()?;

    // Remote UDP listener address.
    let remote_addr = "127.0.0.1:9100".parse().into_diagnostic()?;

    // Create UDP unicast endpoint.
    let endpoint = UdpUnicastEndpoint::new(local_addr, cfg.clone(), tx);

    // Handle used for async send/close operations.
    let handle = endpoint.handle();

    // Run UDP endpoint in the background.
    tokio::spawn(async move {
        if let Err(err) = endpoint.run().await {
            error!(error = %err, "udp unicast sender endpoint failed");
        }
    });

    // Track replies so the demo can terminate once all expected responses
    // have arrived.
    let mut replies_seen = 0usize;

    let mut saw_ping_reply = false;
    let mut saw_echo_reply = false;
    let mut saw_deferred_reply = false;

    // Main sender endpoint event loop.
    while let Some(event) = rx.recv().await {
        match event {
            // Socket successfully bound.
            UdpUnicastEvent::Bound { local_addr } => {
                info!(%local_addr, %remote_addr, "sender bound");

                // Send initial UDP ping.
                //
                // Listener handler:
                //
                // message id 1 -> reply id 1001
                let msg1 = build_raw_message(
                    cfg.expected_magic_string,
                    1,
                    b"udp ping payload",
                    None,
                    None,
                );

                handle
                    .send_to_async(remote_addr, &msg1)
                    .await
                    .into_diagnostic()
                    .wrap_err("failed to send udp ping")?;
            }

            // Endpoint closed.
            UdpUnicastEvent::Closed { local_addr } => {
                info!(%local_addr, "sender endpoint closed");

                break;
            }

            // UDP datagram reply received.
            UdpUnicastEvent::DatagramReceived { datagram } => {
                let payload_text = String::from_utf8_lossy(datagram.payload.as_slice());

                match datagram.header.message_id {
                    // Ping reply.
                    1001 => {
                        info!(
                            from = %datagram.from,
                            payload = %payload_text,
                            "received udp ping reply"
                        );

                        replies_seen += 1;
                        saw_ping_reply = true;

                        // Send UDP echo request.
                        //
                        // Listener handler:
                        //
                        // message id 2 -> reply id 1002
                        let msg2 = build_raw_message(
                            cfg.expected_magic_string,
                            2,
                            b"udp echo me back",
                            None,
                            None,
                        );

                        handle
                            .send_to_async(remote_addr, &msg2)
                            .await
                            .into_diagnostic()
                            .wrap_err("failed to send udp echo")?;
                    }

                    // Echo reply.
                    1002 => {
                        info!(
                            from = %datagram.from,
                            payload = %payload_text,
                            "received udp echo reply"
                        );

                        replies_seen += 1;
                        saw_echo_reply = true;

                        // Send UDP deferred-work request.
                        //
                        // Listener handler:
                        //
                        // message id 3 -> reply id 1003
                        let msg3 = build_raw_message(
                            cfg.expected_magic_string,
                            3,
                            b"udp deferred work please",
                            None,
                            None,
                        );

                        handle
                            .send_to_async(remote_addr, &msg3)
                            .await
                            .into_diagnostic()
                            .wrap_err("failed to send udp deferred")?;
                    }

                    // Deferred-work reply.
                    1003 => {
                        info!(
                            from = %datagram.from,
                            payload = %payload_text,
                            "received udp deferred reply"
                        );

                        replies_seen += 1;
                        saw_deferred_reply = true;
                    }

                    // Anything else is logged for diagnostics.
                    other => {
                        info!(
                            from = %datagram.from,
                            message_id = other,
                            payload = %payload_text,
                            "received unexpected udp reply"
                        );
                    }
                }

                // Close endpoint once all expected demo replies have arrived.
                if replies_seen >= 3 || (saw_ping_reply && saw_echo_reply && saw_deferred_reply) {
                    let _ = handle.close().await;
                }
            }
        }
    }

    Ok(())
}
