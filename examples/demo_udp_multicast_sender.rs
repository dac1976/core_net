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
    config::{MulticastGroup, UdpMulticastConfig},
    logging::{LogTimeMode, LoggingConfig, init_tracing},
    messaging::message_builder::build_raw_message,
    protocol::DEFAULT_MAGIC_STRING,
    udp_multicast::{UdpMulticastEndpoint, UdpMulticastEvent},
};

use miette::{Result, WrapErr};

use std::net::Ipv4Addr;

use tokio::sync::mpsc;

use tracing::{error, info};

/// Demo UDP multicast sender entry point.
///
/// This application demonstrates:
///
/// - creating a UDP multicast endpoint
/// - sending core-net framed messages to a multicast group
/// - receiving multicast replies
/// - using pooled UDP send/receive buffers
/// - validating request/reply behaviour over multicast UDP
///
/// High-level flow:
///
/// ```text
/// bind UDP socket
///   -> send multicast ping
///   -> receive ping reply
///   -> send multicast echo
///   -> receive echo reply
///   -> close endpoint
/// ```
///
/// This pairs with the multicast listener demo, which joins the multicast
/// group and dispatches incoming messages through the normal core-net
/// message dispatcher.
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
        file_name: "demo_udp_multicast_sender.log",
        max_bytes: 5 * 1024 * 1024,
        keep_files: 10,
        level_filter: "info",
        time_mode: LogTimeMode::Utc,
        also_stderr: true,
    })
    .wrap_err("failed to initialize tracing")?;

    // Event channel used by the multicast endpoint to publish:
    //
    // - bind events
    // - group join events
    // - received datagrams
    // - endpoint closure
    let (tx, mut rx) = mpsc::channel(1024);

    // Configure multicast endpoint.
    let mut cfg = UdpMulticastConfig::default();

    // Configure IPv4 multicast group destination.
    //
    // This sender binds locally to:
    //
    // 0.0.0.0:9301
    //
    // and sends to multicast group:
    //
    // 239.255.0.1:9300
    //
    // The listener demo binds to port 9300 and joins this group.
    cfg.group = MulticastGroup::V4 {
        local_bind_addr: "0.0.0.0:9301".parse().unwrap(),

        group_addr: Ipv4Addr::new(239, 255, 0, 1),

        group_port: 9300,

        // UNSPECIFIED lets the OS choose the multicast interface.
        //
        // In production this may be set to the IP address of a specific NIC.
        interface_addr: Ipv4Addr::UNSPECIFIED,
    };

    // Maximum supported UDP datagram size.
    cfg.max_datagram_size = 65507;

    // Outbound pooled message buffer size.
    cfg.send_pool_msg_size = 8192;

    // Receive pool configuration.
    //
    // Incoming replies are stored in reusable buffers to reduce allocation
    // churn during testing.
    cfg.recv_pool_msg_count = 128;
    cfg.recv_pool_msg_size = 8192;

    // Expected protocol magic string used by the core-net parser layer.
    cfg.expected_magic_string = DEFAULT_MAGIC_STRING;

    // IPv4 multicast TTL.
    //
    // TTL=1 keeps multicast traffic on the local subnet.
    cfg.send_ttl_v4 = 1;

    // Enable multicast loopback.
    //
    // This allows the sender host to receive multicast datagrams that it
    // sends itself. Useful for local testing.
    cfg.multicast_loop_v4 = true;

    // The sender does not need to join the multicast group on startup.
    //
    // It sends to the group and receives direct replies. If testing loopback
    // reception of its own group traffic, this can be enabled.
    cfg.join_group_on_start = false;

    // Create multicast endpoint.
    let endpoint = UdpMulticastEndpoint::new(cfg.clone(), tx);

    // Handle used for async send/close operations.
    let handle = endpoint.handle();

    // Run multicast endpoint in the background.
    tokio::spawn(async move {
        if let Err(err) = endpoint.run().await {
            error!(error = %err, "udp multicast sender failed");
        }
    });

    // Track replies so the demo can terminate once expected responses arrive.
    let mut replies_seen = 0usize;

    let mut saw_ping_reply = false;
    let mut saw_echo_reply = false;

    // Main multicast sender event loop.
    while let Some(event) = rx.recv().await {
        match event {
            // Socket successfully bound.
            UdpMulticastEvent::Bound { local_addr } => {
                info!(%local_addr, "multicast sender bound");

                // Send initial multicast ping.
                //
                // Listener handler:
                //
                // message id 1 -> reply id 1001
                let msg1 = build_raw_message(
                    cfg.expected_magic_string,
                    1,
                    b"multicast ping payload",
                    None,
                    None,
                );

                if let Err(err) = handle.send_to_group_async(&msg1).await {
                    error!(error = %err, "failed to send multicast ping");
                }
            }

            // Joined multicast group.
            //
            // This should normally not occur with join_group_on_start=false,
            // but the event is handled for completeness.
            UdpMulticastEvent::Joined { local_addr, group } => {
                info!(%local_addr, %group, "multicast sender joined group");
            }

            // Endpoint closed.
            UdpMulticastEvent::Closed { local_addr } => {
                info!(%local_addr, "multicast sender closed");

                break;
            }

            // Datagram received.
            UdpMulticastEvent::DatagramReceived { datagram } => {
                let payload_text = String::from_utf8_lossy(datagram.payload.as_slice());

                match datagram.header.message_id {
                    // Ignore original multicast request datagrams if they are
                    // observed locally due to multicast loopback.
                    1 | 2 => {
                        info!(
                            from = %datagram.from,
                            message_id = datagram.header.message_id,
                            "ignoring original multicast request datagram"
                        );
                    }

                    // Ping reply.
                    1001 => {
                        info!(
                            from = %datagram.from,
                            payload = %payload_text,
                            "received multicast ping reply"
                        );

                        replies_seen += 1;
                        saw_ping_reply = true;

                        // Send multicast echo request.
                        //
                        // Listener handler:
                        //
                        // message id 2 -> reply id 1002
                        let msg2 = build_raw_message(
                            cfg.expected_magic_string,
                            2,
                            b"multicast echo me back",
                            None,
                            None,
                        );

                        if let Err(err) = handle.send_to_group_async(&msg2).await {
                            error!(error = %err, "failed to send multicast echo");
                        }
                    }

                    // Echo reply.
                    1002 => {
                        info!(
                            from = %datagram.from,
                            payload = %payload_text,
                            "received multicast echo reply"
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
                            "received unexpected multicast reply"
                        );
                    }
                }

                // Close endpoint after expected demo responses have arrived.
                if replies_seen >= 2 || (saw_ping_reply && saw_echo_reply) {
                    let _ = handle.close().await;
                }
            }
        }
    }

    Ok(())
}
