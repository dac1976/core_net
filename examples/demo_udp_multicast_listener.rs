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

/// Demo application message handlers.
///
/// Reuses the same dispatcher handlers used by:
///
/// - TCP demos
/// - UDP broadcast demos
///
/// demonstrating transport-independent message handling.
mod app;

use app::handlers;

use core_net::{
    config::{MulticastGroup, UdpMulticastConfig},
    logging::{LogTimeMode, LoggingConfig, init_tracing},
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

/// Demo UDP multicast listener entry point.
///
/// This application demonstrates:
///
/// - IPv4 UDP multicast reception
/// - multicast group membership
/// - dispatcher-based message routing
/// - transport-independent handlers
/// - async UDP reply handling
/// - pooled UDP datagram buffers
///
/// High-level architecture:
///
/// ```text
/// multicast UDP socket
///   -> join multicast group
///   -> receive multicast datagrams
///   -> MessageDispatcher
///   -> registered async handlers
/// ```
///
/// Unlike TCP:
///
/// - no persistent sessions exist
/// - replies are routed directly using UDP peer addresses
///
/// Unlike broadcast:
///
/// - multicast traffic is limited to subscribed receivers
/// - multicast routing/scoping rules apply
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
        file_name: "demo_udp_multicast_listener.log",
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

    // Configure IPv4 multicast group membership.
    //
    // Group:
    //
    // 239.255.0.1:9300
    //
    // Binding:
    //
    // 0.0.0.0:9300
    //
    // allows reception on all local interfaces.
    cfg.group = MulticastGroup::V4 {
        local_bind_addr: "0.0.0.0:9300".parse().unwrap(),

        group_addr: Ipv4Addr::new(239, 255, 0, 1),

        group_port: 9300,

        // UNSPECIFIED allows the OS to choose the multicast interface.
        //
        // In production systems this may be pinned to a specific NIC IP.
        interface_addr: Ipv4Addr::UNSPECIFIED,
    };

    // Maximum supported UDP datagram size.
    cfg.max_datagram_size = 65507;

    // Outbound pooled message buffer size.
    cfg.send_pool_msg_size = 8192;

    // Receive pool configuration.
    //
    // Incoming datagrams are stored in reusable pooled buffers to minimise
    // allocation churn under sustained receive load.
    cfg.recv_pool_msg_count = 128;
    cfg.recv_pool_msg_size = 8192;

    // Expected protocol magic string used by the core_net parser layer.
    cfg.expected_magic_string = DEFAULT_MAGIC_STRING;

    // IPv4 multicast TTL.
    //
    // TTL=1 limits multicast packets to the local subnet.
    cfg.send_ttl_v4 = 1;

    // Enable multicast loopback.
    //
    // Allows this host to receive multicast packets that it also sends.
    //
    // Very useful for local testing/demo scenarios.
    cfg.multicast_loop_v4 = true;

    // Automatically join multicast group when endpoint starts.
    cfg.join_group_on_start = true;

    // Create multicast endpoint.
    let endpoint = UdpMulticastEndpoint::new(cfg.clone(), tx);

    // Handle used for async outbound replies.
    let handle = endpoint.handle();

    // Build generic async UDP send function used by dispatcher reply
    // routing.
    //
    // This abstraction allows dispatcher handlers to remain independent of
    // the underlying transport implementation.
    let send_fn = Arc::new(move |to: SocketAddr, bytes: Vec<u8>| {
        let handle = handle.clone();

        Box::pin(async move {
            handle
                .send_to_async(to, &bytes)
                .await
                .map_err(|e| e.to_string())
        }) as BoxFuture<Result<(), String>>
    });

    // Create dispatcher builder.
    let mut dispatcher_builder = MessageDispatcherBuilder::new();

    // Register demo handlers.
    handlers::register_handlers(&mut dispatcher_builder);

    // Finalise dispatcher.
    let dispatcher = dispatcher_builder.build();

    // Run multicast endpoint in background task.
    tokio::spawn(async move {
        if let Err(err) = endpoint.run().await {
            error!(error = %err, "udp multicast listener failed");
        }
    });

    info!("udp multicast listener starting");

    // Main multicast endpoint event loop.
    while let Some(event) = rx.recv().await {
        match event {
            // UDP socket successfully bound.
            UdpMulticastEvent::Bound { local_addr } => {
                info!(%local_addr, "multicast listener bound");
            }

            // Successfully joined multicast group.
            UdpMulticastEvent::Joined { local_addr, group } => {
                info!(
                    %local_addr,
                    %group,
                    "multicast listener joined group"
                );
            }

            // Endpoint closed.
            UdpMulticastEvent::Closed { local_addr } => {
                info!(%local_addr, "multicast listener closed");

                break;
            }

            // Multicast datagram received.
            UdpMulticastEvent::DatagramReceived { datagram } => {
                // Source peer/socket address.
                let peer = datagram.from;

                // Convert UDP datagram into transport-independent dispatcher
                // message type.
                let app_message = Message::from_udp(datagram);

                // Build dispatcher context.
                //
                // For UDP/multicast transports replies are routed using:
                //
                // - peer socket address
                // - generic async send function
                let ctx = MessageContext {
                    source_addr: Some(peer),

                    expected_magic: cfg.expected_magic_string,

                    reply_handle: ReplyHandle::Udp {
                        peer_addr: peer,

                        send_fn: send_fn.clone(),
                    },
                };

                // Dispatch message to the registered async handler based on
                // message id.
                dispatcher.dispatch(ctx, app_message).await;
            }
        }
    }

    Ok(())
}
