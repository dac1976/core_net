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
/// Reuses the same dispatcher handlers used by the TCP examples:
///
/// - ping
/// - echo
/// - deferred work
/// - MessagePack ping
///
/// This demonstrates that the dispatcher/message layer is transport
/// independent.
mod app;

use app::handlers;

use core_net::{
    config::UdpConfig,
    logging::{LogTimeMode, LoggingConfig, init_tracing},
    messaging::dispatcher::{BoxFuture, MessageContext, MessageDispatcherBuilder, ReplyHandle},
    messaging::message::Message,
    protocol::DEFAULT_MAGIC_STRING,
    udp_broadcast::{UdpBroadcastEndpoint, UdpBroadcastEvent},
};

use miette::{IntoDiagnostic, Result, WrapErr};

use std::{net::SocketAddr, sync::Arc};

use tokio::sync::mpsc;

use tracing::{error, info};

/// Demo UDP broadcast listener entry point.
///
/// This application demonstrates:
///
/// - UDP broadcast reception
/// - dispatcher-based message routing
/// - transport-independent message handling
/// - async UDP reply handling
/// - pooled UDP datagram buffers
///
/// High-level architecture:
///
/// ```text
/// UDP socket
///   -> UdpBroadcastEndpoint
///   -> UdpBroadcastEvent::DatagramReceived
///   -> MessageDispatcher
///   -> registered async handlers
/// ```
///
/// Unlike the TCP example:
///
/// - there are no persistent client sessions
/// - replies are routed directly to peer socket addresses
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
        file_name: "demo_udp_broadcast_listener.log",
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

    // Configure UDP endpoint and buffer pool behaviour.
    let mut cfg = UdpConfig::default();

    // Maximum supported UDP datagram size.
    cfg.max_datagram_size = 65507;

    // Outbound pooled message buffer size.
    cfg.send_pool_msg_size = 8192;

    // Receive buffer pool configuration.
    //
    // Incoming datagrams are stored in reusable pooled buffers to reduce
    // allocation churn under sustained receive load.
    cfg.recv_pool_msg_count = 128;
    cfg.recv_pool_msg_size = 8192;

    // Expected protocol magic string used by the core_net parser layer.
    cfg.expected_magic_string = DEFAULT_MAGIC_STRING;

    // UDP socket bind address.
    //
    // Binding to 0.0.0.0 allows reception on all local interfaces.
    let local_addr = "0.0.0.0:9200".parse().into_diagnostic()?;

    // Create UDP broadcast endpoint.
    let endpoint = UdpBroadcastEndpoint::new(local_addr, cfg.clone(), tx);

    // Handle used for async outbound replies.
    let handle = endpoint.handle();

    // Build generic async UDP send function used by dispatcher reply
    // routing.
    //
    // This abstracts the underlying transport so dispatcher handlers can
    // remain transport independent.
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

    // Run UDP endpoint in background task.
    tokio::spawn(async move {
        if let Err(err) = endpoint.run().await {
            error!(error = %err, "udp broadcast listener failed");
        }
    });

    info!("udp broadcast listener starting on {}", local_addr);

    // Main UDP event loop.
    while let Some(event) = rx.recv().await {
        match event {
            // UDP socket successfully bound.
            UdpBroadcastEvent::Bound { local_addr } => {
                info!(%local_addr, "broadcast listener bound");
            }

            // UDP endpoint closed.
            UdpBroadcastEvent::Closed { local_addr } => {
                info!(%local_addr, "broadcast listener closed");

                break;
            }

            // UDP datagram received.
            UdpBroadcastEvent::DatagramReceived { datagram } => {
                // Source peer address.
                let peer = datagram.from;

                // Convert UDP datagram into generic dispatcher message type.
                //
                // This provides transport-independent handling above the UDP
                // layer.
                let app_message = Message::from_udp(datagram);

                // Build dispatcher context.
                //
                // For UDP, replies are routed using:
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

                // Dispatch message to registered async handler based on
                // message id.
                dispatcher.dispatch(ctx, app_message).await;
            }
        }
    }

    Ok(())
}
