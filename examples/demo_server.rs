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

mod app;

use app::handlers;
use core_net::{
    config::{KeepAliveOption, SendOption, TcpServerConfig},
    logging::{init_tracing, LogTimeMode, LoggingConfig},
    messaging::dispatcher::{MessageContext, MessageDispatcherBuilder, ReplyHandle},
    messaging::message::Message,
    protocol::DEFAULT_MAGIC_STRING,
    tcp_server::{AppEvent, TcpServer},
};
use miette::{IntoDiagnostic, Result, WrapErr};
use tokio::sync::mpsc;
use tracing::{error, info};

#[tokio::main]
async fn main() -> Result<()> {
    init_tracing(&LoggingConfig {
        directory: "logs",
        file_name: "demo_server.log",
        max_bytes: 5 * 1024 * 1024,
        keep_files: 10,
        level_filter: "info",
        time_mode: LogTimeMode::Utc,
        also_stderr: true,
    })
    .wrap_err("failed to initialize tracing")?;

    let (tx, mut rx) = mpsc::channel(1024);

    let mut cfg = TcpServerConfig::default();
    cfg.socket.send_option = SendOption::NagleOff;
    cfg.socket.keep_alive = KeepAliveOption::On;
    cfg.socket.send_buffer_size = Some(256 * 1024);
    cfg.socket.recv_buffer_size = Some(256 * 1024);
    cfg.send_pool_msg_size = 8192;
    cfg.recv_pool_msg_count = 128;
    cfg.recv_pool_msg_size = 8192;
    cfg.recv_chunk_size = 64 * 1024;
    cfg.expected_magic_string = DEFAULT_MAGIC_STRING;

    let mut dispatcher_builder = MessageDispatcherBuilder::new();
    handlers::register_handlers(&mut dispatcher_builder);
    let dispatcher = dispatcher_builder.build();

    let server = TcpServer::bind("127.0.0.1:9000".parse().into_diagnostic()?, cfg.clone(), tx)
        .await
        .into_diagnostic()
        .wrap_err("failed to bind TCP server")?;

    let handle = server.handle();

    tokio::spawn(async move {
        if let Err(err) = server.run().await {
            error!(error = %err, "server failed");
        }
    });

    info!("demo server listening on 127.0.0.1:9000");

    while let Some(event) = rx.recv().await {
        match event {
            AppEvent::ClientConnected {
                client_id,
                peer_addr,
            } => {
                info!(client_id, %peer_addr, "client connected");
            }

            AppEvent::ClientDisconnected {
                client_id,
                peer_addr,
            } => {
                info!(client_id, %peer_addr, "client disconnected");
            }

            AppEvent::MessageReceived {
                client_id,
                peer_addr,
                message,
            } => {
                let app_message = Message::from_tcp_server(peer_addr, message);

                let ctx = MessageContext {
                    source_addr: Some(peer_addr),
                    expected_magic: cfg.expected_magic_string,
                    reply_handle: ReplyHandle::TcpServer {
                        client_id,
                        handle: handle.clone(),
                    },
                };

                dispatcher.dispatch(ctx, app_message).await;
            }
        }
    }

    Ok(())
}
