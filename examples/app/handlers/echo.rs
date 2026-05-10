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

use core_net::messaging::{
    dispatcher::{MessageContext, MessageDispatcherBuilder},
    message::Message,
    message_builder::build_raw_message,
};

use tracing::{error, info, instrument};

/// Registers the echo message handler with the dispatcher.
///
/// Message ID mapping:
///
/// ```text
/// 2 -> handle_echo()
/// ```
///
/// The echo handler is intentionally simple and useful for:
///
/// - connectivity testing
/// - request/reply validation
/// - protocol verification
/// - latency testing
/// - dispatcher testing
///
/// The handler immediately replies with the same payload data it received.
pub fn register(builder: &mut MessageDispatcherBuilder) {
    builder.register(2, handle_echo);
}

/// Handles incoming echo messages.
///
/// Behaviour:
///
/// ```text
/// receive payload
///   -> build reply message
///   -> send identical payload back
/// ```
///
/// Reply message id:
///
/// ```text
/// 1002
/// ```
///
/// This demonstrates the simplest request/reply pattern using:
///
/// - MessageDispatcher
/// - MessageContext
/// - async reply sending
///
/// The tracing `instrument` macro automatically creates structured tracing
/// spans for this handler.
///
/// `skip(ctx, message)` avoids logging large/internal structures.
#[instrument(skip(ctx, message))]
pub async fn handle_echo(ctx: MessageContext, message: Message) {
    info!(
        message_id = message.header.message_id,
        payload_len = message.payload.len(),
        "handling echo message"
    );

    // Build reply message using the same payload bytes received from
    // the client.
    //
    // No payload modification is performed.
    //
    // The response uses:
    //
    // - same expected protocol magic string
    // - reply message id = 1002
    let response = build_raw_message(
        ctx.expected_magic,
        1002,
        message.payload.as_slice(),
        None,
        None,
    );

    // Send async reply back to the originating peer.
    //
    // MessageContext internally knows how to route replies back through
    // the correct connection/session.
    if let Err(err) = ctx.send_reply(&response).await {
        error!(error = %err, "failed to send echo response");
    }
}
