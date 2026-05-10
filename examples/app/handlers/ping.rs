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

/// Registers the ping handler with the dispatcher.
///
/// Message ID mapping:
///
/// ```text
/// 1 -> handle_ping()
/// ```
///
/// The ping handler is intended as the simplest possible connectivity and
/// request/reply validation example.
///
/// Typical uses:
///
/// - connectivity testing
/// - protocol sanity checks
/// - dispatcher verification
/// - latency testing
/// - integration testing
///
/// Unlike the echo handler, this handler ignores the incoming payload and
/// always responds with a fixed:
///
/// ```text
/// PONG
/// ```
pub fn register(builder: &mut MessageDispatcherBuilder) {
    builder.register(1, handle_ping);
}

/// Handles incoming ping requests.
///
/// Processing flow:
///
/// ```text
/// receive ping message
///   -> build raw reply
///   -> send PONG response
/// ```
///
/// Reply message id:
///
/// ```text
/// 1001
/// ```
///
/// The tracing `instrument` macro automatically creates structured tracing
/// spans for this handler.
///
/// `skip(ctx, message)` avoids logging large/internal values.
#[instrument(skip(ctx, message))]
pub async fn handle_ping(ctx: MessageContext, message: Message) {
    info!(
        message_id = message.header.message_id,
        payload_len = message.payload.len(),
        "handling ping message"
    );

    // Build simple fixed raw reply payload.
    //
    // The incoming payload is ignored in this example.
    //
    // Response:
    //
    // - archive type = Raw
    // - message id   = 1001
    // - payload      = "PONG"
    let response = build_raw_message(ctx.expected_magic, 1001, b"PONG", None, None);

    // Send async reply back to the originating peer.
    //
    // MessageContext internally manages reply routing through the correct
    // connection/session.
    if let Err(err) = ctx.send_reply(&response).await {
        error!(error = %err, "failed to send ping response");
    }
}
