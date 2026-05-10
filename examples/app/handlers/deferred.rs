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

/// Registers the deferred-work message handler with the dispatcher.
///
/// Message ID mapping:
///
/// ```text
/// 3 -> handle_deferred_work()
/// ```
///
/// This demonstrates how core_net dispatchers can route incoming messages
/// to async handler functions based on the message id.
///
/// The deferred-work example intentionally demonstrates a pattern where:
///
/// ```text
/// receive message
///   -> immediately return from dispatcher
///   -> continue processing in background task
///   -> send async reply later
/// ```
///
/// This is useful for:
///
/// - long-running work
/// - database queries
/// - filesystem operations
/// - computational tasks
/// - external service calls
///
/// without blocking the main connection receive/dispatch path.
pub fn register(builder: &mut MessageDispatcherBuilder) {
    builder.register(3, handle_deferred_work);
}

/// Example async deferred-work message handler.
///
/// This handler demonstrates:
///
/// - async dispatcher handlers
/// - extracting payload data from a received message
/// - spawning detached background work
/// - sending asynchronous replies later
///
/// The tracing `instrument` macro automatically adds structured tracing
/// spans for this handler.
///
/// `skip(ctx, message)` avoids logging potentially large/internal values.
#[instrument(skip(ctx, message))]
pub async fn handle_deferred_work(ctx: MessageContext, message: Message) {
    info!(
        message_id = message.header.message_id,
        payload_len = message.payload.len(),
        "handling deferred-work message"
    );

    // Clone payload data into an owned Vec.
    //
    // This is important because the spawned task must own everything it
    // uses. The original Message may be dropped once this handler returns.
    //
    // In this demo the payload is intentionally copied to demonstrate the
    // ownership/lifetime model clearly.
    let payload = message.payload.as_slice().to_vec();

    // Clone the message context so the spawned task can:
    //
    // - access connection metadata
    // - send replies asynchronously later
    //
    // independently of the original handler stack frame.
    let ctx_clone = ctx.clone();

    // Spawn deferred/background work.
    //
    // The dispatcher can now continue processing other incoming messages
    // immediately without waiting for this work to complete.
    tokio::spawn(async move {
        // Build simple reply payload.
        //
        // Result:
        //
        // ```text
        // DEFERRED_ACK: <original payload>
        // ```
        let mut reply_payload = b"DEFERRED_ACK: ".to_vec();

        reply_payload.extend_from_slice(&payload);

        // Build standard core_net raw message reply.
        //
        // Reply message id:
        //
        // 1003
        let response =
            build_raw_message(ctx_clone.expected_magic, 1003, &reply_payload, None, None);

        // Send reply asynchronously back to the originating peer.
        //
        // MessageContext internally knows how to route the reply back to the
        // originating connection/session.
        if let Err(err) = ctx_clone.send_reply(&response).await {
            error!(error = %err, "failed to send deferred response");
        }
    });
}
