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
    messaging::{
        dispatcher::{MessageContext, MessageDispatcherBuilder},
        message::Message,
        message_builder::build_msgpack_message,
        msgpack_codec::{from_msgpack_slice, to_msgpack_vec},
    },
    protocol::ArchiveType,
};

use serde::{Deserialize, Serialize};

use tracing::{error, info, instrument};

/// Example MessagePack request payload.
///
/// Demonstrates how structured data can be transported through
/// core_net using:
///
/// - MessagePack serialization
/// - typed Rust structs
/// - archive type validation
///
/// Example payload:
///
/// ```text
/// {
///   "name": "Duncan",
///   "count": 5
/// }
/// ```
#[derive(Debug, Serialize, Deserialize)]
#[allow(dead_code)]
struct PingRequest {
    /// Example string field.
    name: String,

    /// Example numeric field.
    count: u32,
}

/// Example MessagePack reply payload.
///
/// Demonstrates structured request/reply messaging using MessagePack.
#[derive(Debug, Serialize, Deserialize)]
#[allow(dead_code)]
struct PingReply {
    /// Indicates request success/failure.
    ok: bool,

    /// Human-readable response text.
    message: String,
}

/// Registers the MessagePack ping handler with the dispatcher.
///
/// Message ID mapping:
///
/// ```text
/// 10 -> handle_msgpack_ping()
/// ```
///
/// This handler demonstrates:
///
/// - typed MessagePack serialization/deserialization
/// - archive type validation
/// - structured request/reply messaging
/// - integration with the core_net dispatcher
#[allow(dead_code)]
pub fn register(builder: &mut MessageDispatcherBuilder) {
    builder.register(10, handle_msgpack_ping);
}

/// Handles incoming MessagePack ping requests.
///
/// Processing pipeline:
///
/// ```text
/// validate archive type
///   -> decode MessagePack request
///   -> build typed reply
///   -> encode MessagePack reply
///   -> send async response
/// ```
///
/// Reply message id:
///
/// ```text
/// 1010
/// ```
///
/// The tracing `instrument` macro automatically creates structured tracing
/// spans for this handler.
///
/// `skip(ctx, message)` avoids logging potentially large/internal values.
#[instrument(skip(ctx, message))]
#[allow(dead_code)]
pub async fn handle_msgpack_ping(ctx: MessageContext, message: Message) {
    // Ensure the incoming message uses the expected archive type.
    //
    // This protects the handler from attempting MessagePack decoding on
    // incompatible payload formats.
    if message.header.archive_type != ArchiveType::MessagePack {
        error!(
            actual = ?message.header.archive_type,
            "wrong archive type for MessagePack handler"
        );

        return;
    }

    // Decode MessagePack payload into strongly-typed Rust struct.
    //
    // The payload is borrowed directly from the received message buffer.
    let request: PingRequest = match from_msgpack_slice(message.payload.as_slice()) {
        Ok(v) => v,

        Err(err) => {
            error!(error = %err, "failed to decode MessagePack request");

            return;
        }
    };

    info!(
        name = %request.name,
        count = request.count,
        "decoded MessagePack request"
    );

    // Build typed reply object.
    //
    // This demonstrates a structured request/reply protocol rather than
    // simple raw byte handling.
    let reply = PingReply {
        ok: true,

        message: format!("Hello {}, count={}", request.name, request.count),
    };

    // Serialize reply struct into MessagePack payload bytes.
    let payload = match to_msgpack_vec(&reply) {
        Ok(v) => v,

        Err(err) => {
            error!(error = %err, "failed to encode MessagePack reply");

            return;
        }
    };

    // Wrap MessagePack payload inside a standard core_net message.
    //
    // Reply message id:
    //
    // 1010
    let full_message = build_msgpack_message(ctx.expected_magic, 1010, &payload, None, None);

    // Send async reply back to the originating peer.
    //
    // MessageContext internally handles routing the response through the
    // correct connection/session.
    if let Err(err) = ctx.send_reply(&full_message).await {
        error!(error = %err, "failed to send MessagePack reply");
    }
}
