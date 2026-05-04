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

#[derive(Debug, Serialize, Deserialize)]
#[allow(dead_code)]
struct PingRequest {
    name: String,
    count: u32,
}

#[derive(Debug, Serialize, Deserialize)]
#[allow(dead_code)]
struct PingReply {
    ok: bool,
    message: String,
}

#[allow(dead_code)]
pub fn register(builder: &mut MessageDispatcherBuilder) {
    builder.register(10, handle_msgpack_ping);
}

#[instrument(skip(ctx, message))]
#[allow(dead_code)]
pub async fn handle_msgpack_ping(ctx: MessageContext, message: Message) {
    if message.header.archive_type != ArchiveType::MessagePack {
        error!(
            actual = ?message.header.archive_type,
            "wrong archive type for MessagePack handler"
        );
        return;
    }

    let request: PingRequest = match from_msgpack_slice(message.payload.as_slice()) {
        Ok(v) => v,
        Err(err) => {
            error!(error = %err, "failed to decode MessagePack request");
            return;
        }
    };

    info!(name = %request.name, count = request.count, "decoded MessagePack request");

    let reply = PingReply {
        ok: true,
        message: format!("Hello {}, count={}", request.name, request.count),
    };

    let payload = match to_msgpack_vec(&reply) {
        Ok(v) => v,
        Err(err) => {
            error!(error = %err, "failed to encode MessagePack reply");
            return;
        }
    };

    let full_message = build_msgpack_message(ctx.expected_magic, 1010, &payload);

    if let Err(err) = ctx.send_reply(&full_message).await {
        error!(error = %err, "failed to send MessagePack reply");
    }
}
