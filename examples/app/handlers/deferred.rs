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

pub fn register(builder: &mut MessageDispatcherBuilder) {
    builder.register(3, handle_deferred_work);
}

#[instrument(skip(ctx, message))]
pub async fn handle_deferred_work(ctx: MessageContext, message: Message) {
    info!(
        message_id = message.header.message_id,
        payload_len = message.payload.len(),
        "handling deferred-work message"
    );

    let payload = message.payload.as_slice().to_vec();
    let ctx_clone = ctx.clone();

    tokio::spawn(async move {
        let mut reply_payload = b"DEFERRED_ACK: ".to_vec();
        reply_payload.extend_from_slice(&payload);

        let response =
            build_raw_message(ctx_clone.expected_magic, 1003, &reply_payload, None, None);

        if let Err(err) = ctx_clone.send_reply(&response).await {
            error!(error = %err, "failed to send deferred response");
        }
    });
}
