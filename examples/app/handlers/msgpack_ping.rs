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
