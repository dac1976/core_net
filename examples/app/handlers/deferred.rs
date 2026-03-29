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

        let response = build_raw_message(ctx_clone.expected_magic, 1003, &reply_payload);

        if let Err(err) = ctx_clone.send_reply(&response).await {
            error!(error = %err, "failed to send deferred response");
        }
    });
}
