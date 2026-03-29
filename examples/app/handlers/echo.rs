use core_net::messaging::{
    dispatcher::{MessageContext, MessageDispatcherBuilder},
    message::Message,
    message_builder::build_raw_message,
};
use tracing::{error, info, instrument};

pub fn register(builder: &mut MessageDispatcherBuilder) {
    builder.register(2, handle_echo);
}

#[instrument(skip(ctx, message))]
pub async fn handle_echo(ctx: MessageContext, message: Message) {
    info!(
        message_id = message.header.message_id,
        payload_len = message.payload.len(),
        "handling echo message"
    );

    let response = build_raw_message(ctx.expected_magic, 1002, message.payload.as_slice());

    if let Err(err) = ctx.send_reply(&response).await {
        error!(error = %err, "failed to send echo response");
    }
}
