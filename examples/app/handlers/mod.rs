pub mod deferred;
pub mod echo;
pub mod msgpack_ping;
pub mod ping;

use core_net::messaging::dispatcher::MessageDispatcherBuilder;

pub fn register_handlers(builder: &mut MessageDispatcherBuilder) {
    ping::register(builder);
    echo::register(builder);
    deferred::register(builder);
    msgpack_ping::register(builder);

    builder.set_default(
        |ctx: core_net::messaging::dispatcher::MessageContext,
         message: core_net::messaging::message::Message| async move {
            tracing::warn!(
                message_id = message.header.message_id,
                source_addr = ?ctx.source_addr,
                "default handler invoked for unregistered message id"
            );
        },
    );
}
