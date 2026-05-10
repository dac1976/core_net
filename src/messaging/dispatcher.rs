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

use super::message::Message;

use crate::protocol::MAGIC_STRING_LEN;
use crate::tcp_server::{ClientId, TcpServerHandle};

use std::{collections::HashMap, future::Future, net::SocketAddr, pin::Pin, sync::Arc};

use tracing::warn;

/// Boxed async future type used throughout the dispatcher layer.
///
/// This provides a transport/runtime-independent async handler abstraction.
///
/// Equivalent conceptual type:
///
/// ```text
/// async fn(...) -> T
/// ```
///
/// but boxed so heterogeneous handlers can be stored behind trait objects.
pub type BoxFuture<T> = Pin<Box<dyn Future<Output = T> + Send + 'static>>;

/// Abstract reply transport used by dispatcher handlers.
///
/// The dispatcher layer intentionally does not know whether a message arrived
/// via:
///
/// - TCP
/// - UDP unicast
/// - UDP broadcast
/// - UDP multicast
///
/// Handlers simply call:
///
/// ```rust
/// ctx.send_reply(...)
/// ```
///
/// and the correct transport-specific implementation is used internally.
#[derive(Clone)]
pub enum ReplyHandle {
    /// Reply via TCP server session.
    ///
    /// The dispatcher uses:
    ///
    /// - client id
    /// - TcpServerHandle
    ///
    /// to route replies back to the correct connected TCP client.
    #[allow(dead_code)]
    TcpServer {
        client_id: ClientId,
        handle: TcpServerHandle,
    },

    /// Reply via UDP transport.
    ///
    /// UDP is connectionless, so replies require:
    ///
    /// - destination peer socket address
    /// - async send function
    ///
    /// The send function abstraction allows:
    ///
    /// - unicast
    /// - broadcast
    /// - multicast
    ///
    /// implementations to share the same dispatcher API.
    #[allow(dead_code)]
    Udp {
        peer_addr: SocketAddr,

        send_fn: Arc<dyn Fn(SocketAddr, Vec<u8>) -> BoxFuture<Result<(), String>> + Send + Sync>,
    },
}

/// Context object passed into every message handler.
///
/// This contains:
///
/// - transport-independent reply routing
/// - source address information
/// - expected protocol magic
///
/// The context intentionally avoids exposing low-level transport internals to
/// application handlers.
#[derive(Clone)]
pub struct MessageContext {
    /// Source socket address if available.
    ///
    /// For example:
    ///
    /// - UDP peer
    /// - TCP remote endpoint
    pub source_addr: Option<SocketAddr>,

    /// Expected protocol magic string.
    ///
    /// Useful when building reply messages so handlers do not need to hardcode
    /// protocol constants.
    pub expected_magic: [u8; MAGIC_STRING_LEN],

    /// Abstract reply transport.
    pub reply_handle: ReplyHandle,
}

impl MessageContext {
    /// Sends a reply using the transport associated with this message context.
    ///
    /// Transport routing is handled automatically:
    ///
    /// - TCP -> routed to connected client session
    /// - UDP -> routed to peer socket address
    ///
    /// This keeps application handlers transport-independent.
    pub async fn send_reply(&self, bytes: &[u8]) -> Result<(), String> {
        match &self.reply_handle {
            // TCP reply path.
            ReplyHandle::TcpServer { client_id, handle } => handle
                .send_to_async(*client_id, bytes)
                .await
                .map_err(|e| e.to_string()),

            // UDP reply path.
            //
            // UDP send API currently takes ownership of a Vec<u8>, so the
            // slice is cloned into an owned buffer here.
            ReplyHandle::Udp { peer_addr, send_fn } => send_fn(*peer_addr, bytes.to_vec()).await,
        }
    }
}

/// Trait implemented by all dispatcher handlers.
///
/// Handlers:
///
/// - receive immutable message ownership
/// - receive a transport-independent MessageContext
/// - execute asynchronously
///
/// The trait is object-safe so handlers can be stored dynamically inside the
/// dispatcher routing table.
pub trait MessageHandler: Send + Sync + 'static {
    fn handle(&self, ctx: MessageContext, message: Message) -> BoxFuture<()>;
}

/// Blanket implementation allowing async closures/functions to automatically
/// become dispatcher handlers.
///
/// This enables ergonomic registration such as:
///
/// ```rust
/// builder.register(1, my_handler);
/// ```
///
/// without requiring explicit trait implementation boilerplate.
impl<F, Fut> MessageHandler for F
where
    F: Fn(MessageContext, Message) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = ()> + Send + 'static,
{
    fn handle(&self, ctx: MessageContext, message: Message) -> BoxFuture<()> {
        Box::pin((self)(ctx, message))
    }
}

/// Immutable runtime dispatcher.
///
/// Internally stores:
///
/// - message id -> handler mappings
/// - optional default handler
///
/// Cloning is cheap because internal state is reference counted via Arc.
#[derive(Clone, Default)]
pub struct MessageDispatcher {
    handlers: Arc<HashMap<i32, Arc<dyn MessageHandler>>>,

    default_handler: Option<Arc<dyn MessageHandler>>,
}

/// Mutable builder used to construct a MessageDispatcher.
///
/// Registration is performed during startup/configuration, after which the
/// immutable runtime dispatcher is built.
pub struct MessageDispatcherBuilder {
    handlers: HashMap<i32, Arc<dyn MessageHandler>>,

    default_handler: Option<Arc<dyn MessageHandler>>,
}

impl MessageDispatcherBuilder {
    /// Creates an empty dispatcher builder.
    pub fn new() -> Self {
        Self {
            handlers: HashMap::new(),

            default_handler: None,
        }
    }

    /// Registers a handler for a specific message id.
    ///
    /// If a handler already exists for the same message id it is replaced.
    pub fn register<H>(&mut self, message_id: i32, handler: H) -> &mut Self
    where
        H: MessageHandler,
    {
        self.handlers.insert(message_id, Arc::new(handler));

        self
    }

    /// Registers the default fallback handler.
    ///
    /// The default handler is invoked when no explicit message-id handler is
    /// registered.
    pub fn set_default<H>(&mut self, handler: H) -> &mut Self
    where
        H: MessageHandler,
    {
        self.default_handler = Some(Arc::new(handler));

        self
    }

    /// Builds the immutable runtime dispatcher.
    ///
    /// After build:
    ///
    /// - handler tables become shared/immutable
    /// - cloning becomes cheap
    /// - dispatcher becomes safe for concurrent use
    pub fn build(self) -> MessageDispatcher {
        MessageDispatcher {
            handlers: Arc::new(self.handlers),

            default_handler: self.default_handler,
        }
    }
}

impl MessageDispatcher {
    /// Dispatches a message to the registered async handler.
    ///
    /// Resolution order:
    ///
    /// 1. exact message-id handler
    /// 2. default handler
    /// 3. warning log if no handler exists
    pub async fn dispatch(&self, ctx: MessageContext, message: Message) {
        // Fast-path exact message-id lookup.
        if let Some(handler) = self.handlers.get(&message.header.message_id) {
            handler.handle(ctx, message).await;

            return;
        }

        // Fallback default handler.
        if let Some(handler) = &self.default_handler {
            handler.handle(ctx, message).await;

            return;
        }

        // No handler registered.
        warn!(
            message_id = message.header.message_id,
            source_addr = ?message.source_addr,
            "no handler registered for message id"
        );
    }
}
