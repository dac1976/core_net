use super::message::Message;
use crate::protocol::MAGIC_STRING_LEN;
use crate::tcp_server::{ClientId, TcpServerHandle};
use std::{collections::HashMap, future::Future, net::SocketAddr, pin::Pin, sync::Arc};
use tracing::warn;

pub type BoxFuture<T> = Pin<Box<dyn Future<Output = T> + Send + 'static>>;

#[derive(Clone)]
pub enum ReplyHandle {
    #[allow(dead_code)]
    TcpServer {
        client_id: ClientId,
        handle: TcpServerHandle,
    },
    #[allow(dead_code)]
    Udp {
        peer_addr: SocketAddr,
        send_fn: Arc<dyn Fn(SocketAddr, Vec<u8>) -> BoxFuture<Result<(), String>> + Send + Sync>,
    },
}

#[derive(Clone)]
pub struct MessageContext {
    pub source_addr: Option<SocketAddr>,
    pub expected_magic: [u8; MAGIC_STRING_LEN],
    pub reply_handle: ReplyHandle,
}

impl MessageContext {
    pub async fn send_reply(&self, bytes: &[u8]) -> Result<(), String> {
        match &self.reply_handle {
            ReplyHandle::TcpServer { client_id, handle } => handle
                .send_to_async(*client_id, bytes)
                .await
                .map_err(|e| e.to_string()),

            ReplyHandle::Udp { peer_addr, send_fn } => send_fn(*peer_addr, bytes.to_vec()).await,
        }
    }
}

pub trait MessageHandler: Send + Sync + 'static {
    fn handle(&self, ctx: MessageContext, message: Message) -> BoxFuture<()>;
}

impl<F, Fut> MessageHandler for F
where
    F: Fn(MessageContext, Message) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = ()> + Send + 'static,
{
    fn handle(&self, ctx: MessageContext, message: Message) -> BoxFuture<()> {
        Box::pin((self)(ctx, message))
    }
}

#[derive(Clone, Default)]
pub struct MessageDispatcher {
    handlers: Arc<HashMap<i32, Arc<dyn MessageHandler>>>,
    default_handler: Option<Arc<dyn MessageHandler>>,
}

pub struct MessageDispatcherBuilder {
    handlers: HashMap<i32, Arc<dyn MessageHandler>>,
    default_handler: Option<Arc<dyn MessageHandler>>,
}

impl MessageDispatcherBuilder {
    pub fn new() -> Self {
        Self {
            handlers: HashMap::new(),
            default_handler: None,
        }
    }

    pub fn register<H>(&mut self, message_id: i32, handler: H) -> &mut Self
    where
        H: MessageHandler,
    {
        self.handlers.insert(message_id, Arc::new(handler));
        self
    }

    pub fn set_default<H>(&mut self, handler: H) -> &mut Self
    where
        H: MessageHandler,
    {
        self.default_handler = Some(Arc::new(handler));
        self
    }

    pub fn build(self) -> MessageDispatcher {
        MessageDispatcher {
            handlers: Arc::new(self.handlers),
            default_handler: self.default_handler,
        }
    }
}

impl MessageDispatcher {
    pub async fn dispatch(&self, ctx: MessageContext, message: Message) {
        if let Some(handler) = self.handlers.get(&message.header.message_id) {
            handler.handle(ctx, message).await;
            return;
        }

        if let Some(handler) = &self.default_handler {
            handler.handle(ctx, message).await;
            return;
        }

        warn!(
            message_id = message.header.message_id,
            source_addr = ?message.source_addr,
            "no handler registered for message id"
        );
    }
}
