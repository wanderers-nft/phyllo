use crate::channel::channel_builder::ChannelBuilder;
use crate::channel::{ChannelHandler, ChannelSocketMessage, SocketChannelMessage};
use crate::message::Message;
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::fmt::Debug;
use std::hash::Hash;
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc,
};
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};
use tokio::sync::{broadcast, oneshot};

/// Builder for a `Socket`.
pub mod socket_builder;

/// Handler half of a `Socket`.
#[derive(Debug, Clone)]
pub struct SocketHandler<T> {
    reference: Reference,
    handler_tx: UnboundedSender<HandlerSocketMessage<T>>,
}

impl<T> SocketHandler<T>
where
    T: Serialize + DeserializeOwned + Eq + Hash + Send + 'static + Debug + Sync,
{
    /// Register a new channel for the socket, returning a corresponding `ChannelHandler`.
    /// # Panics
    /// This function will panic if the underlying `Socket` has been dropped.
    /// This function will panic if the given topic has already been registered.
    pub async fn channel<V, P, R>(
        &mut self,
        channel_builder: ChannelBuilder<T>,
    ) -> (
        ChannelHandler<T, V, P, R>,
        broadcast::Receiver<Message<T, V, P, R>>,
    )
    where
        T: Serialize + DeserializeOwned + Send + Sync + Clone + Eq + Hash,
        V: Serialize + DeserializeOwned + Send + Clone + 'static + Debug,
        P: Serialize + DeserializeOwned + Send + Clone + 'static + Debug,
        R: Serialize + DeserializeOwned + Send + Clone + 'static + Debug,
    {
        let (tx, rx) = oneshot::channel();

        let _ = self.handler_tx.send(HandlerSocketMessage::Subscribe {
            topic: channel_builder.topic.clone(),
            callback: tx,
        });

        // todo: handle this error
        let (channel_socket, socket_channel) = rx.await.unwrap();

        channel_builder.build::<V, P, R>(self.reference.clone(), socket_channel, channel_socket)
    }

    /// Close the socket, dropping all queued messages. This function will work even if the underlying socket has already been closed by another `SocketHandler`.
    pub fn close(self) {
        let _ = self.handler_tx.send(HandlerSocketMessage::Close);
    }
}

/// A monotonically-increasing counter for tracking messages.
#[derive(Clone, Debug)]
pub struct Reference(Arc<AtomicU64>);

impl Reference {
    /// Constructs a new `Reference`.
    pub(crate) fn new() -> Self {
        Self(Arc::new(AtomicU64::new(0)))
    }

    /// Fetch the next value.
    pub fn next(&self) -> u64 {
        self.0.fetch_add(1, Ordering::Relaxed)
    }

    /// Reset the counter.
    pub(crate) fn reset(&self) {
        self.0.store(0, Ordering::Relaxed);
    }
}

impl Default for Reference {
    fn default() -> Self {
        Self::new()
    }
}

type HandlerSocketMessageCallback<T> = oneshot::Sender<(
    UnboundedReceiver<SocketChannelMessage<T>>,
    UnboundedSender<ChannelSocketMessage<T>>,
)>;

/// M
enum HandlerSocketMessage<T> {
    Close,
    Subscribe {
        topic: T,
        callback: HandlerSocketMessageCallback<T>,
    },
}
