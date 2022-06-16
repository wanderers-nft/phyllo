use self::socket_builder::SocketBuilder;
use crate::channel::channel_builder::ChannelBuilder;
use crate::channel::{ChannelHandler, ChannelSocketMessage, SocketChannelMessage};
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::fmt::Debug;
use std::hash::Hash;
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc,
};
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};
use tokio::sync::oneshot;
use url::Url;

pub mod socket_builder;

#[derive(Clone)]
pub struct SocketHandler<T> {
    reference: Reference,
    handler_tx: UnboundedSender<HandlerSocketMessage<T>>,
}

impl<T> SocketHandler<T>
where
    T: Serialize + DeserializeOwned + Eq + Hash + Send + 'static + Debug,
{
    pub async fn new(endpoint: Url) -> Self {
        Self::builder(endpoint).build().await
    }

    pub fn builder(endpoint: Url) -> SocketBuilder {
        SocketBuilder::new(endpoint)
    }

    pub fn next_ref(&self) -> u64 {
        self.reference.next()
    }

    pub async fn channel<V, P, R>(
        &mut self,
        channel_builder: ChannelBuilder<T>,
    ) -> ChannelHandler<T, V, P, R>
    where
        T: Serialize + DeserializeOwned + Send + Sync + Clone + Eq + Hash,
        V: Serialize + DeserializeOwned + Send + Clone + 'static,
        P: Serialize + DeserializeOwned + Send + Clone + 'static,
        R: Serialize + DeserializeOwned + Send + Clone + 'static,
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

    pub fn close(self) {
        let _ = self.handler_tx.send(HandlerSocketMessage::Close);
    }
}

#[derive(Clone, Debug)]
pub struct Reference(Arc<AtomicU64>);

impl Reference {
    pub(crate) fn new() -> Self {
        Self(Arc::new(AtomicU64::new(0)))
    }

    pub fn next(&self) -> u64 {
        self.0.fetch_add(1, Ordering::Relaxed)
    }

    pub(crate) fn reset(&self) {
        self.0.store(0, Ordering::Relaxed);
    }
}

impl Default for Reference {
    fn default() -> Self {
        Self::new()
    }
}

enum HandlerSocketMessage<T> {
    Close,
    Subscribe {
        topic: T,
        callback: oneshot::Sender<(
            UnboundedReceiver<SocketChannelMessage<T>>,
            UnboundedSender<ChannelSocketMessage<T>>,
        )>,
    },
}
