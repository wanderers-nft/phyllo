use self::socket_builder::SocketBuilder;
use crate::channel::channel_builder::ChannelBuilder;
use crate::channel::{ChannelHandler, SocketChannelMessage};
use crate::message::TungsteniteMessageResult;
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::hash::Hash;
use std::sync::atomic::AtomicBool;
use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
};
use tokio::sync::broadcast;
use tokio::sync::{
    mpsc::{unbounded_channel, UnboundedSender},
    Mutex,
};
use url::Url;

pub mod socket_builder;

#[derive(Clone)]
pub struct SocketHandler<T> {
    reference: Reference,
    out_tx: UnboundedSender<TungsteniteMessageResult>,
    subscriptions: Arc<Mutex<HashMap<T, UnboundedSender<SocketChannelMessage>>>>,
    close: broadcast::Sender<()>,
    is_closed: Arc<AtomicBool>,
}

impl<T> SocketHandler<T>
where
    T: Serialize + DeserializeOwned + Eq + Hash + Send + 'static,
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

    // async fn run_join_channel_task(
    //     self,
    //     channel_builder: ChannelBuilder<T>,
    //     in_rx: UnboundedReceiver<SocketChannelMessage>,
    // ) -> Result<UnboundedReceiver<SocketChannelMessage>, Error> {
    //     let (t, r) = oneshot::channel::<Result<(), tungstenite::Error>>();
    //     let join_message = TungsteniteMessageResult {
    //         message: Message::<(), serde_json::Value, T>::join(
    //             channel_builder.join_ref,
    //             self.next_ref(),
    //             channel_builder.topic,
    //             channel_builder.params,
    //         )
    //         .try_into()?,
    //         callback: t,
    //     };

    //     // TODO: Use backoff
    //     let _ = self.out_tx.send(join_message);
    //     r.await.unwrap()?;

    //     // Remember to give the Covenant back their receiver
    //     Ok(in_rx)
    // }

    pub async fn channel<V, P, R>(
        &mut self,
        channel_builder: ChannelBuilder<T>,
    ) -> ChannelHandler<T, V, P, R>
    where
        T: Serialize + DeserializeOwned + Send + Clone + Eq + Hash,
        V: Serialize + DeserializeOwned + Send + Clone + 'static,
        P: Serialize + DeserializeOwned + Send + Clone + 'static,
        R: Serialize + DeserializeOwned + Send + Clone + 'static,
    {
        let mut subscriptions = self.subscriptions.lock().await;
        let (in_tx, in_rx) = unbounded_channel::<SocketChannelMessage>();

        // Cannot subscribe to a channel more than once
        if subscriptions.contains_key(&channel_builder.topic) {
            // TODO: make this an error
            panic!("channel already exists");
        }
        subscriptions.insert(channel_builder.topic.clone(), in_tx);
        std::mem::drop(subscriptions);

        channel_builder.build::<V, P, R>(self.reference.clone(), self.out_tx.clone(), in_rx)
    }

    pub fn close(self) {
        self.is_closed.store(true, Ordering::Relaxed);
        let _ = self.close.send(());
    }

    pub fn is_closed(&self) -> bool {
        self.is_closed.load(Ordering::Relaxed)
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
