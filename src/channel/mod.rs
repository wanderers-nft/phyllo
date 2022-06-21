use std::time::Duration;

use crate::{
    error::Error,
    message::{event::Event, run_message, Message, Payload, WithCallback},
};
use serde::{de::DeserializeOwned, Serialize};
use serde_json::Value;
use tokio::sync::{broadcast, mpsc::UnboundedSender, oneshot};
use tokio_tungstenite::tungstenite;

pub mod channel_builder;

struct HandlerChannelMessage<T> {
    message: WithCallback<(Event<Value>, Payload<Value, Value>)>,
    reply_callback: oneshot::Sender<Result<Message<T, Value, Value, Value>, Error>>,
}

enum HandlerChannelInternalMessage<T, V, P, R> {
    Leave {
        callback: WithCallback<()>,
        reply_callback: oneshot::Sender<Result<Message<T, Value, Value, Value>, Error>>,
    },
    Broadcast {
        callback: oneshot::Sender<broadcast::Receiver<Message<T, V, P, R>>>,
    },
}

#[derive(Clone)]
pub struct ChannelHandler<T, V, P, R> {
    handler_tx: UnboundedSender<HandlerChannelMessage<T>>,
    timeout: Duration,
    handler_internal_tx: UnboundedSender<HandlerChannelInternalMessage<T, V, P, R>>,
}

impl<T, V, P, R> ChannelHandler<T, V, P, R>
where
    T: Serialize,
    V: Serialize,
    P: Serialize,
    R: Serialize,
{
    pub async fn send(
        &mut self,
        event: Event<V>,
        payload: Payload<P, R>,
    ) -> Result<Message<T, V, P, R>, Error>
    where
        T: Serialize,
        V: Serialize + DeserializeOwned,
        P: Serialize + DeserializeOwned,
        R: Serialize + DeserializeOwned,
    {
        let event = serde_json::to_value(&event)?;
        let payload = serde_json::to_value(&payload)?;

        let (event_payload, receiver) =
            WithCallback::new((Event::Event(event), Payload::Custom(payload)));
        let (tx, rx) = oneshot::channel();
        let _ = self.handler_tx.send(HandlerChannelMessage {
            message: event_payload,
            reply_callback: tx,
        });

        let res = run_message(receiver, rx, self.timeout).await?;

        Ok(Message {
            join_ref: res.join_ref,
            reference: res.reference,
            topic: res.topic,
            event: res.event.try_map(serde_json::from_value)?,
            payload: res
                .payload
                .map(|p| {
                    p.try_map_push_reply(serde_json::from_value)?
                        .try_map_custom(serde_json::from_value)
                })
                .transpose()?,
        })
    }

    pub async fn subscribe(&mut self) -> broadcast::Receiver<Message<T, V, P, R>> {
        let (tx, rx) = oneshot::channel();

        // todo: handle this error
        let _ = self
            .handler_internal_tx
            .send(HandlerChannelInternalMessage::Broadcast { callback: tx });

        // todo: handle this error
        rx.await.unwrap()
    }

    pub async fn close(self) -> Result<Message<T, V, P, R>, Error>
    where
        T: Serialize,
        V: Serialize + DeserializeOwned,
        P: Serialize + DeserializeOwned,
        R: Serialize + DeserializeOwned,
    {
        let (callback, receiver) = WithCallback::new(());
        let (tx, rx) = oneshot::channel();

        let _ = self
            .handler_internal_tx
            .send(HandlerChannelInternalMessage::Leave {
                callback,
                reply_callback: tx,
            });

        let res = run_message(receiver, rx, self.timeout).await?;

        Ok(Message {
            join_ref: res.join_ref,
            reference: res.reference,
            topic: res.topic,
            event: res.event.try_map(serde_json::from_value)?,
            payload: res
                .payload
                .map(|p| {
                    p.try_map_push_reply(serde_json::from_value)?
                        .try_map_custom(serde_json::from_value)
                })
                .transpose()?,
        })
    }
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
#[repr(u8)]
pub enum ChannelStatus {
    // Channel is closed, and should be rejoined.
    Rejoin,
    // Channel has been closed by the server.
    Closed,
    // Channel has recieved an error from the server.
    Errored,
    // Channel has been joined.
    Joined,
    // The underlying socket has closed.
    SocketClosed,
}

impl ChannelStatus {
    pub fn should_rejoin(self) -> bool {
        self == Self::Rejoin || self == Self::Errored
    }
}

#[derive(Debug)]
pub(crate) enum SocketChannelMessage<T> {
    Message(Message<T, Value, Value, Value>),
    ChannelStatus(ChannelStatus),
}

#[derive(Debug)]
pub(crate) enum ChannelSocketMessage<T> {
    Message(WithCallback<tungstenite::Message>),
    TaskEnded(T),
}
