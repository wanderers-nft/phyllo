use std::time::Duration;

use crate::{
    error::Error,
    message::{event::Event, run_message, Message, Payload, WithCallback},
};
use serde::{de::DeserializeOwned, Serialize};
use serde_json::Value;
use tokio::sync::{
    broadcast::{self, Receiver},
    mpsc::UnboundedSender,
    oneshot::{self, Sender},
};

pub mod channel_builder;

type HandlerChannelMessage<T> = (
    WithCallback<(Event<Value>, Payload<Value, Value>)>, // send callback
    Sender<Result<Message<T, Value, Value, Value>, Error>>, // reply callback
);

#[derive(Clone)]
pub struct ChannelHandler<T, V, P, R> {
    handler_tx: UnboundedSender<HandlerChannelMessage<T>>,
    timeout: Duration,
    broadcast_tx: broadcast::Sender<Message<T, V, P, R>>,
    close: broadcast::Sender<()>,
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
        T: Serialize + Send + 'static,
        V: Serialize + DeserializeOwned + Send + 'static,
        P: Serialize + DeserializeOwned + Send + 'static,
        R: Serialize + DeserializeOwned + Send + 'static,
    {
        let event = serde_json::to_value(&event)?;
        let payload = serde_json::to_value(&payload)?;

        let (event_payload, receiver) =
            WithCallback::new((Event::Event(event), Payload::Custom(payload)));
        let (tx, rx) = oneshot::channel();
        let _ = self.handler_tx.send((event_payload, tx));

        let x = tokio::spawn(run_message(receiver, rx, self.timeout))
            .await
            .unwrap()?;

        Ok(Message {
            join_ref: x.join_ref,
            reference: x.reference,
            topic: x.topic,
            event: x.event.try_map(serde_json::from_value)?,
            payload: x
                .payload
                .map(|p| {
                    Ok::<_, serde_json::Error>(
                        p.try_map_push_reply(serde_json::from_value)?
                            .try_map_custom(serde_json::from_value)?,
                    )
                })
                .transpose()?,
        })
    }

    pub fn subscribe(&mut self) -> Receiver<Message<T, V, P, R>> {
        self.broadcast_tx.subscribe()
    }

    pub fn close(self) {
        let _ = self.close.send(());
    }
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
#[repr(u8)]
pub enum ChannelStatus {
    Closed,
    Errored,
    Joined,
    Joining,
    Leaving,
    SocketClosed,
}

impl ChannelStatus {
    pub fn should_rejoin(self) -> bool {
        self == Self::Closed || self == Self::Errored
    }
}

pub(crate) enum SocketChannelMessage<T> {
    Message(Message<T, Value, Value, Value>),
    ChannelStatus(ChannelStatus),
}
