use serde::{de::DeserializeOwned, Deserialize, Serialize};
use tokio::sync::oneshot;
use tokio_tungstenite::tungstenite;

use self::event::{ChannelEvent, Event};

pub mod event;

#[derive(Debug, Clone)]
pub struct Message<T, V, P, R> {
    pub(crate) join_ref: u64,
    pub(crate) reference: u64,
    pub(crate) topic: T,
    pub(crate) event: Event<V>,
    pub(crate) payload: Payload<P, R>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum Payload<P, R> {
    PushReply { status: PushStatus, response: R },
    Custom(P),
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum PushStatus {
    Ok,
    Error,
}

impl<V, R> Message<String, V, (), R> {
    pub fn heartbeat(reference: u64) -> Self {
        Self {
            join_ref: 0,
            reference,
            topic: "phoenix".to_string(),
            event: Event::Hearbeat,
            payload: Payload::Custom(()),
        }
    }
}

impl<T, V, P, R> Message<T, V, P, R>
where
    T: Serialize,
    V: Serialize,
    P: Serialize,
    R: Serialize,
{
    pub fn new(
        join_ref: u64,
        reference: u64,
        topic: T,
        event: Event<V>,
        payload: Payload<P, R>,
    ) -> Self {
        Self {
            join_ref,
            reference,
            topic,
            event,
            payload,
        }
    }

    pub fn join(join_ref: u64, reference: u64, topic: T, payload: Payload<P, R>) -> Self {
        Self::new(
            join_ref,
            reference,
            topic,
            Event::ChannelEvent(ChannelEvent::Join),
            payload,
        )
    }
}

impl<T, V, P, R> TryFrom<Message<T, V, P, R>> for tungstenite::Message
where
    T: Serialize,
    V: Serialize,
    P: Serialize,
    R: Serialize,
{
    type Error = serde_json::Error;

    fn try_from(value: Message<T, V, P, R>) -> Result<Self, Self::Error> {
        Ok(Self::Text(serde_json::to_string(&value)?))
    }
}

impl<T, V, P, R> TryFrom<tungstenite::Message> for Message<T, V, P, R>
where
    T: DeserializeOwned,
    V: DeserializeOwned,
    P: DeserializeOwned,
    R: DeserializeOwned,
{
    type Error = serde_json::Error;

    fn try_from(value: tungstenite::Message) -> Result<Self, Self::Error> {
        match value {
            tungstenite::Message::Text(t) => serde_json::from_str(&t),
            _ => unreachable!(),
        }
    }
}

impl<T, V, P, R> Serialize for Message<T, V, P, R>
where
    T: Serialize,
    V: Serialize,
    P: Serialize,
    R: Serialize,
{
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        (
            self.join_ref,
            self.reference,
            &self.topic,
            &self.event,
            &self.payload,
        )
            .serialize(serializer)
    }
}

impl<'de, T, V, P, R> Deserialize<'de> for Message<T, V, P, R>
where
    T: DeserializeOwned,
    V: DeserializeOwned,
    P: DeserializeOwned,
    R: DeserializeOwned,
{
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        Deserialize::deserialize(deserializer).map(
            |(join_ref, reference, topic, event, payload)| Message {
                join_ref,
                reference,
                topic,
                event,
                payload,
            },
        )
    }
}

#[derive(Debug)]
pub struct MessageResult<T, V, P, R> {
    pub message: Message<T, V, P, R>,
    pub callback: oneshot::Sender<Result<(), tungstenite::Error>>,
}

impl<T, V, P, R> MessageResult<T, V, P, R> {
    pub fn new(
        message: Message<T, V, P, R>,
    ) -> (Self, oneshot::Receiver<Result<(), tungstenite::Error>>) {
        let (tx, rx) = oneshot::channel();
        (
            Self {
                message,
                callback: tx,
            },
            rx,
        )
    }
}

impl<T, V, P, R> TryFrom<MessageResult<T, V, P, R>> for TungsteniteMessageResult
where
    T: Serialize,
    V: Serialize,
    P: Serialize,
    R: Serialize,
{
    type Error = serde_json::Error;

    fn try_from(value: MessageResult<T, V, P, R>) -> Result<Self, Self::Error> {
        Ok(Self {
            message: value.message.try_into()?,
            callback: value.callback,
        })
    }
}

#[derive(Debug)]
pub struct TungsteniteMessageResult {
    pub message: tungstenite::Message,
    pub callback: oneshot::Sender<Result<(), tungstenite::Error>>,
}

impl TungsteniteMessageResult {
    pub fn new(
        message: tungstenite::Message,
    ) -> (Self, oneshot::Receiver<Result<(), tungstenite::Error>>) {
        let (tx, rx) = oneshot::channel();
        (
            Self {
                message,
                callback: tx,
            },
            rx,
        )
    }
}

impl<T, V, P, R> TryFrom<TungsteniteMessageResult> for MessageResult<T, V, P, R>
where
    T: DeserializeOwned,
    V: DeserializeOwned,
    P: DeserializeOwned,
    R: DeserializeOwned,
{
    type Error = serde_json::Error;

    fn try_from(value: TungsteniteMessageResult) -> Result<Self, Self::Error> {
        Ok(Self {
            message: value.message.try_into()?,
            callback: value.callback,
        })
    }
}

pub struct EventPayloadResult<V, P, R> {
    pub event: Event<V>,
    pub payload: Payload<P, R>,
    pub callback: oneshot::Sender<Result<(), tungstenite::Error>>,
}

impl<V, P, R> EventPayloadResult<V, P, R> {
    pub fn new(
        event: Event<V>,
        payload: Payload<P, R>,
    ) -> (Self, oneshot::Receiver<Result<(), tungstenite::Error>>) {
        let (tx, rx) = oneshot::channel();
        (
            Self {
                event,
                payload,
                callback: tx,
            },
            rx,
        )
    }
}
