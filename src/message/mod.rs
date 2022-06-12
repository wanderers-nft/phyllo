use std::time::Duration;

use serde::{de::DeserializeOwned, Deserialize, Serialize};
use serde_json::Value;
use tokio::{select, sync::oneshot};
use tokio_tungstenite::tungstenite;

use crate::error::Error;

use self::event::{Event, ProtocolEvent};

pub mod event;

#[derive(Debug, Clone)]
pub struct Message<T, V, P, R> {
    pub(crate) join_ref: Option<u64>,
    pub(crate) reference: Option<u64>,
    pub(crate) topic: T,
    pub(crate) event: Event<V>,
    pub(crate) payload: Option<Payload<P, R>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum Payload<P, R> {
    PushReply { status: PushStatus, response: P },
    Custom(R),
}

impl<P, R> Payload<P, R> {
    pub fn map_push_reply<Q, F>(self, f: F) -> Payload<Q, R>
    where
        F: FnOnce(P) -> Q,
    {
        match self {
            Payload::PushReply { status, response } => Payload::PushReply {
                status,
                response: f(response),
            },
            Payload::Custom(c) => Payload::Custom(c),
        }
    }

    pub fn map_custom<S, F>(self, f: F) -> Payload<P, S>
    where
        F: FnOnce(R) -> S,
    {
        match self {
            Payload::PushReply { status, response } => Payload::PushReply { status, response },
            Payload::Custom(c) => Payload::Custom(f(c)),
        }
    }

    pub fn try_map_push_reply<Q, F, E>(self, f: F) -> Result<Payload<Q, R>, E>
    where
        F: FnOnce(P) -> Result<Q, E>,
    {
        match self {
            Payload::PushReply { status, response } => Ok(Payload::PushReply {
                status,
                response: f(response)?,
            }),
            Payload::Custom(c) => Ok(Payload::Custom(c)),
        }
    }

    pub fn try_map_custom<S, F, E>(self, f: F) -> Result<Payload<P, S>, E>
    where
        F: FnOnce(R) -> Result<S, E>,
    {
        match self {
            Payload::PushReply { status, response } => Ok(Payload::PushReply { status, response }),
            Payload::Custom(c) => Ok(Payload::Custom(f(c)?)),
        }
    }
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum PushStatus {
    Ok,
    Error,
}

impl Message<String, (), (), ()> {
    pub fn heartbeat(reference: u64) -> Self {
        Self {
            join_ref: Some(0),
            reference: Some(reference),
            topic: "phoenix".to_string(),
            event: Event::Protocol(ProtocolEvent::Heartbeat),
            payload: None,
        }
    }
}

impl<T> Message<T, Value, Value, Value> {
    pub fn leave(topic: T, reference: u64) -> Self {
        Self {
            join_ref: Some(0),
            reference: Some(reference),
            topic,
            event: Event::Protocol(ProtocolEvent::Leave),
            payload: None,
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
        payload: Option<Payload<P, R>>,
    ) -> Self {
        Self {
            join_ref: Some(join_ref),
            reference: Some(reference),
            topic,
            event,
            payload,
        }
    }

    pub fn join(reference: u64, topic: T, payload: Option<R>) -> Self {
        Self::new(
            0,
            reference,
            topic,
            Event::Protocol(ProtocolEvent::Join),
            payload.map(|r| Payload::Custom(r)),
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

pub(crate) struct WithCallback<T> {
    pub(crate) content: T,
    pub(crate) callback: oneshot::Sender<Result<(), tungstenite::Error>>,
}

impl<T> WithCallback<T> {
    pub(crate) fn new(content: T) -> (Self, oneshot::Receiver<Result<(), tungstenite::Error>>) {
        let (tx, rx) = oneshot::channel();
        (
            Self {
                content,
                callback: tx,
            },
            rx,
        )
    }

    pub(crate) fn map<U>(self) -> WithCallback<U>
    where
        T: Into<U>,
    {
        WithCallback {
            content: self.content.into(),
            callback: self.callback,
        }
    }

    pub(crate) fn try_map<U>(self) -> Result<WithCallback<U>, T::Error>
    where
        T: TryInto<U>,
    {
        self.content.try_into().map(|v| WithCallback {
            content: v,
            callback: self.callback,
        })
    }
}

pub(crate) async fn run_message<T, V, P, R>(
    send_callback: oneshot::Receiver<Result<(), tungstenite::Error>>,
    mut receive_callback: oneshot::Receiver<Result<Message<T, V, P, R>, Error>>,
    timeout: Duration,
) -> Result<Message<T, V, P, R>, Error>
where
    T: Serialize,
    V: Serialize,
    P: Serialize,
    R: Serialize,
{
    let timeout = tokio::time::sleep(timeout);
    tokio::pin!(timeout);

    // Await the send callback
    select! {
        _ = &mut timeout => {
            return Err(Error::Timeout);
        },

        Err(e) = &mut receive_callback => {
            panic!("todo: handle error on transmission");
        },

        v = send_callback => {
            match v {
                // todo: how tf do you handle this
                Err(e) => todo!(),
                Ok(Err(e)) => {
                    return Err(Error::WebSocket(e));
                },
                _ => {}
            };
        }
    };

    // Await the receive callback
    select! {
        _ = &mut timeout => {
            return Err(Error::Timeout);
        }

        v = receive_callback => {
            match v {
                // todo: how tf do you handle this
                Err(e) => todo!(),
                Ok(v) => v,
            }
        }
    }
}
