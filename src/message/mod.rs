use self::event::{Event, ProtocolEvent};
use crate::error::Error;
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use serde_json::Value;
use std::time::Duration;
use tokio::{select, sync::oneshot};
use tokio_tungstenite::tungstenite;
use tracing::warn;

/// Events that messages can represent.
pub mod event;

/// A Phoenix channel message. This struct is serialized into an array (omitting the keys).
#[derive(Debug, Clone)]
pub struct Message<T, V, P, R> {
    /// Reference of the join message for the topic.
    pub join_ref: Option<u64>,
    /// Unique identifier for this message.
    pub reference: Option<u64>,
    /// Topic of this message.
    pub topic: T,
    /// Event this message represents.
    pub event: Event<V>,
    /// Payload of this message.
    pub payload: Option<Payload<P, R>>,
}

/// The payload a `Message` can contain.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum Payload<P, R> {
    /// Reply/acknowledgement of a message sent from the client. (Non-sendable)
    PushReply {
        /// The status of the reply.
        status: PushStatus,
        /// The body of the reply.
        response: P,
    },
    /// A custom outbound payload. (Non-receivable)
    Custom(R),
}

impl<P, R> Payload<P, R> {
    /// Maps a `Payload<P, R>` to `Payload<Q, R>`.
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

    /// Maps a `Payload<P, R>` to `Payload<P, S>`.
    pub fn map_custom<S, F>(self, f: F) -> Payload<P, S>
    where
        F: FnOnce(R) -> S,
    {
        match self {
            Payload::PushReply { status, response } => Payload::PushReply { status, response },
            Payload::Custom(c) => Payload::Custom(f(c)),
        }
    }

    /// Maps a `Payload<P, R>` to `Payload<Q, R>` using a fallible closure.
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

    /// Maps a `Payload<P, R>` to `Payload<P, S>` using a fallible closure.
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

/// Status of a message sent to the server.
#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum PushStatus {
    /// Message was ok.
    Ok,
    /// Message encountered an error.
    Error,
}

impl Message<String, (), (), ()> {
    /// Creates a heartbeat message.
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
    /// Creates a leave message for a topic.
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
    /// Creates a new `Message`.
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

    /// Creates a join message for a topic.
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

/// Wrapper around a type with a callback for the result of send.
#[derive(Debug)]
pub(crate) struct WithCallback<T> {
    pub(crate) content: T,
    pub(crate) callback: oneshot::Sender<Result<(), tungstenite::Error>>,
}

impl<T> WithCallback<T> {
    /// Creates a new `WithCallback`.
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

    /// Maps a `WithCallback<T>` to `WithCallback<U>`.
    pub(crate) fn map<U, F>(self, f: F) -> WithCallback<U>
    where
        F: FnOnce(T) -> U,
    {
        WithCallback {
            content: f(self.content),
            callback: self.callback,
        }
    }

    /// Maps a `WithCallback<T>` to `WithCallback<U>` using a fallible closure.
    pub(crate) fn try_map<U, F, E>(self, f: F) -> Result<WithCallback<U>, E>
    where
        F: FnOnce(T) -> Result<U, E>,
    {
        Ok(WithCallback {
            content: f(self.content)?,
            callback: self.callback,
        })
    }
}

/// Handles the sending of a message and receiving its reply, with a timeout.
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
            warn!("timeout on send callback");
            Err(Error::Timeout)
        },

        Err(_) = &mut receive_callback => Err(Error::SocketDropped),

        v = send_callback => {
            match v {
                Err(_) => Err(Error::SocketDropped),
                Ok(Err(e)) => Err(Error::WebSocket(e)),
                _ => Ok(())
            }
        }
    }?;

    // Await the receive callback
    select! {
        _ = &mut timeout => {
            Err(Error::Timeout)
        }

        v = receive_callback => {
            match v {
                // todo: how tf do you handle this
                Err(_) => Err(Error::SocketDropped),
                Ok(v) => v,
            }
        }
    }
}
