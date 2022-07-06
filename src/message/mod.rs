use crate::error::Error;
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use serde_json::Value;
use std::time::Duration;
use tokio::{select, sync::oneshot};
use tokio_tungstenite::tungstenite;
use tracing::warn;

/// A message in the Phoenix channels protocol.
///
/// Messages are serialized into an array, omitting the keys, instead of a regular JSON object.
/// ```
/// # use serde_json::json;
/// use serde_json::Value;
/// # use phyllo::message::{Message, Event, Payload};
/// 
/// type MessageType = Message<Value, Value, Value, Value>;
///
/// let message: MessageType = Message::new(
///     0,
///     0,
///     json!("topic"),
///     Event::Event(json!("event")),
///     Some(Payload::Custom(json!({"name": "Emerald"}))),
/// );
///
/// assert_eq!(
///     serde_json::to_string(&message)?,
///     r#"[0,0,"topic","event",{"name":"Emerald"}]"#
/// );
/// # Ok::<(), serde_json::Error>(())
/// ```
///
/// # Warning
/// `"phoenix"` is a protocol-reserved identifier for messages such as heartbeat.
/// It is considered a protocol error to send an irregular message with a topic that serializes into `"phoenix"`.
/// This is **not** checked by the library!
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Message<T, V, P, R> {
    /// Reference of the latest successful join message for the topic.
    pub join_ref: Option<u64>,
    /// Unique identifier for this message.
    pub reference: Option<u64>,
    /// Topic of the message.
    pub topic: T,
    /// Event of the message.
    pub event: Event<V>,
    /// Payload of the message.
    pub payload: Option<Payload<P, R>>,
}

/// The payload of a [`Message`].
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(untagged)]
pub enum Payload<P, R> {
    /// Reply/acknowledgement of a message sent from the client.
    /// This variant should not be sent to the server.
    PushReply {
        /// Status of the reply.
        status: PushStatus,
        /// Body of the reply.
        response: P,
    },
    /// A custom payload.
    Custom(R),
}

impl<P, R> Payload<P, R> {
    /// Maps a [`Payload<P, R>`] to [`Payload<Q, R>`].
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

    /// Maps a [`Payload<P, R>`] to [`Payload<P, S>`].
    pub fn map_custom<S, F>(self, f: F) -> Payload<P, S>
    where
        F: FnOnce(R) -> S,
    {
        match self {
            Payload::PushReply { status, response } => Payload::PushReply { status, response },
            Payload::Custom(c) => Payload::Custom(f(c)),
        }
    }

    /// Maps a [`Payload<P, R>`] to [`Payload<Q, R>`] using a fallible closure.
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

    /// Maps a [`Payload<P, R>`] to [`Payload<P, S>`] using a fallible closure.
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

/// Status of a message sent to the server, received as a response.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum PushStatus {
    /// Message was ok.
    Ok,
    /// Message encountered an error.
    Error,
}

impl Message<String, Value, Value, Value> {
    /// Creates a heartbeat message.
    /// ```
    /// # use serde_json::{json, Value};
    /// # use phyllo::message::{Message, Event, Payload};
    /// let message = Message::heartbeat(1);
    ///
    /// assert_eq!(
    ///     serde_json::to_string(&message)?,
    ///     r#"[0,1,"phoenix","heartbeat",null]"#
    /// );
    /// # Ok::<(), serde_json::Error>(())
    /// ```
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
    /// ```
    /// # use serde_json::{json, Value};
    /// # use phyllo::message::{Message, Event, Payload};
    /// let message = Message::leave(json!("topic"), 1);
    ///
    /// assert_eq!(
    ///     serde_json::to_string(&message)?,
    ///     r#"[0,1,"topic","phx_leave",null]"#
    /// );
    /// # Ok::<(), serde_json::Error>(())
    /// ```
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
    pub(crate) fn join(reference: u64, topic: T, payload: Option<R>) -> Self {
        Self::new(
            0,
            reference,
            topic,
            Event::Protocol(ProtocolEvent::Join),
            payload.map(|r| Payload::Custom(r)),
        )
    }

    /// Extracts a potential [`Custom`](crate::message::Payload::Custom) payload from a message.
    /// Useful for inbound messages where you don't care about the raw message from the protcol.
    ///
    /// # Example
    /// ```
    /// # use serde_json::{json, Value};
    /// # use phyllo::message::{Message, Event, Payload};
    /// type MessageType = Message<Value, Value, Value, Value>;
    /// const INCOMING: &str = r#"[0,0,"topic","event",{"friend":"Firefly"}]"#;
    ///
    /// // Assume this is an incoming message from the server.
    /// let message: MessageType = serde_json::from_str(INCOMING)?;
    ///
    /// assert_eq!(
    ///     Some(json!({"friend": "Firefly"})),
    ///     message.into_custom_payload()
    /// );
    /// # Ok::<(), serde_json::Error>(())
    /// ```
    pub fn into_custom_payload(self) -> Option<R> {
        match self.payload {
            Some(Payload::Custom(c)) => Some(c),
            _ => None,
        }
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

/// Wrapper around a type with a callback for the send result.
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
                Err(_) => Err(Error::SocketDropped),
                Ok(v) => v,
            }
        }
    }
}

/// The event that a [`Message`] represents.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(untagged)]
pub enum Event<T> {
    /// Protocol-reserved events.
    Protocol(ProtocolEvent),
    /// User-defined events.
    Event(T),
}

impl<T> Event<T> {
    /// Maps an `Event<T>` to `Event<U>`.
    pub fn map<U, F>(self, f: F) -> Event<U>
    where
        F: FnOnce(T) -> U,
    {
        match self {
            Event::Protocol(e) => Event::Protocol(e),
            Event::Event(e) => Event::Event(f(e)),
        }
    }

    /// Maps an `Event<T>` to `Event<U>` using a fallible closure.
    pub fn try_map<U, F, E>(self, f: F) -> Result<Event<U>, E>
    where
        F: FnOnce(T) -> Result<U, E>,
    {
        match self {
            Event::Protocol(e) => Ok(Event::Protocol(e)),
            Event::Event(e) => Ok(Event::Event(f(e)?)),
        }
    }
}

impl<T, E> Event<Result<T, E>> {
    /// Maps an `Event<Result<T, E>>` to `Result<Event<T>, E>>`.
    pub fn transpose(self) -> Result<Event<T>, E> {
        match self {
            Event::Protocol(e) => Ok(Event::Protocol(e)),
            Event::Event(Ok(e)) => Ok(Event::Event(e)),
            Event::Event(Err(e)) => Err(e),
        }
    }
}

/// Protocol-reserved events.
#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum ProtocolEvent {
    /// Heartbeat.
    #[serde(rename = "heartbeat")]
    Heartbeat,
    /// The connection will be closed.
    #[serde(rename = "phx_close")]
    Close,
    /// A channel has errored and needs to be reconnected.
    #[serde(rename = "phx_error")]
    Error,
    /// Joining a channel. (Non-receivable)
    #[serde(rename = "phx_join")]
    Join,
    /// Reply to a message sent by the client.
    #[serde(rename = "phx_reply")]
    Reply,
    /// Leaving a channel. (Non-receivable)
    #[serde(rename = "phx_leave")]
    Leave,
}
