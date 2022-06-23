use serde::{Deserialize, Serialize};

/// The event that a message represents.
#[derive(Debug, Clone, Serialize, Deserialize)]
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
#[derive(Debug, Copy, Clone, Serialize, Deserialize)]
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
