use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum Event<T> {
    Protocol(ProtocolEvent),
    Event(T),
}

impl<T> Event<T> {
    pub fn map<U, F>(self, f: F) -> Event<U>
    where
        F: FnOnce(T) -> U,
    {
        match self {
            Event::Protocol(e) => Event::Protocol(e),
            Event::Event(e) => Event::Event(f(e)),
        }
    }

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
    pub fn transpose(self) -> Result<Event<T>, E> {
        match self {
            Event::Protocol(e) => Ok(Event::Protocol(e)),
            Event::Event(Ok(e)) => Ok(Event::Event(e)),
            Event::Event(Err(e)) => Err(e),
        }
    }
}

#[derive(Debug, Copy, Clone, Serialize, Deserialize)]
pub enum ProtocolEvent {
    #[serde(rename = "heartbeat")]
    Heartbeat,
    #[serde(rename = "phx_close")]
    Close,
    #[serde(rename = "phx_error")]
    Error,
    #[serde(rename = "phx_join")]
    Join,
    #[serde(rename = "phx_reply")]
    Reply,
    #[serde(rename = "phx_leave")]
    Leave,
}
