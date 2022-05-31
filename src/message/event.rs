use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum Event<T> {
    Protocol(ProtocolEvent),
    Event(T),
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
