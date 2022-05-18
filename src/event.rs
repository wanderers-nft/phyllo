use serde::{Deserialize, Serialize};
use std::fmt::Display;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum Event<T> {
    Hearbeat,
    ChannelEvent(ChannelEvent),
    Event(T),
}

impl<T> Display for Event<T>
where
    T: ToString,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = match &self {
            Event::Hearbeat => "heartbeat".to_string(),
            Event::ChannelEvent(ce) => ce.to_string(),
            Event::Event(e) => e.to_string(),
        };
        write!(f, "{}", s)
    }
}

#[derive(Debug, Copy, Clone, Serialize, Deserialize)]
pub enum ChannelEvent {
    Close,
    Error,
    Join,
    Reply,
    Leave,
}

impl Display for ChannelEvent {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = match &self {
            ChannelEvent::Close => "phx_close",
            ChannelEvent::Error => "phx_error",
            ChannelEvent::Join => "phx_join",
            ChannelEvent::Reply => "phx_reply",
            ChannelEvent::Leave => "phx_leave",
        };
        write!(f, "{}", s)
    }
}
