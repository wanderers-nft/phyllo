use std::result;
use thiserror::Error;

pub type Result<T, E = Error> = result::Result<T, E>;

#[derive(Debug, Error)]
pub enum Error {
    #[error("could not (de)serialize")]
    Serde(#[from] serde_json::Error),
    #[error("websocket failure")]
    WebSocket(#[from] tokio_tungstenite::tungstenite::Error),
    #[error("message reply timeout")]
    Timeout,
}
