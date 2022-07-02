use std::result;
use thiserror::Error;

/// Convenience result type for [`enum@Error`].
pub type Result<T, E = Error> = result::Result<T, E>;

/// Errors that can be encountered while handling messages.
#[derive(Debug, Error)]
pub enum Error {
    /// Could not (de)serialize the message.
    #[error("could not (de)serialize")]
    Serde(#[from] serde_json::Error),
    /// Underlying error from `tungstenite` websocket.
    #[error("websocket failure")]
    WebSocket(#[from] tokio_tungstenite::tungstenite::Error),
    /// Message reply timed out.
    #[error("message reply timeout")]
    Timeout,
    /// The channel responsible for this message has been dropped.
    #[error("underlying channel dropped")]
    ChannelDropped,
    /// The socket responsible for this message has been dropped.
    #[error("underlying socket dropped")]
    SocketDropped,
}

/// Errors that can be encountered during [channel registration](crate::socket::SocketHandler::channel).
#[derive(Debug, Error, PartialEq, Eq, Hash)]
pub enum RegisterChannelError {
    /// The socket responsible has been dropped.
    #[error("underlying socket dropped")]
    SocketDropped,
    /// The topic has already been registered.
    #[error("topic has already been registered")]
    DuplicateTopic,
}

/// Errors that can be encountered during [channel subscription](crate::channel::ChannelHandler::subscribe)
#[derive(Debug, Error, PartialEq, Eq, Hash)]
pub enum ChannelSubscribeError {
    /// The channel responsible for this subscription has been dropped.
    #[error("underlying channel dropped")]
    ChannelDropped,
}

/// Errors that can be encountered while joining a channel.
/// This error variant is used to catch successful replies with an errored payload, which is **not** considered an error in regular usage.
#[derive(Debug, Error)]
pub(crate) enum ChannelJoinError<P> {
    /// See [`enum@Error`].
    #[error(transparent)]
    Error(#[from] Error),
    /// Message was replied to successfully, but payload contained an error.
    #[error("replied with error")]
    Join(P),
}
