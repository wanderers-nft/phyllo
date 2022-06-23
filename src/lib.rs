#![deny(missing_docs)]

//! Phoenix channels in Rust.

/// Channels for sending/receiving messages related to a topic.
pub mod channel;
/// Error handling.
pub mod error;
/// Definition of a message in the Phoenix protocol.
pub mod message;
/// Socket for sending/receiving messages.
pub mod socket;
