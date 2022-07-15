use crate::{
    error::{ChannelJoinError, ChannelSubscribeError, Error},
    message::{
        run_message, run_message_with_timeout, Event, Message, Payload, ProtocolEvent, PushStatus,
        WithCallback,
    },
    socket::Reference,
};
use backoff::ExponentialBackoff;
use futures_util::future::OptionFuture;
use serde::{de::DeserializeOwned, Serialize};
use serde_json::Value;
use std::{
    collections::{hash_map::Entry, HashMap},
    fmt::Debug,
    time::Duration,
};
use tokio::{
    select,
    sync::{
        broadcast,
        mpsc::{error::SendError, unbounded_channel, UnboundedReceiver, UnboundedSender},
        oneshot,
    },
};
use tokio_tungstenite::tungstenite;
use tracing::{info, instrument, warn};

/// Message sent from a `ChannelHandler` to its `Channel`. These messages will not be consumed by the `Channel` until it has joined and is ready to send messages to the `Socket`.
#[derive(Debug)]
struct HandlerChannelMessage<T> {
    message: WithCallback<(Event<Value>, Payload<Value, Value>)>,
    reply_callback: oneshot::Sender<Result<Message<T, Value, Value, Value>, Error>>,
}

/// A priority message sent from a `ChannelHandler` to its `Channel`. These messages will always be processed regardless of whether the `Channel` has been joined.
#[derive(Debug)]
enum HandlerChannelInternalMessage<T, V, P, R> {
    /// Send a leave message, destroying the `Channel`.
    Leave {
        callback: WithCallback<()>,
        reply_callback: oneshot::Sender<Result<Message<T, Value, Value, Value>, Error>>,
    },
    /// Create a new subscription to the message broadcast.
    Broadcast {
        callback: oneshot::Sender<broadcast::Receiver<Message<T, V, P, R>>>,
    },
}

/// Handler half of a `Channel`.
///
/// # Errors
/// For functions that return `Result<Message, Error>`, an error is returned if the function fails to send, receive, encode or decode messages.
/// It is **not** considered a failure for the server to successfully reply with an error.
#[derive(Debug, Clone)]
pub struct ChannelHandler<T, V, P, R> {
    handler_tx: UnboundedSender<HandlerChannelMessage<T>>,
    timeout: Duration,
    handler_internal_tx: UnboundedSender<HandlerChannelInternalMessage<T, V, P, R>>,
}

impl<T, V, P, R> ChannelHandler<T, V, P, R>
where
    T: Serialize,
    V: Serialize + DeserializeOwned,
    P: Serialize + DeserializeOwned,
    R: Serialize + DeserializeOwned,
{
    /// Sends a message to the server. The response from the server is returned.
    pub async fn send(
        &mut self,
        event: Event<V>,
        payload: Payload<P, R>,
    ) -> Result<Message<T, V, P, R>, Error> {
        self.send_inner(event, payload, Some(self.timeout)).await
    }

    /// Sends a message to the server. The response from the server is returned.
    /// This function will not time out waiting for a response.
    pub async fn send_no_timeout(
        &mut self,
        event: Event<V>,
        payload: Payload<P, R>,
    ) -> Result<Message<T, V, P, R>, Error> {
        self.send_inner(event, payload, None).await
    }

    async fn send_inner(
        &mut self,
        event: Event<V>,
        payload: Payload<P, R>,
        timeout: Option<Duration>,
    ) -> Result<Message<T, V, P, R>, Error> {
        let event = serde_json::to_value(&event)?;
        let payload = serde_json::to_value(&payload)?;

        let (event_payload, receiver) =
            WithCallback::new((Event::Event(event), Payload::Custom(payload)));
        let (tx, rx) = oneshot::channel();
        self.handler_tx
            .send(HandlerChannelMessage {
                message: event_payload,
                reply_callback: tx,
            })
            .map_err(|_| Error::ChannelDropped)?;

        let res = match timeout {
            Some(t) => run_message_with_timeout(receiver, rx, t).await,
            None => run_message(receiver, rx).await,
        }?;

        Ok(Message {
            join_ref: res.join_ref,
            reference: res.reference,
            topic: res.topic,
            event: res.event.try_map(serde_json::from_value)?,
            payload: res
                .payload
                .map(|p| {
                    p.try_map_push_reply(serde_json::from_value)?
                        .try_map_custom(serde_json::from_value)
                })
                .transpose()?,
        })
    }

    /// Gets a receiver subscribed to non-reply channel messages.
    pub async fn subscribe(
        &mut self,
    ) -> Result<broadcast::Receiver<Message<T, V, P, R>>, ChannelSubscribeError> {
        let (tx, rx) = oneshot::channel();

        self.handler_internal_tx
            .send(HandlerChannelInternalMessage::Broadcast { callback: tx })
            .map_err(|_| ChannelSubscribeError::ChannelDropped)?;

        rx.await.map_err(|_| ChannelSubscribeError::ChannelDropped)
    }

    /// Closes the channel, dropping the corresponding `Channel`. The response from the server is returned.
    pub async fn close(self) -> Result<Message<T, V, P, R>, Error> {
        let (callback, receiver) = WithCallback::new(());
        let (tx, rx) = oneshot::channel();

        self.handler_internal_tx
            .send(HandlerChannelInternalMessage::Leave {
                callback,
                reply_callback: tx,
            })
            .map_err(|_| Error::ChannelDropped)?;

        let res = run_message_with_timeout(receiver, rx, self.timeout).await?;

        Ok(Message {
            join_ref: res.join_ref,
            reference: res.reference,
            topic: res.topic,
            event: res.event.try_map(serde_json::from_value)?,
            payload: res
                .payload
                .map(|p| {
                    p.try_map_push_reply(serde_json::from_value)?
                        .try_map_custom(serde_json::from_value)
                })
                .transpose()?,
        })
    }
}

/// The status of a `Channel`.
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub(crate) enum ChannelStatus {
    /// Underlying websocket was reset, a rejoin will be attempted.
    Rejoin,
    /// Closed by the server.
    Closed,
    /// Recieved an error from the server, a rejoin will be attempted.
    Errored,
    /// Joined and ready to receive messages.
    Joined,
    /// The underlying socket has closed.
    SocketClosed,
}

impl ChannelStatus {
    /// Whether a rejoin should be attempted.
    pub(crate) fn should_rejoin(self) -> bool {
        self == Self::Rejoin || self == Self::Errored
    }
}

/// Message sent from a `Socket` to a `Channel`.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum SocketChannelMessage<T> {
    /// A message with the registered topic.
    Message(Message<T, Value, Value, Value>),
    /// An update to the channel's status.
    ChannelStatus(ChannelStatus),
}

/// Message sent from a `Channel` to a `Socket`.
#[derive(Debug)]
pub(crate) enum ChannelSocketMessage<T> {
    /// A message to be sent to the websocket.
    Message(WithCallback<tungstenite::Message>),
    /// The channel has been dropped.
    TaskEnded(T),
}

/// Builder for a `Channel`.
#[derive(Debug, Clone)]
pub struct ChannelBuilder<T> {
    pub(crate) topic: T,
    timeout: Duration,
    rejoin_timeout: Duration,
    rejoin: ExponentialBackoff,
    params: Option<serde_json::Value>,
    broadcast_buffer: usize,
}

impl<T> ChannelBuilder<T>
where
    T: Serialize,
{
    /// Constructs a new [`ChannelBuilder`]. `topic` is the topic used for messages sent/received through this channel.
    pub fn new(topic: T) -> Self {
        Self {
            topic,
            timeout: Duration::from_millis(20000),
            rejoin_timeout: Duration::from_millis(10000),
            rejoin: ExponentialBackoff::default(),
            params: None,
            broadcast_buffer: 128,
        }
    }

    /// Sets the topic used for messages sent/recieved through this channel.
    pub fn topic(mut self, topic: T) -> Self {
        self.topic = topic;
        self
    }

    /// Sets the timeout duration for messages sent/received through this channel.
    pub fn timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    #[deprecated(
        note = "Rejoin messages now wait indefinitely instead of timing out. This value does nothing"
    )]
    /// Sets the timeout duration for rejoin messages sent by this channel.
    pub fn rejoin_timeout(mut self, rejoin_timeout: Duration) -> Self {
        self.rejoin_timeout = rejoin_timeout;
        self
    }

    /// Sets the strategy for attempting rejoining using exponential backoff.
    pub fn rejoin(mut self, rejoin_after: ExponentialBackoff) -> Self {
        self.rejoin = rejoin_after;
        self
    }

    /// Sets the param to be sent during joining.
    /// # Panics
    /// Panics if `params` fails to serialize.
    pub fn params<U>(mut self, params: Option<U>) -> Self
    where
        U: Serialize,
    {
        self = self
            .try_params(params)
            .expect("could not serialize parameter");
        self
    }

    /// Sets the param to be sent during joining.
    pub fn try_params<U>(mut self, params: Option<U>) -> Result<Self, serde_json::Error>
    where
        U: Serialize,
    {
        self.params = params.map(|v| serde_json::to_value(&v)).transpose()?;
        Ok(self)
    }

    /// Sets the buffer size of the broadcast channel. See [`tokio::sync::broadcast`].
    pub fn broadcast_buffer(mut self, broadcast_buffer: usize) -> Self {
        self.broadcast_buffer = broadcast_buffer;
        self
    }

    /// Spawns the `Channel` and returns a corresponding [`ChannelHandler`].
    // Allow type complexity here (for some reason it doesn't complain about SocketHandler::channel!)
    #[allow(clippy::type_complexity)]
    pub(crate) fn build<V, P, R>(
        self,
        reference: Reference,
        out_tx: UnboundedSender<ChannelSocketMessage<T>>,
        in_rx: UnboundedReceiver<SocketChannelMessage<T>>,
    ) -> (
        ChannelHandler<T, V, P, R>,
        broadcast::Receiver<Message<T, V, P, R>>,
    )
    where
        T: Serialize + DeserializeOwned + Send + Sync + Clone + 'static + Debug,
        V: Serialize + DeserializeOwned + Send + Clone + 'static + Debug,
        P: Serialize + DeserializeOwned + Send + Clone + 'static + Debug,
        R: Serialize + DeserializeOwned + Send + Clone + 'static + Debug,
    {
        let (handler_tx, handler_rx) = unbounded_channel();
        let (broadcast_tx, _) = broadcast::channel(self.broadcast_buffer);
        let immediate_rx = broadcast_tx.subscribe();
        let (rejoin_tx, rejoin_rx) = unbounded_channel();

        let (handler_internal_tx, handler_internal_rx) = unbounded_channel();

        let channel: Channel<T, V, P, R> = Channel {
            status: ChannelStatus::Rejoin,
            topic: self.topic.clone(),
            rejoin_after: self.rejoin.clone(),
            params: self.params.clone(),
            replies: HashMap::new(),
            reference: reference.clone(),
            join_ref: reference.next(),
            rejoin_tx,
            rejoin_rx,
            handler_rx,
            handler_internal_rx,
            out_tx,
            in_rx,
            broadcast: broadcast_tx,
            rejoin_inflight: false,
        };

        tokio::spawn(channel.run());

        (
            ChannelHandler {
                handler_tx,
                timeout: self.timeout,
                handler_internal_tx,
            },
            immediate_rx,
        )
    }
}

/// Mapping of message references to their callback.
type RepliesMapping<T> =
    HashMap<u64, oneshot::Sender<Result<Message<T, Value, Value, Value>, Error>>>;

/// A channel for receiving/sending Phoenix messages of a particular topic.
#[derive(Debug)]
struct Channel<T, V, P, R> {
    status: ChannelStatus,

    topic: T,
    rejoin_after: ExponentialBackoff,
    params: Option<serde_json::Value>,

    replies: RepliesMapping<T>,
    reference: Reference,

    /// Reference of last successful rejoin message
    join_ref: u64,

    /// Tx for Rejoin -> Channel
    rejoin_tx: UnboundedSender<RejoinChannelMessage<T, Value, Value, Value>>,
    /// Rx for Rejoin -> Channel
    rejoin_rx: UnboundedReceiver<RejoinChannelMessage<T, Value, Value, Value>>,

    /// Handler -> Channel
    handler_rx: UnboundedReceiver<HandlerChannelMessage<T>>,
    /// Handler -> Channel (for internal messages)
    handler_internal_rx: UnboundedReceiver<HandlerChannelInternalMessage<T, V, P, R>>,

    /// Channel -> Socket
    out_tx: UnboundedSender<ChannelSocketMessage<T>>,
    /// Socket -> CHannel
    in_rx: UnboundedReceiver<SocketChannelMessage<T>>,

    /// Broadcaster for non-reply messages
    broadcast: broadcast::Sender<Message<T, V, P, R>>,

    /// Whether a rejoiner task has already been dispatched
    rejoin_inflight: bool,
}

impl<T, V, P, R> Channel<T, V, P, R>
where
    T: Serialize + DeserializeOwned + Debug + Clone,
    V: Serialize + DeserializeOwned + Debug,
    P: Serialize + DeserializeOwned + Debug,
    R: Serialize + DeserializeOwned + Debug,
{
    /// Sends the reply of a message to its waiting callback.
    fn send_reply(
        &mut self,
        reference: u64,
        message: Result<Message<T, Value, Value, Value>, Error>,
    ) {
        if let Some(reply) = self.replies.remove(&reference) {
            if let Err(e) = reply.send(message) {
                warn!(error = ?e, "reply send failed");
            }
        }
    }

    /// Handles an inbound message from the `Socket`.
    #[instrument(skip_all, fields(topic = ?self.topic))]
    async fn inbound(&mut self, message: SocketChannelMessage<T>) -> Result<(), serde_json::Error> {
        match message {
            SocketChannelMessage::Message(msg) => {
                info!(message = ?msg, "incoming");

                match (&msg.event, &msg.payload) {
                    // On close, do not attempt re-connection
                    (Event::Protocol(ProtocolEvent::Close), _) => {
                        self.status = ChannelStatus::Closed;
                    }

                    // On error, mark channel has errored so the next iteration can attempt re-connect
                    (Event::Protocol(ProtocolEvent::Error), _) => {
                        self.status = ChannelStatus::Errored;
                    }

                    // On message reply, trigger callback
                    (Event::Protocol(ProtocolEvent::Reply), _) => {
                        if let Some(message_ref) = msg.reference {
                            self.send_reply(message_ref, Ok(msg));
                        }
                    }

                    // On new message
                    (Event::Event(_), Some(Payload::Custom(_))) => {
                        let msg = Message {
                            join_ref: msg.join_ref,
                            reference: msg.reference,
                            topic: msg.topic,
                            event: msg.event.try_map(serde_json::from_value)?,
                            payload: msg
                                .payload
                                .map(|p| {
                                    p.try_map_push_reply(serde_json::from_value)?
                                        .try_map_custom(serde_json::from_value)
                                })
                                .transpose()?,
                        };
                        let res = self.broadcast.send(msg);
                        if let Err(e) = res {
                            warn!(error = ?e, "broadcast failed");
                        }
                    }

                    // Heartbeat, Join, Leave messages cannot be received.
                    _ => {}
                };
            }
            SocketChannelMessage::ChannelStatus(cs) => {
                info!(status = ?cs, "updating status");
                self.status = cs;
            }
        };
        Ok(())
    }

    /// Handles a message from a `ChannelHandler`.
    async fn outbound(
        &mut self,
        HandlerChannelMessage {
            message,
            reply_callback,
        }: HandlerChannelMessage<T>,
    ) -> Result<(), SendError<ChannelSocketMessage<T>>>
    where
        T: Clone,
    {
        let message = message.map(|(e, p)| {
            Message::new(
                self.join_ref,
                self.reference.next(),
                self.topic.clone(),
                e,
                Some(p),
            )
        });

        self.outbound_inner(message, reply_callback)
    }

    /// Handles a leave message from a `ChannelHandler`.
    fn outbound_leave(
        &mut self,
        message: WithCallback<()>,
        reply_callback: oneshot::Sender<Result<Message<T, Value, Value, Value>, Error>>,
    ) -> Result<(), SendError<ChannelSocketMessage<T>>> {
        let message = message.map(|_| Message::leave(self.topic.clone(), self.reference.next()));
        self.outbound_inner(message, reply_callback)
    }

    /// Sends a message to the `Socket`.
    #[instrument(name = "outbound", skip(self), fields(topic = ?self.topic, message = ?message.content))]
    fn outbound_inner(
        &mut self,
        message: WithCallback<Message<T, Value, Value, Value>>,
        reply_callback: oneshot::Sender<Result<Message<T, Value, Value, Value>, Error>>,
    ) -> Result<(), SendError<ChannelSocketMessage<T>>> {
        let reference = message.content.reference.unwrap();
        match self.replies.entry(reference) {
            Entry::Occupied(mut e) => {
                warn!(kv = ?e, "reference already used");
                e.insert(reply_callback);
            }
            Entry::Vacant(e) => {
                e.insert(reply_callback);
            }
        }

        let message = match message.try_map(TryInto::try_into) {
            Ok(v) => v,
            Err(e) => {
                warn!(value = ?e, "message could not be serialized");
                self.send_reply(reference, Err(Error::Serde(e)));
                return Ok(());
            }
        };

        self.out_tx
            .send(ChannelSocketMessage::Message(message))
            .map_err(|e| {
                warn!(error = ?e, "failed to send to socket");
                e
            })
    }

    /// Runs the `Channel` task.
    pub(crate) async fn run(mut self)
    where
        T: Send + Sync + 'static + Debug,
        V: Send + 'static,
        P: Send + 'static,
        R: Send + 'static,
    {
        'retry: loop {
            let mut rejoin: OptionFuture<_> = match self.status {
                ChannelStatus::Errored | ChannelStatus::Rejoin if !self.rejoin_inflight => {
                    self.rejoin_inflight = true;
                    let rejoiner = Rejoin {
                        rejoin_after: self.rejoin_after.clone(),
                        reference: self.reference.clone(),
                        topic: self.topic.clone(),
                        params: self.params.clone(),
                        rejoin_tx: self.rejoin_tx.clone(),
                    };
                    Some(tokio::spawn(rejoiner.join_with_backoff())).into()
                }
                _ => {
                    self.rejoin_inflight = false;
                    None.into()
                }
            };

            'inner: loop {
                select! {
                    Some(v) = self.handler_internal_rx.recv() => {
                        match v {
                            HandlerChannelInternalMessage::Leave { callback, reply_callback } => {
                                let _ = self.outbound_leave(callback, reply_callback);
                            },
                            HandlerChannelInternalMessage::Broadcast { callback } => {
                                let _ = callback.send(self.broadcast.subscribe());
                            },
                        }
                    }

                    // In from channel handler. Only attempt to pull values from the queue if we know we can send them out
                    Some(value) = self.handler_rx.recv(), if !self.status.should_rejoin() => {
                        let _ = self.outbound(value).await;
                    },

                    // Inbound from socket
                    Some(value) = self.in_rx.recv() => {
                        let _ = self.inbound(value).await;
                    },

                    // Inbound from rejoiner
                    Some(RejoinChannelMessage { message, reply_callback }) = self.rejoin_rx.recv() => {
                        let _ = self.outbound_inner(message, reply_callback);
                    }

                    // When rejoiner task is complete.
                    // Rejoin cannot ever be polled twice because of `rejoin_inflight`.
                    Some(v) = &mut rejoin, if self.rejoin_inflight => {
                        self.rejoin_inflight = false;
                        match v {
                            Ok(Ok(new_join_ref)) => {
                                self.status = ChannelStatus::Joined;
                                self.join_ref = new_join_ref;
                            },
                            // If socket is dropped, this task should be destroyed
                            Ok(Err(ChannelJoinError::Error(Error::SocketDropped))) => {
                                self.status = ChannelStatus::SocketClosed;
                            }
                            _ => {
                                break 'inner;
                            }
                        }
                    }

                    else => {}
                }

                match self.status {
                    // Closed: this channel task needs to be destroyed
                    ChannelStatus::Closed | ChannelStatus::SocketClosed => {
                        info!(?self.topic, "destroying channel");
                        break 'retry;
                    }
                    // We can still reconnect
                    ChannelStatus::Errored | ChannelStatus::Rejoin if !self.rejoin_inflight => {
                        info!(?self.topic, "will attempt rejoin");
                        break 'inner;
                    }
                    _ => {}
                }
            }
        }
        let _ = self
            .out_tx
            .send(ChannelSocketMessage::TaskEnded(self.topic));
    }
}

/// Message sent from `Rejoin` to `Channel`.
#[derive(Debug)]
struct RejoinChannelMessage<T, V, P, R> {
    message: WithCallback<Message<T, V, P, R>>,
    reply_callback: oneshot::Sender<Result<Message<T, V, P, R>, Error>>,
}

/// Rejoiner task responsible for sending rejoins for a topic.
#[derive(Debug, Clone)]
struct Rejoin<T, V, P> {
    rejoin_after: ExponentialBackoff,
    reference: Reference,
    topic: T,
    params: Option<serde_json::Value>,
    rejoin_tx: UnboundedSender<RejoinChannelMessage<T, V, P, serde_json::Value>>,
}

impl<T, V, P> Rejoin<T, V, P>
where
    T: Serialize + Clone + Send + Debug,
    V: Serialize,
    P: Serialize + Debug,
{
    /// Sends a rejoin message.
    async fn join(&self) -> Result<u64, backoff::Error<ChannelJoinError<P>>> {
        let join_ref = self.reference.next();
        let message = Message::<T, V, P, serde_json::Value>::join(
            join_ref,
            self.topic.clone(),
            self.params.clone(),
        );

        let (message, rx) = WithCallback::new(message);
        let (res_tx, res_rx) = oneshot::channel();

        self.rejoin_tx
            .send(RejoinChannelMessage {
                message,
                reply_callback: res_tx,
            })
            .map_err(|_| {
                warn!("socket dropped");
                backoff::Error::Permanent(ChannelJoinError::Error(Error::SocketDropped))
            })?;

        let res = run_message::<T, V, P, serde_json::Value>(rx, res_rx)
            .await
            .map_err(|e| match e {
                // Never re-attempt a dropped socket
                Error::SocketDropped => {
                    warn!("socket dropped");
                    backoff::Error::Permanent(ChannelJoinError::Error(Error::SocketDropped))
                }
                _ => backoff::Error::transient(ChannelJoinError::Error(e)),
            })?;

        match res.payload {
            Some(Payload::PushReply {
                status: PushStatus::Error,
                response: p,
            }) => Err(ChannelJoinError::Join(p))?,
            _ => Ok(join_ref),
        }
    }

    /// Attempts to rejoin a channel with exponential backoff on failure. If the underlying `Socket` has been dropped, this function immediately returns.
    #[instrument(skip(self), fields(topic = ?self.topic))]
    async fn join_with_backoff(self) -> Result<u64, ChannelJoinError<P>> {
        backoff::future::retry(self.rejoin_after.clone(), || async {
            info!("attempting rejoin");
            self.join().await.map_err(|e| {
                warn!(error = ?e);
                e
            })
        })
        .await
    }
}
