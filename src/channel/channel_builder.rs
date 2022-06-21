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
use tracing::{info, instrument, warn};

use crate::{
    error::{ChannelJoinError, Error},
    message::{
        event::{Event, ProtocolEvent},
        run_message, Message, Payload, PushStatus, WithCallback,
    },
    socket::Reference,
};

use super::{
    ChannelHandler, ChannelSocketMessage, ChannelStatus, HandlerChannelInternalMessage,
    HandlerChannelMessage, SocketChannelMessage,
};

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

    pub fn topic(&mut self, topic: T) {
        self.topic = topic;
    }

    pub fn timeout(&mut self, timeout: Duration) {
        self.timeout = timeout;
    }

    pub fn rejoin_timeout(&mut self, rejoin_timeout: Duration) {
        self.rejoin_timeout = rejoin_timeout;
    }

    pub fn rejoin(&mut self, rejoin_after: ExponentialBackoff) {
        self.rejoin = rejoin_after;
    }

    pub fn params<U>(&mut self, params: Option<U>)
    where
        U: Serialize,
    {
        self.try_params(params)
            .expect("could not serialize parameter");
    }

    pub fn try_params<U>(&mut self, params: Option<U>) -> Result<(), serde_json::Error>
    where
        U: Serialize,
    {
        self.params = params.map(|v| serde_json::to_value(&v)).transpose()?;
        Ok(())
    }

    pub fn broadcast_buffer(&mut self, broadcast_buffer: usize) {
        self.broadcast_buffer = broadcast_buffer;
    }

    pub(crate) fn build<V, P, R>(
        &self,
        reference: Reference,
        out_tx: UnboundedSender<ChannelSocketMessage<T>>,
        in_rx: UnboundedReceiver<SocketChannelMessage<T>>,
    ) -> ChannelHandler<T, V, P, R>
    where
        T: Serialize + DeserializeOwned + Send + Sync + Clone + 'static + Debug,
        V: Serialize + DeserializeOwned + Send + Clone + 'static + Debug,
        P: Serialize + DeserializeOwned + Send + Clone + 'static + Debug,
        R: Serialize + DeserializeOwned + Send + Clone + 'static + Debug,
    {
        let (handler_tx, handler_rx) = unbounded_channel();
        let (broadcast_tx, _) = broadcast::channel(self.broadcast_buffer);
        let (rejoin_tx, rejoin_rx) = unbounded_channel();

        let (handler_internal_tx, handler_internal_rx) = unbounded_channel();

        let channel: Channel<T, V, P, R> = Channel {
            status: ChannelStatus::Rejoin,
            topic: self.topic.clone(),
            rejoin_timeout: self.rejoin_timeout,
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
            broadcast: broadcast_tx.clone(),
        };

        tokio::spawn(channel.run());

        ChannelHandler {
            handler_tx,
            timeout: self.timeout,
            handler_internal_tx,
        }
    }
}

type RepliesMapping<T> =
    HashMap<u64, oneshot::Sender<Result<Message<T, Value, Value, Value>, Error>>>;

#[derive(Debug)]
struct Channel<T, V, P, R> {
    status: ChannelStatus,

    topic: T,
    rejoin_timeout: Duration,
    rejoin_after: ExponentialBackoff,
    params: Option<serde_json::Value>,

    replies: RepliesMapping<T>,
    reference: Reference,
    join_ref: u64,

    rejoin_tx: UnboundedSender<RejoinChannelMessage<T, Value, Value, Value>>,
    rejoin_rx: UnboundedReceiver<RejoinChannelMessage<T, Value, Value, Value>>,

    // In from handler
    handler_rx: UnboundedReceiver<HandlerChannelMessage<T>>,
    // In from handler (for internal messages)
    handler_internal_rx: UnboundedReceiver<HandlerChannelInternalMessage<T, V, P, R>>,

    // Out to socket
    out_tx: UnboundedSender<ChannelSocketMessage<T>>,
    // In from socket
    in_rx: UnboundedReceiver<SocketChannelMessage<T>>,

    // Broadcaster for non-reply messages
    broadcast: broadcast::Sender<Message<T, V, P, R>>,
}

impl<T, V, P, R> Channel<T, V, P, R>
where
    T: Serialize + DeserializeOwned + Debug,
    V: Serialize + DeserializeOwned + Debug,
    P: Serialize + DeserializeOwned + Debug,
    R: Serialize + DeserializeOwned + Debug,
{
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
                                    Ok(p.try_map_push_reply(serde_json::from_value)?
                                        .try_map_custom(serde_json::from_value)?)
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

    fn outbound_leave(
        &mut self,
        message: WithCallback<()>,
        reply_callback: oneshot::Sender<Result<Message<T, Value, Value, Value>, Error>>,
    ) -> Result<(), SendError<ChannelSocketMessage<T>>>
    where
        T: Clone,
    {
        let message = message.map(|_| Message::leave(self.topic.clone(), self.reference.next()));
        self.outbound_inner(message, reply_callback)
    }

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

    pub(crate) async fn run(mut self)
    where
        T: Clone + Send + Sync + 'static + Debug,
        V: Send + 'static,
        P: Send + 'static,
        R: Send + 'static,
    {
        'retry: loop {
            let mut rejoin: OptionFuture<_> = match self.status {
                ChannelStatus::Errored | ChannelStatus::Rejoin => {
                    let rejoiner = Rejoin {
                        rejoin_after: self.rejoin_after.clone(),
                        timeout: self.rejoin_timeout,
                        reference: self.reference.clone(),
                        topic: self.topic.clone(),
                        params: self.params.clone(),
                        rejoin_tx: self.rejoin_tx.clone(),
                    };
                    Some(tokio::spawn(rejoiner.join_with_backoff())).into()
                }
                _ => None.into(),
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
                    // Note that on a successful rejoin, the channel status is set to Joined, which means that it is
                    // impossible for the rejoin to be polled twice.
                    Some(v) = &mut rejoin, if self.status.should_rejoin() => {
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
                    ChannelStatus::Errored => {
                        info!(?self.topic, "errored");
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

struct RejoinChannelMessage<T, V, P, R> {
    message: WithCallback<Message<T, V, P, R>>,
    reply_callback: oneshot::Sender<Result<Message<T, V, P, R>, Error>>,
}

#[derive(Clone, Debug)]
struct Rejoin<T, V, P> {
    rejoin_after: ExponentialBackoff,
    timeout: Duration,
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

        let res = run_message::<T, V, P, serde_json::Value>(rx, res_rx, self.timeout)
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
