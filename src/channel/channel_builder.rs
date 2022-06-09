use backoff::ExponentialBackoff;
use futures_util::future::OptionFuture;
use serde::{de::DeserializeOwned, Serialize};
use std::{
    collections::{hash_map::Entry, HashMap},
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::{
    select,
    sync::{
        broadcast,
        mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender},
        oneshot,
    },
};
use tokio_tungstenite::tungstenite;

use crate::{
    error::{ChannelJoinError, Error},
    message::{
        event::{Event, ProtocolEvent},
        run_message, EventPayloadResult, Message, MessageResult, Payload, PushStatus,
        TungsteniteMessageResult,
    },
    socket::Reference,
};

use super::{ChannelHandler, ChannelStatus, HandlerToChannelMessage, SocketChannelMessage};

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
        out_tx: UnboundedSender<TungsteniteMessageResult>,
        in_rx: UnboundedReceiver<SocketChannelMessage>,
    ) -> ChannelHandler<T, V, P, R>
    where
        T: Serialize + DeserializeOwned + Send + Sync + Clone + 'static,
        V: Serialize + DeserializeOwned + Send + Clone + 'static,
        P: Serialize + DeserializeOwned + Send + Clone + 'static,
        R: Serialize + DeserializeOwned + Send + Clone + 'static,
    {
        let (handler_tx, handler_rx) = unbounded_channel();
        let (broadcast_tx, _) = broadcast::channel(self.broadcast_buffer);
        let (close_tx, close_rx) = broadcast::channel(1);
        let (rejoin_tx, rejoin_rx) = unbounded_channel();

        let channel: Channel<T, V, P, R> = Channel {
            status: ChannelStatus::Closed,
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
            out_tx,
            in_rx,
            broadcast: broadcast_tx.clone(),
            close: close_rx,
        };

        tokio::spawn(channel.run());

        ChannelHandler {
            handler_tx,
            timeout: self.timeout,
            broadcast_tx,
            close: close_tx,
        }
    }
}

type RepliesMapping<T, V, P, R> = HashMap<u64, oneshot::Sender<Result<Message<T, V, P, R>, Error>>>;

type RejoinToChannelMessage<T, V, P, R> = (
    MessageResult<T, V, P, R>,
    oneshot::Sender<Result<Message<T, V, P, R>, Error>>,
);

#[derive(Debug)]
struct Channel<T, V, P, R> {
    status: ChannelStatus,

    topic: T,
    rejoin_timeout: Duration,
    rejoin_after: ExponentialBackoff,
    params: Option<serde_json::Value>,

    replies: RepliesMapping<T, V, P, R>,
    reference: Reference,
    join_ref: u64,

    rejoin_tx: UnboundedSender<RejoinToChannelMessage<T, V, P, serde_json::Value>>,
    rejoin_rx: UnboundedReceiver<RejoinToChannelMessage<T, V, P, serde_json::Value>>,

    // In from handler
    handler_rx: UnboundedReceiver<HandlerToChannelMessage<T, V, P, R>>,
    // Out to socket
    out_tx: UnboundedSender<TungsteniteMessageResult>,
    // In from socket
    in_rx: UnboundedReceiver<SocketChannelMessage>,
    // Broadcaster for non-reply messages
    broadcast: broadcast::Sender<Message<T, V, P, R>>,

    close: broadcast::Receiver<()>,
}

impl<T, V, P, R> Channel<T, V, P, R>
where
    T: Serialize + DeserializeOwned,
    V: Serialize + DeserializeOwned,
    P: Serialize + DeserializeOwned,
    R: Serialize + DeserializeOwned,
{
    fn send_reply(&mut self, reference: u64, message: Result<Message<T, V, P, R>, Error>) {
        if let Some(reply) = self.replies.remove(&reference) {
            // todo: handle this error
            let _ = reply.send(message);
        }
    }

    async fn inbound(&mut self, message: SocketChannelMessage) -> Result<(), serde_json::Error> {
        match message {
            SocketChannelMessage::Message(msg) => {
                let msg: Message<T, V, P, R> = msg.try_into()?;

                match (&msg.event, &msg.payload) {
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
                        // TODO: Handle this error
                        let _ = self.broadcast.send(msg);
                    }
                    _ => {}
                };
            }
            SocketChannelMessage::ChannelStatus(cs) => {
                self.status = cs;
            }
        };
        Ok(())
    }

    async fn outbound(&mut self, (message, reply_callback): HandlerToChannelMessage<T, V, P, R>)
    where
        T: Clone,
    {
        let EventPayloadResult {
            event,
            payload,
            callback,
        } = message;

        let reference = self.reference.next();

        let message = MessageResult {
            message: Message::new(
                self.join_ref,
                reference,
                self.topic.clone(),
                event,
                Some(payload),
            ),
            callback,
        };

        self.outbound_inner(message, reply_callback)

        // match self.replies.entry(reference) {
        //     Entry::Occupied(_) => {
        //         panic!("reference already used, wtf")
        //     }
        //     Entry::Vacant(e) => {
        //         e.insert(reply_callback);
        //     }
        // }

        // let message = match message.try_into() {
        //     Ok(v) => v,
        //     Err(e) => {
        //         self.send_reply(reference, Err(Error::Serde(e)));
        //         return;
        //     }
        // };

        // // todo: handle this error
        // let _ = self.out_tx.send(message);
    }

    fn outbound_inner(&mut self, message: MessageResult<T, V, P, R>, callback: oneshot::Sender<Result<Message<T, V, P, R>, Error>>) {
        let reference = message.message.reference.unwrap();
        match self.replies.entry(reference) {
            Entry::Occupied(_) => {
                panic!("reference already used, wtf")
            }
            Entry::Vacant(e) => {
                e.insert(callback);
            }
        }

        let message = match message.try_into() {
            Ok(v) => v,
            Err(e) => {
                self.send_reply(reference, Err(Error::Serde(e)));
                return;
            }
        };

        let _ = self.out_tx.send(message);
    }

    async fn outbound_2(&mut self, (message, reply_callback): RejoinToChannelMessage<T, V, P, serde_json::Value>) {
        let MessageResult {
            message,
            callback,
        } = message;

        todo!()
    }

    pub(crate) async fn run(mut self)
    where
        T: Clone + Send + Sync + 'static,
        V: Send + 'static,
        P: Send + 'static,
        R: Send + 'static,
    {
        'retry: loop {
            let mut rejoin: OptionFuture<_> = if self.status.should_rejoin() {
                let rejoiner = Rejoin {
                    rejoin_after: self.rejoin_after.clone(),
                    timeout: self.rejoin_timeout,
                    reference: self.reference.clone(),
                    topic: self.topic.clone(),
                    params: self.params.clone(),
                    rejoin_tx: self.rejoin_tx.clone(),
                };
                Some(tokio::spawn(rejoiner.join_with_backoff())).into()
            } else {
                None.into()
            };

            'inner: loop {
                select! {
                    // Close signal
                    _ = self.close.recv() => {
                        let leave_message = Message::leave(
                            self.topic.clone(),
                            self.reference.next()
                        ).try_into().unwrap();
                        let _ = self.inbound(SocketChannelMessage::Message(leave_message)).await;
                        break 'retry;
                    },

                    // In from channel handler. Only attempt to pull values from the queue if we know we can send them out
                    Some(v) = self.handler_rx.recv(), if !self.status.should_rejoin() => {
                        let _ = self.outbound(v).await;
                    },

                    // In from inbound
                    Some(v) = self.in_rx.recv() => {
                        let _ = self.inbound(v).await;
                    },

                    // In from rejoiner
                    Some(v) = self.rejoin_rx.recv() => {
                        let (r, c) = v;
                        let _ = self.outbound_inner(r, c);
                    }

                    // Rejoiner
                    Some(v) = &mut rejoin, if self.status.should_rejoin() => {
                        match v {
                            Ok(Ok(new_join_ref)) => {
                                self.join_ref = new_join_ref;
                            },
                            _ => {
                                break 'inner;
                            }
                        }
                    }
                }
            }
        }
    }
}

#[derive(Clone, Debug)]
struct Rejoin<T, V, P> {
    rejoin_after: ExponentialBackoff,
    timeout: Duration,
    reference: Reference,
    topic: T,
    params: Option<serde_json::Value>,
    rejoin_tx: UnboundedSender<RejoinToChannelMessage<T, V, P, serde_json::Value>>,
}

impl<T, V, P> Rejoin<T, V, P>
where
    T: Serialize + Clone + Send,
    V: Serialize,
    P: Serialize,
{
    async fn join(&self) -> Result<u64, ChannelJoinError<P>> {
        let join_ref = self.reference.next();
        let message = Message::<T, V, P, serde_json::Value>::join(
            join_ref,
            self.topic.clone(),
            self.params.clone(),
        );

        let (message, rx) = MessageResult::new(message);
        let (res_tx, res_rx) = oneshot::channel();

        let _ = self.rejoin_tx.send((message, res_tx));

        let res = run_message::<T, V, P, serde_json::Value>(rx, res_rx, self.timeout).await?;

        match res.payload {
            Some(Payload::PushReply {
                status: PushStatus::Error,
                response: p,
            }) => Err(ChannelJoinError::Join(p)),
            _ => Ok(join_ref),
        }
    }

    async fn join_with_backoff(self) -> Result<u64, ChannelJoinError<P>> {
        backoff::future::retry(self.rejoin_after.clone(), || async {
            Ok(self.join().await?)
        })
        .await
    }
}
