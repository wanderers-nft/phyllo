use crate::{
    channel::{
        ChannelBuilder, ChannelHandler, ChannelSocketMessage, ChannelStatus, SocketChannelMessage,
    },
    error::RegisterChannelError,
    message::Message,
};
use backoff::ExponentialBackoff;
use futures_util::{stream::SplitSink, SinkExt, StreamExt};
use serde::{de::DeserializeOwned, Serialize};
use std::{
    collections::{hash_map::Entry, HashMap},
    fmt::Debug,
    hash::Hash,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::Duration,
};
use tokio::{
    net::TcpStream,
    select,
    sync::{
        broadcast,
        mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender},
        oneshot,
    },
    time,
};
use tokio_tungstenite::{
    connect_async_with_config,
    tungstenite::{self, protocol::WebSocketConfig},
    MaybeTlsStream, WebSocketStream,
};
use tracing::{info, instrument, warn};
use url::Url;

/// Handler half of a `Socket`.
#[derive(Debug, Clone)]
pub struct SocketHandler<T> {
    reference: Reference,
    handler_tx: UnboundedSender<HandlerSocketMessage<T>>,
}

impl<T> SocketHandler<T>
where
    T: Serialize + DeserializeOwned + Hash + Eq + Clone + Send + Sync + 'static + Debug,
{
    /// Register a new channel for the socket, returning a corresponding [`ChannelHandler`].
    ///
    /// To avoid a potential race condition where the join is established and a message is received before [`ChannelHandler::subscribe`](crate::channel::ChannelHandler::subscribe)
    /// returns [(`broadcast` channels only receives values sent after a `subscribe` call)](tokio::sync::broadcast), a ready-to-use [`broadcast::Receiver`] is included
    /// in the return.
    ///
    /// # Errors
    /// If the underlying `Socket` has been dropped, or if the given topic has already been registered, an error is returned.
    pub async fn channel<V, P, R>(
        &mut self,
        channel_builder: ChannelBuilder<T>,
    ) -> Result<
        (
            ChannelHandler<T, V, P, R>,
            broadcast::Receiver<Message<T, V, P, R>>,
        ),
        RegisterChannelError,
    >
    where
        V: Serialize + DeserializeOwned + Clone + Send + 'static + Debug,
        P: Serialize + DeserializeOwned + Clone + Send + 'static + Debug,
        R: Serialize + DeserializeOwned + Clone + Send + 'static + Debug,
    {
        let (tx, rx) = oneshot::channel();

        let _ = self.handler_tx.send(HandlerSocketMessage::Subscribe {
            topic: channel_builder.topic.clone(),
            callback: tx,
        });

        let (channel_socket, socket_channel) = rx
            .await
            .map_err(|_| RegisterChannelError::SocketDropped)?
            .ok_or(RegisterChannelError::DuplicateTopic)?;

        Ok(
            channel_builder.build::<V, P, R>(
                self.reference.clone(),
                socket_channel,
                channel_socket,
            ),
        )
    }

    /// Close the socket, dropping all queued messages. This function will work even if the underlying socket has already been closed by another `SocketHandler`.
    pub fn close(self) {
        let _ = self.handler_tx.send(HandlerSocketMessage::Close);
    }
}

/// A monotonically-increasing counter for tracking messages.
#[derive(Clone, Debug)]
pub struct Reference(Arc<AtomicU64>);

impl Reference {
    /// Constructs a new `Reference`.
    pub(crate) fn new() -> Self {
        Self(Arc::new(AtomicU64::new(0)))
    }

    /// Fetch the next value.
    pub fn next(&self) -> u64 {
        self.0.fetch_add(1, Ordering::Relaxed)
    }

    /// Reset the counter.
    pub(crate) fn reset(&self) {
        self.0.store(0, Ordering::Relaxed);
    }
}

impl Default for Reference {
    fn default() -> Self {
        Self::new()
    }
}

/// Callback for a topic subscription message sent from a `SocketHandler` to a `Socket`.
type HandlerSocketSubscribeCallback<T> = oneshot::Sender<
    Option<(
        UnboundedReceiver<SocketChannelMessage<T>>,
        UnboundedSender<ChannelSocketMessage<T>>,
    )>,
>;

/// A message sent from a `SocketHandler` to a `Socket`.
#[derive(Debug)]
enum HandlerSocketMessage<T> {
    /// Close the socket.
    Close,
    /// Create a subscription for the topic.
    Subscribe {
        topic: T,
        callback: HandlerSocketSubscribeCallback<T>,
    },
}

type Sink = SplitSink<WebSocketStream<MaybeTlsStream<TcpStream>>, tungstenite::Message>;
// type Stream = SplitStream<WebSocketStream<MaybeTlsStream<TcpStream>>>;
type TungsteniteWebSocketStream = WebSocketStream<MaybeTlsStream<TcpStream>>;

/// What the socket should do if an IO error is encountered.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum OnIoError {
    /// Close the socket connection permanently.
    Die,
    /// Retry according to the configured reconnection strategy.
    Retry,
}

/// Builder for a `Socket`.
#[derive(Debug, Clone)]
pub struct SocketBuilder {
    endpoint: Url,
    websocket_config: Option<WebSocketConfig>,
    heartbeat: Duration,
    reconnect: ExponentialBackoff,
    on_io_error: OnIoError,
}

impl SocketBuilder {
    /// Constructs a new `SocketBuilder`. `endpoint` is the endpoint to connect to; only `vsn=2.0.0` is supported.
    pub fn new(mut endpoint: Url) -> Self {
        endpoint.query_pairs_mut().append_pair("vsn", "2.0.0");

        Self {
            endpoint,
            websocket_config: None,
            heartbeat: Duration::from_millis(30000),
            reconnect: ExponentialBackoff::default(),
            on_io_error: OnIoError::Retry,
        }
    }

    /// Sets the endpoint to connect to. Only `vsn=2.0.0` is supported.
    pub fn endpoint(&mut self, mut endpoint: Url) -> &mut Self {
        endpoint.query_pairs_mut().append_pair("vsn", "2.0.0");

        self.endpoint = endpoint;
        self
    }

    /// Sets the configuration for the underlying `tungstenite` websocket.
    pub fn websocket_config(&mut self, websocket_config: Option<WebSocketConfig>) -> &mut Self {
        self.websocket_config = websocket_config;
        self
    }

    /// Sets the interval between heartbeat messages.
    pub fn heartbeat(&mut self, heartbeat: Duration) -> &mut Self {
        self.heartbeat = heartbeat;
        self
    }

    /// Sets the strategy for attempting reconnection using exponential backoff.
    pub fn reconnect(&mut self, reconnect: ExponentialBackoff) -> &mut Self {
        self.reconnect = reconnect;
        self
    }

    /// Sets how to handle an IO error.
    pub fn on_io_error(&mut self, on_io_error: OnIoError) -> &mut Self {
        self.on_io_error = on_io_error;
        self
    }

    /// Spawns the `Socket` and returns a corresponding `SocketHandler`.
    pub async fn build<T>(&self) -> SocketHandler<T>
    where
        T: Serialize + DeserializeOwned + Eq + Clone + Hash + Send + Sync + 'static + Debug,
    {
        // Send, receiver for client -> server
        let (out_tx, out_rx) = unbounded_channel();

        // Send, receiver for handler -> socket
        let (handler_tx, handler_rx) = unbounded_channel();

        let subscriptions = HashMap::new();
        let reference = Reference::new();

        // Spawn task
        let socket: Socket<T> = Socket {
            handler_rx,
            out_tx,
            out_rx,
            subscriptions,
            reference: reference.clone(),
            endpoint: self.endpoint.clone(),
            websocket_config: self.websocket_config,
            heartbeat: self.heartbeat,
            reconnect: self.reconnect.clone(),
            on_io_error: self.on_io_error,
        };
        tokio::spawn(socket.run());

        SocketHandler {
            reference,
            handler_tx,
        }
    }
}

/// A socket for managing and receiving/sending Phoenix messages.
#[derive(Debug)]
struct Socket<T> {
    /// SocketHandler -> Socket
    handler_rx: UnboundedReceiver<HandlerSocketMessage<T>>,

    /// Tx for Channel -> Socket
    out_tx: UnboundedSender<ChannelSocketMessage<T>>,
    /// Rx for Channel -> Socket
    out_rx: UnboundedReceiver<ChannelSocketMessage<T>>,

    /// Mapping of channels to their channel senders
    subscriptions: HashMap<T, UnboundedSender<SocketChannelMessage<T>>>,

    /// Counter for heartbeat
    reference: Reference,

    endpoint: Url,
    websocket_config: Option<WebSocketConfig>,
    heartbeat: Duration,
    reconnect: ExponentialBackoff,
    on_io_error: OnIoError,
}

impl<T> Socket<T>
where
    T: Serialize + DeserializeOwned + Clone + Eq + Hash + Send + 'static + Debug,
{
    /// Connect to the websocket with exponential backoff.
    #[instrument(skip(self), fields(endpoint = %self.endpoint))]
    async fn connect_with_backoff(&self) -> Result<TungsteniteWebSocketStream, tungstenite::Error> {
        backoff::future::retry(self.reconnect.clone(), || async {
            info!("attempting connection");
            Ok(
                connect_async_with_config(self.endpoint.clone(), self.websocket_config)
                    .await
                    .map_err(|e| {
                        warn!(error = ?e);
                        e
                    })?,
            )
        })
        .await
        .map(|(twss, _)| twss)
    }

    /// Runs the `Socket` task.
    pub async fn run(mut self) -> Result<(), tungstenite::Error> {
        let mut interval = {
            let mut i = time::interval(self.heartbeat);
            i.set_missed_tick_behavior(time::MissedTickBehavior::Skip);
            i
        };

        'retry: loop {
            for (_, chan) in self.subscriptions.iter_mut() {
                let _ = chan.send(SocketChannelMessage::ChannelStatus(ChannelStatus::Rejoin));
            }
            self.reference.reset();

            // Connect to socket
            let (mut sink, mut stream) = self.connect_with_backoff().await.map(|ws| ws.split())?;
            info!("connected to websocket {}", self.endpoint);

            'conn: loop {
                if let Err(tungstenite::Error::Io(_)) = select! {
                    Some(v) = self.handler_rx.recv() => {
                        match v {
                            HandlerSocketMessage::Close => {
                                let _ = sink.close().await;
                                break 'retry;
                            },
                            HandlerSocketMessage::Subscribe { topic, callback } => {
                                let (in_tx, in_rx) = unbounded_channel();
                                let callback_value = match self.subscriptions.entry(topic.clone()) {
                                    Entry::Occupied(_) => {
                                        None
                                    },
                                    Entry::Vacant(e) => {
                                        e.insert(in_tx);
                                        Some((in_rx, self.out_tx.clone()))
                                    },
                                };
                                let _ = callback.send(callback_value);
                            },
                        }
                        Ok(())
                    },

                    // Heartbeat
                    _ = interval.tick() => Socket::<T>::send_hearbeat(self.reference.next(), &mut sink).await,

                    // Incoming message from channels
                    Some(v) = self.out_rx.recv() => self.from_channel(&mut sink, v).await,

                    // Incoming message from websocket
                    i = stream.next() => {
                        // If the stream is closed we can never receive any more messages. Break
                        match i {
                            Some(i) => {
                                match self.from_websocket(i).await {
                                    Ok(()) => Ok(()),
                                    Err(_) => break 'conn,
                                }
                            }
                            None => break 'conn,
                        }
                    },
                } {
                    match self.on_io_error {
                        OnIoError::Die => {
                            break 'retry;
                        }
                        OnIoError::Retry => {
                            break 'conn;
                        }
                    }
                };
            }
        }

        // Send close signal to all subscriptions
        for (topic, chan) in self.subscriptions.iter_mut() {
            info!(?topic, "close signal");
            let _ = chan.send(SocketChannelMessage::ChannelStatus(
                ChannelStatus::SocketClosed,
            ));
        }

        Ok(())
    }

    /// Sends a heartbeat message to the server.
    #[instrument(skip_all)]
    async fn send_hearbeat(reference: u64, sink: &mut Sink) -> Result<(), tungstenite::Error> {
        let heartbeat_message: tungstenite::Message =
            Message::heartbeat(reference).try_into().unwrap();

        info!(message = %heartbeat_message);

        sink.send(heartbeat_message).await
    }

    /// Handles an incoming message from a `Channel`.
    #[instrument(skip_all, fields(endpoint = %self.endpoint))]
    async fn from_channel(
        &mut self,
        sink: &mut Sink,
        message: ChannelSocketMessage<T>,
    ) -> Result<(), tungstenite::Error> {
        match message {
            ChannelSocketMessage::Message(message) => {
                info!(%message.content, "to websocket");
                let _ = message.callback.send(sink.send(message.content).await);
            }
            ChannelSocketMessage::TaskEnded(topic) => {
                info!(?topic, "removing task");
                self.subscriptions.remove(&topic);
            }
        }
        Ok(())
    }

    /// Handles an incoming message from the websocket.
    #[instrument(skip_all, fields(endpoint = %self.endpoint))]
    async fn from_websocket(
        &mut self,
        message: Result<tungstenite::Message, tungstenite::Error>,
    ) -> Result<(), tungstenite::Error> {
        match message {
            Ok(tungstenite::Message::Text(t)) => {
                info!(message = %t, "incoming");
                let _ = self.decode_and_relay(t).await;
                Ok(())
            }
            Err(e) => {
                warn!(error = ?e, "error received");
                Err(e)
            }
            _ => Ok(()),
        }
    }

    /// Transforms a websocket message into a `Message`, then relays it to the appropriate `Channel`.
    async fn decode_and_relay(&mut self, text: String) -> Result<(), serde_json::Error> {
        use serde_json::Value;

        // To determine which topic we should relay the raw tungstenite mesage to, we "ignore" V, P but deserialise for T.
        let message = serde_json::from_str::<Message<T, Value, Value, Value>>(&text)?;

        if let Some(chan) = self.subscriptions.get(&message.topic) {
            if let Err(e) = chan.send(SocketChannelMessage::Message(message)) {
                warn!(error = ?e, "failed to send message to channel");
            }
        }
        Ok(())
    }
}
