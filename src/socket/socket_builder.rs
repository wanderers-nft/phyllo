use super::{HandlerSocketMessage, Reference, SocketHandler};
use crate::{
    channel::{ChannelSocketMessage, ChannelStatus, SocketChannelMessage},
    message::Message,
};
use backoff::ExponentialBackoff;
use futures_util::{stream::SplitSink, SinkExt, StreamExt};
use serde::{de::DeserializeOwned, Serialize};
use std::{collections::HashMap, fmt::Debug, hash::Hash, time::Duration};
use tokio::{
    net::TcpStream,
    select,
    sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender},
    time,
};
use tokio_tungstenite::{
    connect_async_with_config,
    tungstenite::{self, protocol::WebSocketConfig},
    MaybeTlsStream, WebSocketStream,
};
use tracing::{info, instrument, warn};
use url::Url;

type Sink = SplitSink<WebSocketStream<MaybeTlsStream<TcpStream>>, tungstenite::Message>;
// type Stream = SplitStream<WebSocketStream<MaybeTlsStream<TcpStream>>>;
type TungsteniteWebSocketStream = WebSocketStream<MaybeTlsStream<TcpStream>>;

/// What the socket should do if an IO error is encountered.
#[derive(Debug, Clone, Copy)]
pub enum OnIoError {
    /// Close the socket connection permanently.
    Die,
    /// Retry according to the configured reconnection strategy.
    Retry,
}

/// Builder for a socket
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
    pub fn endpoint(&mut self, mut endpoint: Url) {
        // Only vsn=2.0.0 is supported
        endpoint.query_pairs_mut().append_pair("vsn", "2.0.0");

        self.endpoint = endpoint;
    }

    /// Sets the configuration for the underlying `tungstenite` websocket.
    pub fn websocket_config(&mut self, websocket_config: Option<WebSocketConfig>) {
        self.websocket_config = websocket_config;
    }

    /// Sets the interval between heartbeat messages.
    pub fn heartbeat(&mut self, heartbeat: Duration) {
        self.heartbeat = heartbeat;
    }

    /// Sets the strategy for attempting reconnection using exponential backoff.
    pub fn reconnect(&mut self, reconnect: ExponentialBackoff) {
        self.reconnect = reconnect;
    }

    /// Sets how to handle an IO error.
    pub fn on_io_error(&mut self, on_io_error: OnIoError) {
        self.on_io_error = on_io_error;
    }

    /// Spawns the `Socket` and returns a corresponding `SocketHandler`.
    pub async fn build<T>(&self) -> SocketHandler<T>
    where
        T: Serialize + DeserializeOwned + Eq + Hash + Send + Sync + 'static + Debug,
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
    T: Serialize + DeserializeOwned + Eq + Hash + Send + 'static + Debug,
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

                                if self.subscriptions.contains_key(&topic) {
                                    panic!("channel already exists");
                                }
                                self.subscriptions.insert(topic, in_tx);

                                let _ = callback.send((in_rx, self.out_tx.clone()));

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
            Message::<String, (), (), ()>::heartbeat(reference)
                .try_into()
                .unwrap();

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

    /// Transforms a websocket message into a Phoenix message, then relaying it to the appropriate `Channel`.
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
