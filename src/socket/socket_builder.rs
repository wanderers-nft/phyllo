use super::{HandlerSocketMessage, Reference, SocketHandler};
use crate::{
    channel::{ChannelSocketMessage, ChannelStatus, SocketChannelMessage},
    message::Message,
};
use backoff::ExponentialBackoff;
use futures_util::{stream::SplitSink, SinkExt, StreamExt};
use serde::{de::DeserializeOwned, Serialize};
use std::{collections::HashMap, fmt::Debug, hash::Hash, sync::Arc, time::Duration};
use tokio::{
    net::TcpStream,
    select,
    sync::{
        mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender},
        Mutex,
    },
    time,
};
use tokio_tungstenite::{
    connect_async_with_config,
    tungstenite::{self, protocol::WebSocketConfig},
    MaybeTlsStream, WebSocketStream,
};
use tracing::{error, info};
use url::Url;

type Sink = SplitSink<WebSocketStream<MaybeTlsStream<TcpStream>>, tungstenite::Message>;
// type Stream = SplitStream<WebSocketStream<MaybeTlsStream<TcpStream>>>;
type TungsteniteWebSocketStream = WebSocketStream<MaybeTlsStream<TcpStream>>;

#[derive(Clone, Copy)]
pub enum OnIoError {
    Die,
    Retry,
}

#[derive(Clone)]
pub struct SocketBuilder {
    endpoint: Url,
    websocket_config: Option<WebSocketConfig>,
    heartbeat: Duration,
    reconnect: ExponentialBackoff,
    on_io_error: OnIoError,
}

impl SocketBuilder {
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

    pub fn endpoint(&mut self, mut endpoint: Url) {
        // Only vsn=2.0.0 is supported
        endpoint.query_pairs_mut().append_pair("vsn", "2.0.0");

        self.endpoint = endpoint;
    }

    pub fn websocket_config(&mut self, websocket_config: Option<WebSocketConfig>) {
        self.websocket_config = websocket_config;
    }

    pub fn heartbeat(&mut self, heartbeat: Duration) {
        self.heartbeat = heartbeat;
    }

    pub fn reconnect(&mut self, reconnect: ExponentialBackoff) {
        self.reconnect = reconnect;
    }

    pub fn on_io_error(&mut self, on_io_error: OnIoError) {
        self.on_io_error = on_io_error;
    }

    pub async fn build<T>(&self) -> SocketHandler<T>
    where
        T: Serialize + DeserializeOwned + Eq + Hash + Send + 'static + Debug,
    {
        // Send, receiver for client -> server
        let (out_tx, out_rx) = unbounded_channel();

        // Send, receiver for handler -> socket
        let (handler_tx, handler_rx) = unbounded_channel();

        let subscriptions = Arc::new(Mutex::new(HashMap::new()));
        let reference = Reference::new();

        // Spawn task
        let socket: Socket<T> = Socket {
            handler_rx,
            out_tx,
            out_rx,
            subscriptions: subscriptions.clone(),
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

struct Socket<T> {
    handler_rx: UnboundedReceiver<HandlerSocketMessage<T>>,

    out_tx: UnboundedSender<ChannelSocketMessage<T>>,
    out_rx: UnboundedReceiver<ChannelSocketMessage<T>>,

    subscriptions: Arc<Mutex<HashMap<T, UnboundedSender<SocketChannelMessage<T>>>>>,

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
    async fn connect_with_backoff(&self) -> Result<TungsteniteWebSocketStream, tungstenite::Error> {
        backoff::future::retry(self.reconnect.clone(), || async {
            Ok(connect_async_with_config(self.endpoint.clone(), self.websocket_config).await?)
        })
        .await
        .map(|(twss, _)| twss)
    }

    pub async fn run(mut self) -> Result<(), tungstenite::Error> {
        let mut interval = {
            let mut i = time::interval(self.heartbeat);
            i.set_missed_tick_behavior(time::MissedTickBehavior::Skip);
            i
        };

        'retry: loop {
            // TODO: Lock all channels
            {
                let mut subscriptions = self.subscriptions.lock().await;
                for (_, chan) in subscriptions.iter_mut() {
                    let _ = chan.send(SocketChannelMessage::ChannelStatus(ChannelStatus::Rejoin));
                }
            }
            self.reference.reset();

            // Connect to socket
            let (mut sink, mut stream) = self.connect_with_backoff().await.map(|ws| ws.split())?;
            info!("connected to websocket: {:?}", self.endpoint);

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

                                let mut subscriptions = self.subscriptions.lock().await;
                                if subscriptions.contains_key(&topic) {
                                    panic!("channel already exists");
                                }
                                subscriptions.insert(topic, in_tx);

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
        let mut subscriptions = self.subscriptions.lock().await;
        for (topic, chan) in subscriptions.iter_mut() {
            info!(?topic, "close signal");
            let _ = chan.send(SocketChannelMessage::ChannelStatus(
                ChannelStatus::SocketClosed,
            ));
        }

        Ok(())
    }

    async fn send_hearbeat(reference: u64, sink: &mut Sink) -> Result<(), tungstenite::Error> {
        let heartbeat_message: tungstenite::Message =
            Message::<String, (), (), ()>::heartbeat(reference)
                .try_into()
                .unwrap();

        info!(%heartbeat_message, "to websocket");

        sink.send(heartbeat_message).await
    }

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
                let mut subscription = self.subscriptions.lock().await;
                subscription.remove(&topic);
            }
        }
        Ok(())
    }

    async fn from_websocket(
        &mut self,
        message: Result<tungstenite::Message, tungstenite::Error>,
    ) -> Result<(), tungstenite::Error> {
        match message {
            Ok(tungstenite::Message::Text(t)) => {
                info!(%t, "from websocket");
                let _ = self.decode_and_relay(t).await;
                Ok(())
            }
            Err(e) => {
                error!(%e, "from websocket");
                Err(e)
            }
            _ => Ok(()),
        }
    }

    async fn decode_and_relay(&mut self, text: String) -> Result<(), serde_json::Error> {
        use serde_json::Value;

        // To determine which topic we should relay the raw tungstenite mesage to, we "ignore" V, P but deserialise for T.
        let message = serde_json::from_str::<Message<T, Value, Value, Value>>(&text)?;

        let subscriptions = self.subscriptions.lock().await;
        if let Some(chan) = subscriptions.get(&message.topic) {
            // TODO: handle this error
            let _ = chan.send(SocketChannelMessage::Message(message));
        }
        Ok(())
    }
}
