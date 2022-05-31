use super::{Reference, SocketHandler};
use crate::{
    channel::{ChannelStatus, SocketChannelMessage},
    message::{Message, TungsteniteMessageResult},
};
use backoff::ExponentialBackoff;
use futures_util::{stream::SplitSink, SinkExt, StreamExt};
use serde::{de::DeserializeOwned, Serialize};
use std::{
    collections::HashMap,
    fmt::Debug,
    hash::Hash,
    sync::{atomic::AtomicBool, Arc},
    time::Duration,
};
use tokio::{
    net::TcpStream,
    select,
    sync::{
        broadcast::{self, Receiver},
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
use url::Url;

type Sink = SplitSink<WebSocketStream<MaybeTlsStream<TcpStream>>, tungstenite::Message>;
// type Stream = SplitStream<WebSocketStream<MaybeTlsStream<TcpStream>>>;
type TungsteniteWebSocketStream = WebSocketStream<MaybeTlsStream<TcpStream>>;

pub enum OnDisconnect {
    Die,
    Retry,
}

#[derive(Clone)]
pub struct SocketBuilder {
    endpoint: Url,
    websocket_config: Option<WebSocketConfig>,
    heartbeat: Duration,
    reconnect: ExponentialBackoff,
}

impl SocketBuilder {
    pub fn new(mut endpoint: Url) -> Self {
        endpoint.query_pairs_mut().append_pair("vsn", "2.0.0");

        Self {
            endpoint,
            websocket_config: None,
            heartbeat: Duration::from_millis(30000),
            reconnect: ExponentialBackoff::default(),
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

    pub async fn build<T>(&self) -> SocketHandler<T>
    where
        T: Serialize + DeserializeOwned + Eq + Hash + Send + 'static + Debug,
    {
        // println!("{}", self.endpoint);

        // Send, receiver for client -> server
        let (out_tx, out_rx) = unbounded_channel::<TungsteniteMessageResult>();

        let subscriptions = Arc::new(Mutex::new(HashMap::new()));
        let (close_tx, _) = broadcast::channel(1);
        let reference = Reference::new();

        // Spawn task
        let socket: Socket<T> = Socket {
            out_rx,
            subscriptions: subscriptions.clone(),
            reference: reference.clone(),
            endpoint: self.endpoint.clone(),
            websocket_config: self.websocket_config,
            heartbeat: self.heartbeat,
            reconnect: self.reconnect.clone(),
            close: close_tx.subscribe(),
        };
        tokio::spawn(socket.run());

        SocketHandler {
            reference,
            out_tx,
            subscriptions,
            close: close_tx,
            is_closed: Arc::new(AtomicBool::new(false)),
        }
    }
}

struct Socket<T> {
    out_rx: UnboundedReceiver<TungsteniteMessageResult>,
    subscriptions: Arc<Mutex<HashMap<T, UnboundedSender<SocketChannelMessage>>>>,

    reference: Reference,
    endpoint: Url,
    websocket_config: Option<WebSocketConfig>,
    heartbeat: Duration,
    reconnect: ExponentialBackoff,
    close: Receiver<()>,
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
                    let _ = chan.send(SocketChannelMessage::ChannelStatus(ChannelStatus::Closed));
                }
            }

            // Connect to socket
            let (mut sink, mut stream) = self.connect_with_backoff().await.map(|ws| ws.split())?;

            'conn: loop {
                if let Err(tungstenite::Error::Io(_)) = select! {
                    // Close signal
                    _ = self.close.recv() => {
                        let _ = sink.close().await;
                        break 'retry;
                    }

                    // Heartbeat
                    _ = interval.tick() => Socket::<T>::send_hearbeat(self.reference.next(), &mut sink).await,

                    // Outbound message to be sent
                    v = self.out_rx.recv() => Socket::<T>::on_outbound(&mut sink, v).await,

                    // Inbound message to be relayed
                    i = stream.next() => {
                        // If the stream is closed we can never receive any more messages. Break
                        match i {
                            Some(i) => {
                                match self.on_inbound(i).await {
                                    Ok(()) => Ok(()),
                                    Err(_) => break 'conn,
                                }
                            }
                            None => break 'conn,
                        }
                    },
                } {
                    break 'conn;
                };
            }
        }

        // Send close signal to all subscriptions
        let mut subscriptions = self.subscriptions.lock().await;
        for (_, chan) in subscriptions.iter_mut() {
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
        // println!("Heartbeat: {:?}", &heartbeat_message);

        sink.send(heartbeat_message).await
    }

    async fn on_outbound(
        sink: &mut Sink,
        message: Option<TungsteniteMessageResult>,
    ) -> Result<(), tungstenite::Error> {
        // println!("Outbound: {:?}", &message);
        // Check if channel is closed
        if let Some(message_res) = message {
            let _ = message_res
                .callback
                .send(sink.feed(message_res.message).await);
        }
        Ok(())
    }

    async fn on_inbound(
        &mut self,
        message: Result<tungstenite::Message, tungstenite::Error>,
    ) -> Result<(), tungstenite::Error> {
        // println!("Inbound: {:?}", &message);
        match message {
            Ok(tungstenite::Message::Text(t)) => {
                let _ = self.decode_and_relay(t).await;
                Ok(())
            }
            Err(e) => Err(e),
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
            let _ = chan.send(SocketChannelMessage::Message(tungstenite::Message::Text(
                text,
            )));
        }
        Ok(())
    }
}
