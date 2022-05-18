use futures_util::{SinkExt, StreamExt};
use std::{
    sync::{atomic::AtomicU64, Arc},
    time::Duration,
};
use tokio::{sync::Mutex, time};
use tokio_tungstenite::{
    connect_async_with_config,
    tungstenite::{self, protocol::WebSocketConfig},
};
use tracing::{error, info};
use url::Url;

use crate::message::Message;

use super::{InnerSocket, Socket};

pub struct SocketBuilder {
    endpoint: Url,
    websocket_config: Option<WebSocketConfig>,
    heartbeat: Duration,
    reconnect_after: Duration,
}

impl SocketBuilder {
    pub fn new(endpoint: Url) -> Self {
        Self {
            endpoint,
            websocket_config: None,
            heartbeat: Duration::from_millis(30000),
            reconnect_after: Duration::from_millis(5000),
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

    pub fn reconnect_after(&mut self, reconnect_after: Duration) {
        self.reconnect_after = reconnect_after;
    }

    pub async fn build(&self) -> Result<Socket, tungstenite::Error> {
        let (sink, stream) =
            connect_async_with_config(self.endpoint.clone(), self.websocket_config)
                .await?
                .0
                .split();

        let socket = Socket {
            inner: Arc::new(InnerSocket {
                sink: Mutex::new(sink),
                stream: Mutex::new(stream),
                reference: AtomicU64::new(0),
            }),
        };

        tokio::task::spawn(heartbeat_task(socket.clone(), self.heartbeat));

        Ok(socket)
    }
}

async fn heartbeat_task(socket: Socket, duration: Duration) {
    let mut interval = time::interval(duration);
    loop {
        interval.tick().await;
        let message = Message::<String, ()>::heartbeat().try_into().unwrap();

        match socket.inner.sink.lock().await.send(message).await {
            Ok(_) => {
                info!("sent heartbeat");
                continue;
            }
            Err(_) => {
                error!("could not send heartbeat");
                continue;
            }
        };
    }
}
