use self::socket_builder::SocketBuilder;
use futures_util::stream::{SplitSink, SplitStream};
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc,
};
use tokio::{net::TcpStream, sync::Mutex};
use tokio_tungstenite::{tungstenite, MaybeTlsStream, WebSocketStream};
use url::Url;

pub mod socket_builder;

type Sink = SplitSink<WebSocketStream<MaybeTlsStream<TcpStream>>, tungstenite::Message>;
type Stream = SplitStream<WebSocketStream<MaybeTlsStream<TcpStream>>>;

#[derive(Clone)]
pub struct Socket {
    inner: Arc<InnerSocket>,
}

struct InnerSocket {
    sink: Mutex<Sink>,
    stream: Mutex<Stream>,
    reference: AtomicU64,
}

impl Socket {
    pub async fn new(endpoint: Url) -> Result<Self, tungstenite::Error> {
        Self::builder(endpoint).build().await
    }

    pub fn builder(endpoint: Url) -> SocketBuilder {
        SocketBuilder::new(endpoint)
    }

    pub fn next_ref(&self) -> u64 {
        self.inner.reference.fetch_add(1, Ordering::Relaxed)
    }
}
