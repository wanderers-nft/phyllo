use std::time::Duration;

use crate::{
    error::Error,
    message::{event::Event, run_message, EventPayloadResult, Message, Payload},
};
use serde::Serialize;
use tokio::{
    select,
    sync::{
        broadcast::{self, Receiver},
        mpsc::UnboundedSender,
        oneshot::{self, Sender},
    },
    time,
};
use tokio_tungstenite::tungstenite;

pub mod channel_builder;

type HandlerToChannelMessage<T, V, P, R> = (
    EventPayloadResult<V, P, R>,
    Sender<Result<Message<T, V, P, R>, Error>>,
);

#[derive(Clone)]
pub struct ChannelHandler<T, V, P, R> {
    // reference: Reference,
    // join_ref: Arc<AtomicU64>,
    handler_tx: UnboundedSender<HandlerToChannelMessage<T, V, P, R>>,
    timeout: Duration,
    broadcast_tx: broadcast::Sender<Message<T, V, P, R>>,
    close: broadcast::Sender<()>,
}

impl<T, V, P, R> ChannelHandler<T, V, P, R>
where
    T: Serialize,
    V: Serialize,
    P: Serialize,
    R: Serialize,
{
    pub async fn send(
        &mut self,
        event: Event<V>,
        payload: Payload<P, R>,
    ) -> Result<Message<T, V, P, R>, Error>
    where
        T: Serialize + Send + 'static,
        V: Serialize + Send + 'static,
        P: Serialize + Send + 'static,
        R: Serialize + Send + 'static,
    {
        let (event_payload, receiver) = EventPayloadResult::new(event, payload);
        let (tx, rx) = oneshot::channel();
        let _ = self.handler_tx.send((event_payload, tx));

        tokio::spawn(run_message(receiver, rx, self.timeout))
            .await
            .unwrap()
    }

    pub fn subscribe(&mut self) -> Receiver<Message<T, V, P, R>> {
        self.broadcast_tx.subscribe()
    }

    pub fn close(self) {
        let _ = self.close.send(());
    }
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
#[repr(u8)]
pub enum ChannelStatus {
    Closed,
    Errored,
    Joined,
    Joining,
    Leaving,
    SocketClosed,
}

impl ChannelStatus {
    pub fn should_rejoin(self) -> bool {
        self == Self::Closed || self == Self::Errored
    }
}

pub(crate) enum SocketChannelMessage {
    Message(tungstenite::Message),
    ChannelStatus(ChannelStatus),
}
