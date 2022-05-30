use std::time::Duration;

use crate::{
    error::Error,
    message::{event::Event, EventPayloadResult, Message, Payload},
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
    async fn run_message(
        event: Event<V>,
        payload: Payload<P, R>,
        handler_tx: UnboundedSender<HandlerToChannelMessage<T, V, P, R>>,
        timeout: Duration,
    ) -> Result<Message<T, V, P, R>, Error>
    where
        T: Serialize,
        V: Serialize,
        P: Serialize,
        R: Serialize,
    {
        let timeout = time::sleep(timeout);
        tokio::pin!(timeout);

        let (event_payload, receiver) = EventPayloadResult::new(event, payload);
        let (tx, mut rx) = oneshot::channel();
        let _ = handler_tx.send((event_payload, tx));

        // Await the send
        select! {
            _ = &mut timeout => {
                return Err(Error::Timeout);
            },

            Err(e) = &mut rx => {
                panic!("todo: handle error on transmission");
            },


            v = receiver => {
                match v {
                    // todo: how tf do you handle this
                    Err(e) => todo!(),
                    Ok(Err(e)) => {
                        return Err(Error::WebSocket(e));
                    },
                    _ => {}
                };
            },
        };

        select! {
            _ = &mut timeout => {
                Err(Error::Timeout)
            }
            v = rx => {
                match v {
                    // todo: how tf do you handle this
                    Err(e) => todo!(),
                    Ok(v) => v,
                }
            }
        }
    }

    // pub async fn send_raw(&mut self, message: Message<T, V, P, R>) -> Result<Message<T, V, P, R>, Error>
    // where
    //     T: Serialize + Send + 'static,
    //     V: Serialize + Send + 'static,
    //     P: Serialize + Send + 'static,
    //     R: Serialize + Send + 'static,
    // {
    //     tokio::spawn(ChannelHandler::run_message(
    //         message,
    //         self.handler_tx.clone(),
    //         self.timeout,
    //     ))
    //     .await
    //     // todo: handle joinerror
    //     .unwrap()
    // }

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
        tokio::spawn(ChannelHandler::run_message(
            event,
            payload,
            self.handler_tx.clone(),
            self.timeout,
        ))
        .await
        // todo: handle joinerror
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
