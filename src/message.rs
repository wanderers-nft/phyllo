use crate::event::Event;
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use tokio_tungstenite::tungstenite;

#[derive(Debug)]
pub struct Message<T, U> {
    join_ref: u64,
    reference: u64,
    topic: String,
    event: Event<T>,
    payload: U,
}

impl<T> Message<T, ()>
where
    T: ToString,
{
    pub fn heartbeat() -> Self {
        Self {
            join_ref: 0,
            reference: 0,
            topic: "phoenix".to_string(),
            event: Event::Hearbeat,
            payload: (),
        }
    }
}

impl<T, U> TryFrom<Message<T, U>> for tungstenite::Message
where
    T: Serialize,
    U: Serialize,
{
    type Error = serde_json::Error;

    fn try_from(value: Message<T, U>) -> Result<Self, Self::Error> {
        Ok(Self::Text(serde_json::to_string(&value)?))
    }
}

impl<T, U> Serialize for Message<T, U>
where
    T: Serialize,
    U: Serialize,
{
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        (
            self.join_ref,
            self.reference,
            &self.topic,
            &self.event,
            &self.payload,
        )
            .serialize(serializer)
    }
}

impl<'de, T, U> Deserialize<'de> for Message<T, U>
where
    T: DeserializeOwned,
    U: DeserializeOwned,
{
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        Deserialize::deserialize(deserializer).map(
            |(join_ref, reference, topic, event, payload)| Message {
                join_ref,
                reference,
                topic,
                event,
                payload,
            },
        )
    }
}
