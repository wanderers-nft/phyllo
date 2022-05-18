use serde::Serialize;
use std::time::Duration;

pub struct ChannelBuilder<T> {
    topic: T,
    timeout: Duration,
    rejoin_after: Duration,
    params: serde_json::Value,
}

impl<T> ChannelBuilder<T>
where
    T: Serialize,
{
    pub fn new(topic: T) -> Self {
        Self {
            topic,
            timeout: Duration::from_millis(20000),
            rejoin_after: Duration::from_millis(5000),
            params: serde_json::Value::Null,
        }
    }

    pub fn topic(&mut self, topic: T) {
        self.topic = topic;
    }

    pub fn timeout(&mut self, timeout: Duration) {
        self.timeout = timeout;
    }

    pub fn rejoin_after(&mut self, rejoin_after: Duration) {
        self.rejoin_after = rejoin_after;
    }

    pub fn params<U>(&mut self, params: U)
    where
        U: Serialize,
    {
        self.try_params(params)
            .expect("could not serialize parameter");
    }

    pub fn try_params<U>(&mut self, params: U) -> Result<(), serde_json::Error>
    where
        U: Serialize,
    {
        self.params = serde_json::to_value(params)?;
        Ok(())
    }
}
