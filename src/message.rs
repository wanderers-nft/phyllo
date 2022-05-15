use std::fmt::Display;

use crate::event::Event;
use serde::{de::DeserializeOwned, Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub struct Message<T, U> {
    join_ref: u64,
    #[serde(rename = "ref")]
    reference: u64,
    topic: String,
    event: Event<T>,
    payload: U,
}

impl<T, U> Message<T, U>
where
    T: Display,
    U: Serialize + DeserializeOwned,
{
}
