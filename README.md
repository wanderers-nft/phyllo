# phyllo

Phoenix channels in Rust.
This crate uses the Actor model to provide Socket and Channel abstractions for connecting to, receiving and sending messages in a topical fashion.
The overall structure is based on the [reference JavaScript client](https://www.npmjs.com/package/phoenix).

## Example
Warning: the results returned may include NSFW links or comments.
```rust
use serde_json::Value;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // The socket is generic over a Topic.
    let mut socket = SocketBuilder::new(Url::parse("wss://furbooru.org/socket/websocket")?)
        .build::<String>()
        .await;

    // Each channel is generic over an Event and Payload type.
    // For simplicity we use serde_json::Value, but in your own code you should deserialize
    // to something strongly-typed.
    let (_channel, mut subscription) = socket
        .channel::<String, Value, Value>(ChannelBuilder::new("firehose".to_string()))
        .await?;

    loop {
        let v = subscription.recv().await?;
        println!("{:?}", v);
    }
}
```

## Features
TLS is not enabled by default. Enable either of the following for TLS support:

- `rustls-native-roots` (uses [`rustls-native-certs`](https://crates.io/crates/rustls-native-certs)
for root certificates)
- `rustls-webpki-roots` (uses [`webpki-roots`](https://crates.io/crates/webpki-roots) for root
certificates)

License: MIT OR Apache-2.0
