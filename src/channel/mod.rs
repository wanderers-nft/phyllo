pub mod channel_builder;

pub struct Channel {
    state: ChannelState,
}

pub enum ChannelState {
    Closed,
    Errored,
    Joined,
    Joining,
    Leaving,
}
