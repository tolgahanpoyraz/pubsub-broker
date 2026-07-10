# pubsub-broker

Concurrent pub/sub message broker in Rust. tokio + axum + WebSockets.

## How it works

Subscribers connect over WebSocket and subscribe to topics. Publishers send messages to topics, and the broker fans them out to all subscribers.

Each subscriber gets a bounded mpsc channel. The publish path doesn't touch sockets directly; it just does `try_send` into each channel. A separate task per connection drains the channel and writes to the socket. This means a slow subscriber can't block anyone else.

Locks are `std::sync::RwLock`, not `tokio::sync::RwLock`. The point is that the guard is `!Send`, so the compiler won't let you hold it across an `.await`.

## Wire protocol

JSON over WebSocket.

```json
{"op": "Subscribe", "topic": "foo"}
{"op": "Publish", "topic": "foo", "data": "hello"}
{"op": "Unsubscribe", "topic": "foo"}
```

```json
{"type": "Message", "topic": "foo", "data": "hello"}
{"type": "Lagged", "topic": "foo", "dropped": 12}
{"type": "Ack", "op": "Subscribe", "topic": "foo"}
```

## Build

```
cargo build
cargo run
```

## Status

Work in progress.

## License

MIT
