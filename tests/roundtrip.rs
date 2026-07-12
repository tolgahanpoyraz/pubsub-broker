use futures_util::{SinkExt, StreamExt};
use pubsub_broker::run_server;
use tokio_tungstenite::tungstenite::Message;

#[tokio::test]
pub async fn roundtrip_test() {
    let (addr_res, _) = run_server("127.0.0.1:0").await;
    let local_addr = addr_res.unwrap();

    let (mut stream1, _) = tokio_tungstenite::connect_async(format!("ws://{}/ws", local_addr))
        .await
        .unwrap();
    let (mut stream2, _) = tokio_tungstenite::connect_async(format!("ws://{}/ws", local_addr))
        .await
        .unwrap();

    stream1
        .send(Message::Text(
            r#"{"op": "subscribe", "topic": "test_topic"}"#.into(),
        ))
        .await
        .expect("Subscribe failed");

    stream2
        .send(Message::Text(
            r#"{"op": "publish", "topic": "test_topic", "data": "test data"}"#.into(),
        ))
        .await
        .expect("Publish failed");

    let _ = stream1.next().await.expect("Zort");
}
