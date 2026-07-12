use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use pubsub_broker::run_server;
use tokio_tungstenite::tungstenite::Message;

#[tokio::test]
pub async fn roundtrip_test() {
    let (addr_res, _, _reg) = run_server("127.0.0.1:0").await;
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

    let _ = stream1.next().await.expect("Receive failed");
}

#[tokio::test]
pub async fn isolation_test() {
    let (addr_res, _, _reg) = run_server("127.0.0.1:0").await;
    let local_addr = addr_res.unwrap();

    let (mut stream1, _) = tokio_tungstenite::connect_async(format!("ws://{}/ws", local_addr))
        .await
        .unwrap();
    let (mut stream2, _) = tokio_tungstenite::connect_async(format!("ws://{}/ws", local_addr))
        .await
        .unwrap();
    let (mut stream3, _) = tokio_tungstenite::connect_async(format!("ws://{}/ws", local_addr))
        .await
        .unwrap();

    stream1
        .send(Message::Text(
            r#"{"op": "subscribe", "topic": "t1"}"#.into(),
        ))
        .await
        .expect("Subscribe failed");

    stream2
        .send(Message::Text(
            r#"{"op": "subscribe", "topic": "t2"}"#.into(),
        ))
        .await
        .expect("Subscribe failed");

    stream3
        .send(Message::Text(
            r#"{"op": "publish", "topic": "t2", "data": "test data"}"#.into(),
        ))
        .await
        .expect("Publish failed");

    stream2.next().await; // consume the ack
    let _ = stream2.next().await.expect("Receive failed");

    stream1.next().await; // consume the ack

    let res = tokio::time::timeout(Duration::from_secs(1), stream1.next()).await;

    assert!(res.is_err());
}

#[tokio::test]
pub async fn cleanup_on_disconnect_test() {
    let (addr_res, _, reg) = run_server("127.0.0.1:0").await;
    let local_addr = addr_res.unwrap();

    let (mut stream, _) = tokio_tungstenite::connect_async(format!("ws://{}/ws", local_addr))
        .await
        .unwrap();

    stream
        .send(Message::Text(
            r#"{"op": "subscribe", "topic": "t1"}"#.into(),
        ))
        .await
        .expect("Subscribe failed");
    stream
        .send(Message::Text(
            r#"{"op": "subscribe", "topic": "t2"}"#.into(),
        ))
        .await
        .expect("Subscribe failed");
    stream
        .send(Message::Text(
            r#"{"op": "subscribe", "topic": "t3"}"#.into(),
        ))
        .await
        .expect("Subscribe failed");

    // process acks
    stream.next().await;
    stream.next().await;
    stream.next().await;

    assert!(reg.get_topic_subscriber_count(&"t1".to_string()) == 1);
    assert!(reg.get_topic_subscriber_count(&"t2".to_string()) == 1);
    assert!(reg.get_topic_subscriber_count(&"t3".to_string()) == 1);

    let _ = stream.send(Message::Close(None)).await;
    tokio::time::sleep(Duration::from_secs(1)).await;

    assert!(reg.get_topic_subscriber_count(&"t1".to_string()) == 0);
    assert!(reg.get_topic_subscriber_count(&"t2".to_string()) == 0);
    assert!(reg.get_topic_subscriber_count(&"t3".to_string()) == 0);
}
