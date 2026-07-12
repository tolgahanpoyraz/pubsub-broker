#[tokio::main]
async fn main() {
    let (_, handle) = pubsub_broker::run_server("127.0.0.1:3000").await;
    let _ = handle.await;
}
