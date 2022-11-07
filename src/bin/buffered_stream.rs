use rusoto_dynamodb::DynamoDbClient;
use tokio::time::Instant;

use dynamodb_rust_concurrent::common::{batch_write_item, make_items, make_test_date, TestClient};

/// stream, buffered で少しずつ処理する
#[allow(clippy::extra_unused_lifetimes)]
async fn concurrent_stream<'a>(
    client: &'_ DynamoDbClient,
    chunks: Vec<Vec<i32>>,
) -> Result<(), ()> {
    use futures::{StreamExt, TryStreamExt};

    let result = futures::stream::iter(chunks.into_iter().map(|chunk| {
        let cloned_client = client.clone();

        tokio::spawn(async move {
            println!("start: {:?}", std::thread::current().id());

            let items = make_items(chunk);
            let result = batch_write_item(&cloned_client, items).await;

            println!("end: {:?}", std::thread::current().id());

            result
        })
    }))
    .buffered(4)
    .try_collect::<Vec<_>>()
    .await;

    result.map_err(|_| ())?;

    Ok(())
}

#[tokio::main(flavor = "multi_thread", worker_threads = 10)]
async fn main() {
    let client = TestClient::default().into_inner();

    let test_data = make_test_date();

    let start = Instant::now();

    let res = concurrent_stream(&client, test_data).await;

    println!("{:?}", res);

    let duration = start.elapsed();
    println!("elapsed time: {:?}", duration);
}
