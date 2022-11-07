use std::collections::HashMap;

use itertools::Itertools;
use rusoto_core::Region;
use rusoto_dynamodb::{DynamoDbClient, WriteRequest};
use tokio::time::Instant;

use dynamodb_rust_concurrent::common::{batch_write_item, make_values};

/// stream で並列
#[allow(clippy::extra_unused_lifetimes)]
async fn concurrent_stream<'a>(
    client: &'_ DynamoDbClient,
    chunks: Vec<Vec<i32>>,
) -> Result<(), ()> {
    use futures::{StreamExt, TryStreamExt};

    let result = futures::stream::iter(chunks.into_iter().map(|c| {
        let cloned_client = client.clone();
        tokio::spawn(async move {
            println!("start: {:?}", std::thread::current().id());

            let values: Vec<WriteRequest> = make_values(c);
            let mut items = HashMap::new();
            items.insert("users".to_string(), values);
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
    let client = DynamoDbClient::new(Region::Custom {
        name: "ap-northeast-1".to_string(),
        endpoint: "http://localhost:4566".to_string(),
    });
    let test_data: Vec<Vec<i32>> = (0..1000)
        .into_iter()
        .chunks(25)
        .into_iter()
        .map(|v| v.into_iter().collect())
        .collect();

    let start = Instant::now();

    let res = concurrent_stream(&client, test_data).await;

    println!("{:?}", res);

    let duration = start.elapsed();
    println!("elapsed time: {:?}", duration);
}
