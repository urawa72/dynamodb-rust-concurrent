use std::collections::HashMap;

use itertools::Itertools;
use rusoto_core::Region;
use rusoto_dynamodb::{DynamoDbClient, WriteRequest};
use tokio::time::Instant;

use dynamodb_rust_concurrent::common::{batch_write_item, make_values};

/// tokio::task 使わずループ
async fn simple_loop(client: &DynamoDbClient, chunks: Vec<Vec<i32>>) -> Result<(), ()> {
    for chunk in chunks {
        println!("start: {:?}", std::thread::current().id());

        let values: Vec<WriteRequest> = make_values(chunk);
        let mut items = HashMap::new();
        items.insert("users".to_string(), values);
        batch_write_item(client, items).await.unwrap();

        println!("end: {:?}", std::thread::current().id());
    }

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

    let res = simple_loop(&client, test_data).await;

    println!("{:?}", res);

    let duration = start.elapsed();
    println!("elapsed time: {:?}", duration);
}
