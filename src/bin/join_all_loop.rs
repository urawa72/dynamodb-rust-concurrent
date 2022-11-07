use std::collections::HashMap;

use itertools::Itertools;
use rusoto_core::Region;
use rusoto_dynamodb::{DynamoDbClient, WriteRequest};
use tokio::time::Instant;

use dynamodb_rust_concurrent::common::{batch_write_item, make_values};

/// ループで join_all
async fn join_all_loop(client: &DynamoDbClient, v: Vec<i32>) -> Result<(), ()> {
    let chunks: Vec<Vec<i32>> = v
        .into_iter()
        .chunks(25)
        .into_iter()
        .map(|v| v.into_iter().collect())
        .collect();

    let mut tasks = vec![];
    for chunk in chunks {
        let cloned_client = client.clone();

        let task = tokio::spawn(async move {
            println!("start: {:?}", std::thread::current().id());

            let values: Vec<WriteRequest> = make_values(chunk);
            let mut items = HashMap::new();
            items.insert("users".to_string(), values);
            batch_write_item(&cloned_client, items).await.unwrap();

            println!("end: {:?}", std::thread::current().id());
        });
        tasks.push(task);
    }

    let ret = futures::future::try_join_all(tasks).await;
    println!("{:?}", ret);

    Ok(())
}

#[tokio::main(flavor = "multi_thread", worker_threads = 10)]
async fn main() {
    let client = DynamoDbClient::new(Region::Custom {
        name: "ap-northeast-1".to_string(),
        endpoint: "http://localhost:4566".to_string(),
    });
    let test_data = 0..1000;

    let start = Instant::now();

    let res = join_all_loop(&client, test_data.collect_vec()).await;

    println!("{:?}", res);

    let duration = start.elapsed();
    println!("elapsed time: {:?}", duration);
}
