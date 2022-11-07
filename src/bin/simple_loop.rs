use rusoto_dynamodb::DynamoDbClient;
use tokio::time::Instant;

use dynamodb_rust_concurrent::common::{batch_write_item, make_items, make_test_date, TestClient};

/// tokio::task 使わずループで 1 chunk ずつ処理する
async fn simple_loop(client: &DynamoDbClient, chunks: Vec<Vec<i32>>) -> Result<(), ()> {
    for chunk in chunks {
        println!("start: {:?}", std::thread::current().id());

        let items = make_items(chunk);
        batch_write_item(client, items).await.unwrap();

        println!("end: {:?}", std::thread::current().id());
    }

    Ok(())
}

#[tokio::main(flavor = "multi_thread", worker_threads = 10)]
async fn main() {
    let client = TestClient::default().into_inner();

    let test_data = make_test_date();

    let start = Instant::now();

    let res = simple_loop(&client, test_data).await;

    println!("{:?}", res);

    let duration = start.elapsed();
    println!("elapsed time: {:?}", duration);
}
