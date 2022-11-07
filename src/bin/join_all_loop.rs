use rusoto_dynamodb::DynamoDbClient;
use tokio::time::Instant;

use dynamodb_rust_concurrent::common::{batch_write_item, make_items, make_test_date, TestClient};

/// tokio::taks をループで生成し join_all で一括で実行する
async fn join_all_loop(client: &DynamoDbClient, chunks: Vec<Vec<i32>>) -> Result<(), ()> {
    let mut tasks = vec![];
    for chunk in chunks {
        let cloned_client = client.clone();

        let task = tokio::spawn(async move {
            println!("start: {:?}", std::thread::current().id());

            let items = make_items(chunk);
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
    let client = TestClient::default().into_inner();

    let test_data = make_test_date();

    let start = Instant::now();

    let res = join_all_loop(&client, test_data).await;

    println!("{:?}", res);

    let duration = start.elapsed();
    println!("elapsed time: {:?}", duration);
}
