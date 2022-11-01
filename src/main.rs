use std::{collections::HashMap, time::Instant};

use itertools::Itertools;
use rusoto_core::{Region, RusotoError};
use rusoto_dynamodb::{
    AttributeValue, BatchWriteItemError, BatchWriteItemInput, DynamoDb, DynamoDbClient, PutRequest,
    WriteRequest,
};

/// DynamoDB の BatchWriteItem を実行する
/// 処理できなかった unprocessed_items は無視する
async fn batch_write_item(
    client: &DynamoDbClient,
    request_items: HashMap<String, Vec<WriteRequest>>,
) -> Result<(), RusotoError<BatchWriteItemError>> {
    let input = BatchWriteItemInput {
        request_items,
        ..Default::default()
    };

    client.batch_write_item(input).await?;

    Ok(())
}

fn make_values(v: Vec<i32>) -> Vec<WriteRequest> {
    v.into_iter()
        .map(|i| {
            let mut item = HashMap::new();
            item.insert(
                "id".to_string(),
                AttributeValue {
                    n: Some(i.to_string()),
                    ..Default::default()
                },
            );
            WriteRequest {
                put_request: Some(PutRequest { item }),
                ..Default::default()
            }
        })
        .collect()
}

/// ループで直列
async fn serial_loop(client: &DynamoDbClient, v: Vec<i32>) -> Result<String, ()> {
    let mut res = String::new();

    let chunks: Vec<Vec<i32>> = v
        .into_iter()
        .chunks(25)
        .into_iter()
        .map(|v| v.into_iter().collect())
        .collect();

    for chunk in chunks {
        let cloned_client = client.clone();

        tokio::spawn(async move {
            println!("start: {:?}", std::thread::current().id());

            let values: Vec<WriteRequest> = make_values(chunk);
            let mut items = HashMap::new();
            items.insert("users".to_string(), values);
            batch_write_item(&cloned_client, items).await.unwrap();

            println!("end: {:?}", std::thread::current().id());
        })
        .await
        .unwrap();
        res = format!("{}:{}", res, "done!");
    }

    Ok(res)
}

/// stream で直列
#[allow(clippy::extra_unused_lifetimes)]
fn serial_stream<'a>(
    client: &'_ DynamoDbClient,
    v: Vec<i32>,
) -> impl futures::Future<Output = Result<String, ()>> + '_ {
    use futures::{StreamExt, TryStreamExt};

    let chunks: Vec<Vec<i32>> = v
        .into_iter()
        .chunks(25)
        .into_iter()
        .map(|v| v.into_iter().collect())
        .collect();

    futures::stream::iter(chunks)
        .map(|chunk| {
            let cloned_client = client.clone();

            tokio::spawn(async move {
                println!("start: {:?}", std::thread::current().id());

                let values: Vec<WriteRequest> = make_values(chunk);
                let mut items = HashMap::new();
                items.insert("users".to_string(), values);
                batch_write_item(&cloned_client, items).await.unwrap();

                println!("end: {:?}", std::thread::current().id());

                Ok(())
            })
        })
        .then(|x| async move { x.await.map_err(|_| ())? })
        .try_fold(String::new(), |acc, _x| async move {
            Ok(format!("{}:{}", acc, "done!"))
        })
}

/// stream で並列
#[allow(clippy::extra_unused_lifetimes)]
fn concurrent_stream<'a>(
    client: &'_ DynamoDbClient,
    v: Vec<i32>,
) -> impl futures::Future<Output = Result<String, ()>> + '_ {
    use futures::{StreamExt, TryStreamExt};

    let chunks: Vec<Vec<i32>> = v
        .into_iter()
        .chunks(25)
        .into_iter()
        .map(|v| v.into_iter().collect())
        .collect();

    futures::stream::iter(chunks)
        .map(|chunk| {
            let cloned_client = client.clone();

            tokio::spawn(async move {
                println!("start: {:?}", std::thread::current().id());

                let values: Vec<WriteRequest> = make_values(chunk);
                let mut items = HashMap::new();
                items.insert("users".to_string(), values);
                batch_write_item(&cloned_client, items).await.unwrap();

                println!("end: {:?}", std::thread::current().id());

                Ok(())
            })
        })
        .buffered(10)
        .map(|x| x.map_err(|_| ())?)
        .try_fold(String::new(), |acc, _x| async move {
            Ok(format!("{}:{}", acc, "done!"))
        })
}

#[tokio::main]
async fn main() {
    let client = DynamoDbClient::new(Region::Custom {
        name: "ap-northeast-1".to_string(),
        endpoint: "http://localhost:4566".to_string(),
    });
    let test_data = 0..1000;

    let start = Instant::now();

    // let res = serial_loop(&client, test_data.collect_vec()).await;
    // let res = serial_stream(&client, test_data.collect_vec()).await;
    let res = concurrent_stream(&client, test_data.collect_vec()).await;

    println!("{:?}", res);

    let duration = start.elapsed();
    println!("duration: {:?}", duration);
}
