use std::{collections::HashMap, time::Instant};

use itertools::Itertools;
use rusoto_core::Region;
use rusoto_dynamodb::{
    AttributeValue, BatchWriteItemInput, DynamoDb, DynamoDbClient, PutRequest, WriteRequest,
};

async fn batch_write_item(
    client: &DynamoDbClient,
    request_items: HashMap<String, Vec<WriteRequest>>,
) {
    let input = BatchWriteItemInput {
        request_items,
        ..Default::default()
    };

    let _ = client.batch_write_item(input).await;
}

async fn serial(client: &DynamoDbClient) -> Result<(), ()> {
    for chunk in (0..200).into_iter().chunks(25).into_iter() {
        let cloned_client = client.clone();
        let chunk: Vec<i32> = chunk.into_iter().collect();

        let r = tokio::spawn(async move {
            println!("start: {:?}", std::thread::current().id());

            let values: Vec<WriteRequest> = chunk
                .into_iter()
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
                .collect();
            let mut items = HashMap::new();
            items.insert("users".to_string(), values);
            batch_write_item(&cloned_client, items).await;

            println!("end: {:?}", std::thread::current().id());
        })
        .await;
        println!("{:?}", r);
    }

    Ok(())
}

#[tokio::main]
async fn main() {
    let client = DynamoDbClient::new(Region::Custom {
        name: "ap-northeast-1".to_string(),
        endpoint: "http://localhost:4566".to_string(),
    });

    let start = Instant::now();

    let _ = serial(&client).await;

    let duration = start.elapsed();
    println!("duration: {:?}", duration);
}
