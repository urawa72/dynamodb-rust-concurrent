use std::collections::HashMap;

use itertools::Itertools;
use rusoto_core::{Region, RusotoError};
use rusoto_dynamodb::{
    AttributeValue, BatchWriteItemError, BatchWriteItemInput, DynamoDb, DynamoDbClient, PutRequest,
    WriteRequest,
};

pub struct TestClient(DynamoDbClient);

impl Default for TestClient {
    fn default() -> Self {
        TestClient(DynamoDbClient::new(Region::Custom {
            name: "ap-northeast-1".to_string(),
            endpoint: "http://localhost:4566".to_string(),
        }))
    }
}

impl TestClient {
    pub fn into_inner(self) -> DynamoDbClient {
        self.0
    }
}

/// テストデータを作成する
/// DynamoDB に保存可能な最大件数で分割しておく
/// BatchWriteItem は 25 件 or 16 MB の制限あり
pub fn make_test_date() -> Vec<Vec<i32>> {
    (0..1000)
        .into_iter()
        .chunks(25)
        .into_iter()
        .map(|v| v.into_iter().collect())
        .collect()
}

/// DynamoDB の BatchWriteItem を実行する
/// 処理できなかった unprocessed_items は無視する
pub async fn batch_write_item(
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

/// DynamoDB に保存するための型に変換する
pub fn make_items(chunk: Vec<i32>) -> HashMap<String, Vec<WriteRequest>> {
    let values: Vec<WriteRequest> = make_values(chunk);
    let mut items = HashMap::new();
    items.insert("users".to_string(), values);
    items
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
