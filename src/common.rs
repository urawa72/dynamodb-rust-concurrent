use std::collections::HashMap;

use rusoto_core::RusotoError;
use rusoto_dynamodb::{
    AttributeValue, BatchWriteItemError, BatchWriteItemInput, DynamoDb, DynamoDbClient, PutRequest,
    WriteRequest,
};

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

pub fn make_values(v: Vec<i32>) -> Vec<WriteRequest> {
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
