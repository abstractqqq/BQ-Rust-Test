use gcp_bigquery_client::model::query_request::QueryRequest;
use std::env;
use std::time::Instant;
// use serde_json::{Value};


#[tokio::main]
async fn main(){
    println!("Query started.");
    let now = Instant::now();
    let query_result = get_data_from_bq();
    query_result.await;
    let elapsed = now.elapsed();
    println!("Time spent: {:.2?}", elapsed);
}

async fn get_data_from_bq(){
    // The environment var returns the path to the key json file
    let env_path = env::var("GOOGLE_APPLICATION_CREDENTIALS").unwrap();

    let project_id = "plucky-handler-325914".to_string();
    let dataset_id = "TeslaStonks".to_string();
    let table_id = "TeslaStonksDaily".to_string();

    let client = gcp_bigquery_client::Client::from_service_account_key_file(&env_path).await;

    // Query
    let rs = client
        .job()
        .query(&project_id,
            QueryRequest {
                query:format!(
                    "SELECT * FROM `{}.{}.{}`",
                    project_id, dataset_id, table_id
                ),
                location : Some("us-east1".to_string()),
                ..Default::default()
            }
        )
        .await.unwrap();

    println!("Number of rows: {}", rs.row_count())

}