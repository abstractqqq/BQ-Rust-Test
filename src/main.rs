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
    let mut rs = client
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

    // if result is empty, then stop or send warning.
    
    
    println!("Number of rows: {}", rs.row_count());
    println!("Column names are: {:?}", rs.column_names());

    let mut close_prices:Vec<f64> = Vec::with_capacity(rs.row_count());
    let mut dates:Vec<String> = Vec::with_capacity(rs.row_count());
    let close_idx = *rs.column_index("Open").unwrap();
    let date_idx = *rs.column_index("Date").unwrap();
    let mut counter = 0;
    while rs.next_row() {
        
        let mut price:f64 = -1.0;
        match rs.get_f64(close_idx) {
            Ok(v) => {
                match v {
                    Some(v) => price = v,
                    _ => println!("Null value detected in returned dataset at row: {}", counter)
                }
            },
            Err(v) => {
                println!("Error encountered when executing BQ Query. Details: {}", v);
            }
        }

        counter += 1;
        close_prices.push(price);
        // Too lazy to use match to deal with errors...
        dates.push(rs.get_string(date_idx).expect("Erron when reading date").unwrap());
    };

    // Find max price in existing history. close_prices should not be empty.
    let (max_index, max_price) = 
        close_prices.iter().enumerate().fold(
            (usize::MIN, f64::MIN),
            |(i_a, a), (i_b, &b)| {
                if b > a {
                    (i_b, b)
                } else {
                    (i_a, a)
                }
            }
        );

    println!("The max price of Tesla in this dataset is {}", max_price);
    println!("Which happened on date {}", dates[max_index]);

}