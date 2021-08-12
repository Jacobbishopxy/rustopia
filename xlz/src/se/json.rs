// use std::{collections::HashMap};

use tiny_df::{Dataframe, DF};

/// Serialize Dataframe to JSON
#[derive(Debug)]
pub enum Json {
    Dataset,
    ListObject,
}

impl Json {
    pub fn to_json(&self, dataframe: Dataframe) -> serde_json::Value {
        match self {
            Json::Dataset => {
                let data: DF = dataframe.into();
                serde_json::json!(data)
            }
            Json::ListObject => {
                // let head = dataframe.columns().clone();
                // let mut hash_row: HashMap<&str, DataframeData> = HashMap::new();
                // for r in dataframe.data {
                //     for (idx, i) in r.into_iter().enumerate() {
                //         if let Some(k) = head.get(idx) {
                //             hash_row.insert(&k.name, i);
                //         }
                //     }
                //     let mut cache_hash_row = HashMap::new();
                //     mem::swap(&mut cache_hash_row, &mut hash_row);
                //     res.push(serde_json::json!(cache_hash_row));
                // }
                todo!()
            }
        }
    }
}

#[test]
fn test_to_json() {
    use chrono::NaiveDate;

    use tiny_df::{df, DataframeData};

    let data = df![
        ["name", "progress", "date"],
        ["Jacob", 100f64, NaiveDate::from_ymd(2000, 1, 1)],
        ["Sam", 80f64, NaiveDate::from_ymd(2000, 5, 1)]
    ];
    let df = Dataframe::new(data, "h");

    let json = Json::Dataset;
    let res = json.to_json(df);

    println!("{:?}", res.to_string());

    let data = df![
        ["name", "Jacob", "Sam"],
        ["progress", 100f64, 80f64],
        [
            "date",
            NaiveDate::from_ymd(2000, 1, 1),
            NaiveDate::from_ymd(2010, 1, 1)
        ]
    ];

    let df = Dataframe::new(data, "v");

    let json = Json::Dataset;
    let res = json.to_json(df);

    println!("{:?}", res.to_string());
}

#[test]
fn test_to_json_col() {
    use chrono::NaiveDate;

    use tiny_df::{df, DataframeData};

    let data = df![
        ["name", "progress", "date"],
        ["Jacob", 100f64, NaiveDate::from_ymd(2000, 1, 1)],
        ["Sam", 80f64, NaiveDate::from_ymd(2000, 5, 1)]
    ];
    let df = Dataframe::new(data, "h");

    println!("{:?}", serde_json::json!(df.columns()).to_string());
}
