use std::{collections::HashMap, mem};

use super::df::{Dataframe, DataframeData};

#[derive(Debug)]
pub enum Json {
    Dataset,
    ListObject,
}

impl Json {
    pub fn to_json(&self, df: Dataframe) -> serde_json::Value {
        let mut res = Vec::new();

        match self {
            Json::Dataset => {
                let head = serde_json::json!(df.column);
                res.push(head);
                let data = df
                    .data
                    .into_iter()
                    .map(|r| serde_json::json!(r.0))
                    .collect();
                res.push(data);
            }
            Json::ListObject => {
                let head = df.column;
                let mut hash_row: HashMap<String, DataframeData> = HashMap::new();
                for r in df.data {
                    for (idx, i) in r.0.into_iter().enumerate() {
                        if let Some(k) = head.get(idx) {
                            hash_row.insert(k.clone(), i);
                        }
                    }
                    let mut cache_hash_row = HashMap::new();
                    mem::swap(&mut cache_hash_row, &mut hash_row);
                    res.push(serde_json::json!(cache_hash_row));
                }
            }
        }

        serde_json::json!(res)
    }
}

#[test]
fn test_to_json() {
    use super::df::DataframeRow;

    let column = Some(vec!["name".to_owned(), "progress".to_owned()]);
    let data = vec![
        DataframeRow(vec![
            DataframeData::String("Jacob".to_owned()),
            DataframeData::Number(100f64),
        ]),
        DataframeRow(vec![
            DataframeData::String("Sam".to_owned()),
            DataframeData::Number(80f64),
        ]),
    ];
    let df = Dataframe::new(column, data);

    let json = Json::ListObject;
    let res = json.to_json(df);

    println!("{:?}", res.to_string());
}
