use std::collections::BTreeMap;

use crate::prelude::*;

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
                let data: D2 = dataframe.into();
                serde_json::json!(data)
            }
            Json::ListObject => {
                let mut res = Vec::new();

                let head = dataframe.columns_name();
                for r in dataframe.data().into_iter() {
                    let mut hash_row: BTreeMap<&str, _> = BTreeMap::new();
                    for (idx, i) in r.into_iter().enumerate() {
                        if let Some(k) = head.get(idx) {
                            hash_row.insert(k, i);
                        }
                    }
                    res.push(hash_row);
                }
                serde_json::json!(res)
            }
        }
    }
}

#[test]
fn test_to_json() {
    use chrono::NaiveDate;

    use crate::d2;
    use crate::prelude::*;

    let data = d2![
        ["name", "progress", "date"],
        ["Jacob", 100f64, NaiveDate::from_ymd(2000, 1, 1)],
        ["Sam", 80f64, NaiveDate::from_ymd(2000, 5, 1)]
    ];
    let df = Dataframe::from_vec(data, "h");

    let json = Json::Dataset;
    let res = json.to_json(df);

    println!("{:?}", res.to_string());

    let data = d2![
        ["name", "Jacob", "Sam"],
        ["progress", 100f64, 80f64],
        [
            "date",
            NaiveDate::from_ymd(2000, 1, 1),
            NaiveDate::from_ymd(2010, 1, 1)
        ]
    ];

    let df = Dataframe::from_vec(data, "v");

    let json = Json::ListObject;
    let res = json.to_json(df);

    println!("{:?}", res.to_string());
}

#[test]
fn test_to_json_col() {
    use chrono::NaiveDate;

    use crate::df;
    use crate::prelude::*;

    let df = df![
        "h";
        ["name", "progress", "date"],
        ["Jacob", 100f64, NaiveDate::from_ymd(2000, 1, 1)],
        ["Sam", 80f64, NaiveDate::from_ymd(2000, 5, 1)]
    ];

    // println!("{:?}", serde_json::json!(df.data()).to_string());
    // println!("{:?}", serde_json::json!(df.columns()).to_string());

    let json = Json::ListObject;

    println!("{:?}", json.to_json(df).to_string());
}
