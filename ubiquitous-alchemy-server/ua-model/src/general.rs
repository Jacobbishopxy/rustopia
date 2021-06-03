use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use crate::common::QueryResult;

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub enum DataEnum {
    Integer(i64),
    Float(f64),
    String(String),
    Bool(bool),
    Null,
}

impl QueryResult for DataEnum {
    fn json(&self) -> serde_json::value::Value {
        serde_json::json!(self)
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct TabulateTable {
    pub columns: Vec<String>,
    pub data: Vec<DataEnum>,
}

impl QueryResult for TabulateTable {
    fn json(&self) -> serde_json::value::Value {
        serde_json::json!(self)
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct TabulateRow(pub HashMap<String, DataEnum>);

impl QueryResult for TabulateRow {
    fn json(&self) -> serde_json::value::Value {
        serde_json::json!(self)
    }
}

impl QueryResult for Vec<TabulateRow> {
    fn json(&self) -> serde_json::value::Value {
        serde_json::json!(self)
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct Tabulate(pub Vec<TabulateRow>);

impl QueryResult for Tabulate {
    fn json(&self) -> serde_json::value::Value {
        serde_json::json!(self)
    }
}
