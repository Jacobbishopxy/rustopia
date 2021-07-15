use std::collections::HashMap;

use serde::{Deserialize, Serialize};

pub type JsonType = serde_json::value::Value;

#[derive(Deserialize, Serialize)]
pub struct DbQueryResult {
    pub rows_affected: u64,
    pub last_insert_id: Option<u64>,
}

pub trait QueryResult {
    fn json(&self) -> JsonType;
}

impl QueryResult for (String, String) {
    fn json(&self) -> JsonType {
        serde_json::json!(self)
    }
}

impl QueryResult for Vec<(String, String)> {
    fn json(&self) -> JsonType {
        serde_json::json!(self)
    }
}

impl QueryResult for Vec<String> {
    fn json(&self) -> JsonType {
        serde_json::json!(self)
    }
}

impl QueryResult for Vec<HashMap<String, DataEnum>> {
    fn json(&self) -> JsonType {
        serde_json::json!(self)
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
#[serde(untagged)]
pub enum DataEnum {
    Integer(i64),
    Float(f64),
    String(String),
    Bool(bool),
    Null,
}

impl QueryResult for DataEnum {
    fn json(&self) -> JsonType {
        serde_json::json!(self)
    }
}

impl From<String> for DataEnum {
    fn from(v: String) -> Self {
        DataEnum::String(v)
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct TabulateTable {
    pub columns: Vec<String>,
    pub data: Vec<DataEnum>,
}

impl QueryResult for TabulateTable {
    fn json(&self) -> JsonType {
        serde_json::json!(self)
    }
}

impl QueryResult for DbQueryResult {
    fn json(&self) -> JsonType {
        serde_json::json!(self)
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct TabulateRow(pub HashMap<String, DataEnum>);

impl QueryResult for TabulateRow {
    fn json(&self) -> JsonType {
        serde_json::json!(self)
    }
}

impl QueryResult for Vec<TabulateRow> {
    fn json(&self) -> JsonType {
        serde_json::json!(self)
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct Tabulate(pub Vec<TabulateRow>);

impl QueryResult for Tabulate {
    fn json(&self) -> JsonType {
        serde_json::json!(self)
    }
}
