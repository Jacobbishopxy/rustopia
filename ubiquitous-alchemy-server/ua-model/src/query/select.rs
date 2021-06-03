use serde::{Deserialize, Serialize};

use crate::common::QueryResult;

#[derive(Deserialize, Serialize, Debug, Clone, PartialEq)]
pub struct Select {
    pub table: String,
    pub columns: Vec<String>,
}

#[derive(Deserialize, Serialize, Debug, Clone, PartialEq)]
pub struct SelectResult(pub serde_json::value::Value);

impl QueryResult for SelectResult {
    fn json(&self) -> serde_json::value::Value {
        serde_json::json!(self.0)
    }
}

#[derive(Deserialize, Serialize, Debug, Clone, PartialEq)]
pub struct SelectVecResult(pub Vec<SelectResult>);

impl QueryResult for SelectVecResult {
    fn json(&self) -> serde_json::value::Value {
        serde_json::json!(self.0)
    }
}
