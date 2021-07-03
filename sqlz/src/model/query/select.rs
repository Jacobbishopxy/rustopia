use serde::{Deserialize, Serialize};

#[derive(Deserialize, Serialize, Debug, Clone, PartialEq)]
pub struct Select {
    pub table: String,
    pub columns: Vec<String>,
}

#[derive(Deserialize, Serialize, Debug, Clone, PartialEq)]
pub struct SelectResult(pub serde_json::value::Value);

#[derive(Deserialize, Serialize, Debug, Clone, PartialEq)]
pub struct SelectVecResult(pub Vec<SelectResult>);
