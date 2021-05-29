use serde::{Deserialize, Serialize};

use crate::QueryResult;

#[derive(Deserialize, Serialize, Debug, Clone, PartialEq)]
pub struct TableSimpleList {
    pub table_name: String,
}

impl QueryResult for TableSimpleList {
    fn json(&self) -> serde_json::value::Value {
        serde_json::json!(self.table_name)
    }
}

impl QueryResult for Vec<TableSimpleList> {
    fn json(&self) -> serde_json::value::Value {
        serde_json::json!(self)
    }
}
