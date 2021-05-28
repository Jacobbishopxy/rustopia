use serde::{Deserialize, Serialize};

use crate::QueryResult;
#[derive(Deserialize, Serialize, Debug, Clone, PartialEq)]
pub struct TableSimpleList {
    pub name: String,
}

impl QueryResult for TableSimpleList {}

impl QueryResult for Vec<TableSimpleList> {}
