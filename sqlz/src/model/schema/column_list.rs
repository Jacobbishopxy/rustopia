use serde::{Deserialize, Serialize};

#[derive(Deserialize, Serialize, Debug, Clone, PartialEq)]
pub struct ColumnSimpleList {
    pub column_name: String,
    pub data_type: String,
}
