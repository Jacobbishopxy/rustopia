use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub enum ColumnKey {
    NotKey,
    Primary,
    Unique,
    Multiple,
}

impl Default for ColumnKey {
    fn default() -> Self {
        ColumnKey::NotKey
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct ColumnExtra {
    pub uuid: bool,
}

impl Default for ColumnExtra {
    fn default() -> Self {
        ColumnExtra { uuid: false }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub enum ColumnType {
    Bool,
    Int,
    Float,
    Double,
    Date,
    Time,
    DateTime,
    Timestamp,
    Char,
    VarChar,
    Text,
    Json,
    Binary,
}

impl Default for ColumnType {
    fn default() -> Self {
        ColumnType::VarChar
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Default)]
pub struct Column {
    pub name: String,
    pub col_type: ColumnType,
    pub null: Option<bool>,
    pub key: Option<ColumnKey>,
    // pub extra: SColumnExtra,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct Table {
    pub name: String,
    pub columns: Vec<Column>,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct Schema {
    pub schema: String,
    pub tables: Vec<Table>,
}

#[cfg(test)]
mod tests_common {

    use super::*;

    #[test]
    fn convert() {
        let table = Table {
            name: "test".to_string(),
            columns: vec![
                Column {
                    name: "id".to_string(),
                    key: Some(ColumnKey::Primary),
                    ..Default::default()
                },
                Column {
                    name: "name".to_string(),
                    ..Default::default()
                },
            ],
        };

        let serialized = serde_json::to_string(&table).unwrap();

        let deserialized: Table = serde_json::from_str(&serialized).unwrap();

        assert_eq!(deserialized, table);
    }
}
