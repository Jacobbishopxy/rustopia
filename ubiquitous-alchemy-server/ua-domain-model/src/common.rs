use serde::{Deserialize, Serialize};

/// column key type
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

/// column type, variant can have specific size, e.g.: Int(i32)
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

/// a column mainly contains four arguments
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Default)]
pub struct Column {
    pub name: String,
    pub col_type: ColumnType,
    pub null: Option<bool>,
    pub key: Option<ColumnKey>,
}

/// table with its' name, columns and optional foreign key
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Default)]
pub struct Table {
    pub name: String,
    pub columns: Vec<Column>,
    pub foreign_key: Option<ForeignKey>,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub enum Order {
    Asc,
    Desc,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Default)]
pub struct IndexCol {
    pub name: String,
    pub order: Option<Order>,
}

/// index with its' unique name, table belonged, and related index/ indices
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct Index {
    pub name: String,
    pub table: String,
    pub columns: Vec<IndexCol>,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct ForeignKeyDir {
    pub table: String,
    pub column: String,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub enum ForeignKeyAction {
    Restrict,
    Cascade,
    SetNull,
    NoAction,
    SetDefault,
}

impl Default for ForeignKeyAction {
    fn default() -> Self {
        ForeignKeyAction::NoAction
    }
}

/// foreign key with its' unique name, from & to table relations, and actions
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct ForeignKey {
    pub name: String,
    pub from: ForeignKeyDir,
    pub to: ForeignKeyDir,
    pub on_delete: ForeignKeyAction,
    pub on_update: ForeignKeyAction,
}

/// schema indicates a database's tables (not in use)
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct Schema {
    pub schema: String,
    pub tables: Vec<Table>,
}

/// all the return value's type should implement this trait

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
            ..Default::default()
        };

        let serialized = serde_json::to_string(&table).unwrap();

        let deserialized: Table = serde_json::from_str(&serialized).unwrap();

        assert_eq!(deserialized, table);
    }
}
