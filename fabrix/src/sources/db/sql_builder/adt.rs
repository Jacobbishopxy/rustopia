//! Fabrix SqlBuilder ADT

use serde::{Deserialize, Serialize};

use crate::Value;

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

/// order type
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub enum OrderType {
    Asc,
    Desc,
}

/// an order contains a column name and it's order type
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Default)]
pub struct Order {
    pub name: String,
    pub order: Option<OrderType>,
}

/// index with its' unique name, table belonged, and related index/ indices
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct Index {
    pub name: String,
    pub table: String,
    pub columns: Vec<Order>,
}

/// foreign key direction
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct ForeignKeyDir {
    pub table: String,
    pub column: String,
}

/// foreign key action
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

/// column name, can be alias. used it in `select`
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
#[serde(untagged)]
pub enum ColumnAlias {
    Simple(String),
    Alias((String, String)),
}

impl ColumnAlias {
    pub fn name(&self) -> String {
        match self {
            ColumnAlias::Simple(s) => s.to_owned(),
            ColumnAlias::Alias((s, _)) => s.to_owned(),
        }
    }
}

#[derive(Deserialize, Serialize, Debug, Clone, PartialEq)]
pub struct Select {
    pub table: String,
    pub columns: Vec<ColumnAlias>,
    pub filter: Option<Vec<Expression>>,
    pub order: Option<Vec<Order>>,
    pub limit: Option<u64>,
    pub offset: Option<u64>,
}

#[derive(Deserialize, Serialize, Debug, Clone, PartialEq)]
pub struct SelectResult(pub serde_json::value::Value);

#[derive(Deserialize, Serialize, Debug, Clone, PartialEq)]
pub struct SelectVecResult(pub Vec<SelectResult>);

#[derive(Deserialize, Serialize, Debug, Clone, PartialEq)]
pub enum Conjunction {
    AND,
    OR,
}

#[derive(Deserialize, Serialize, Debug, Clone, PartialEq)]
pub enum Equation {
    Equal(Value),
    NotEqual(Value),
    Greater(Value),
    GreaterEqual(Value),
    Less(Value),
    LessEqual(Value),
    In(Vec<Value>),
    Between((Value, Value)),
    Like(String),
}

#[derive(Deserialize, Serialize, Debug, Clone, PartialEq)]
pub struct Condition {
    pub column: String,
    pub equation: Equation,
}

#[derive(Deserialize, Serialize, Debug, Clone, PartialEq)]
#[serde(untagged)]
pub enum Expression {
    Conjunction(Conjunction),
    Simple(Condition),
    Nest(Vec<Expression>),
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
            ..Default::default()
        };

        let serialized = serde_json::to_string(&table).unwrap();

        let deserialized: Table = serde_json::from_str(&serialized).unwrap();

        assert_eq!(deserialized, table);
    }
}
