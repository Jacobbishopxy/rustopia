//! Fabrix SqlBuilder ADT

use itertools::Itertools;
use polars::prelude::{DataType, Field};
use serde::{Deserialize, Serialize};

use crate::{FabrixError, FabrixResult, Series, Value};

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

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]

pub struct NameAlias {
    pub from: String,
    pub to: String,
}

/// column name, can be alias. used it in `select`
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
#[serde(untagged)]
pub enum ColumnAlias {
    Simple(String),
    Alias(NameAlias),
}

impl ColumnAlias {
    pub fn original_name(&self) -> String {
        match self {
            ColumnAlias::Simple(s) => s.to_owned(),
            ColumnAlias::Alias(s) => s.from.to_owned(),
        }
    }

    pub fn name(&self) -> String {
        match self {
            ColumnAlias::Simple(s) => s.to_owned(),
            ColumnAlias::Alias(s) => s.to.to_owned(),
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

impl Select {
    pub fn columns_name(&self, alias: bool) -> Vec<String> {
        self.columns
            .iter()
            .map(|c| if alias { c.name() } else { c.original_name() })
            .collect_vec()
    }
}

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

impl Expression {
    pub fn builder() -> Expression {
        todo!()
    }
}

#[derive(Deserialize, Serialize, Debug, Clone, PartialEq)]
pub enum SaveStrategy {
    Replace,
    Append,
    Upsert,
    Fail,
}

/// index type is used for defining Sql column type
pub enum IndexType {
    Int,
    BigInt,
    Uuid,
}

impl From<&str> for IndexType {
    fn from(v: &str) -> Self {
        match &v.to_lowercase()[..] {
            "int" | "i" => IndexType::Int,
            "bigint" | "b" => IndexType::BigInt,
            "uuid" | "u" => IndexType::Uuid,
            _ => IndexType::Int,
        }
    }
}

/// index option
pub struct IndexOption<'a> {
    pub name: &'a str,
    pub index_type: IndexType,
}

impl<'a> IndexOption<'a> {
    pub fn new<T>(name: &'a str, index_type: T) -> Self
    where
        T: Into<IndexType>,
    {
        let index_type: IndexType = index_type.into();
        IndexOption { name, index_type }
    }

    pub fn try_from_series(series: &'a Series) -> FabrixResult<Self> {
        let dtype = series.dtype();
        let index_type = match dtype {
            DataType::UInt8 => Ok(IndexType::Int),
            DataType::UInt16 => Ok(IndexType::Int),
            DataType::UInt32 => Ok(IndexType::Int),
            DataType::UInt64 => Ok(IndexType::BigInt),
            DataType::Int8 => Ok(IndexType::Int),
            DataType::Int16 => Ok(IndexType::Int),
            DataType::Int32 => Ok(IndexType::Int),
            DataType::Int64 => Ok(IndexType::BigInt),
            DataType::Utf8 => Ok(IndexType::Uuid), // TODO: saving uuid as String?
            _ => Err(FabrixError::new_common_error(format!(
                "{:?} cannot convert to index type",
                dtype
            ))),
        }?;

        Ok(IndexOption {
            name: series.name(),
            index_type,
        })
    }
}

/// table field: column name, column type & is nullable
pub struct TableField {
    pub(crate) field: Field,
    pub(crate) nullable: bool,
}

impl TableField {
    pub fn new(field: Field, nullable: bool) -> Self {
        TableField { field, nullable }
    }

    pub fn field(&self) -> &Field {
        &self.field
    }

    pub fn name(&self) -> &String {
        &self.field.name()
    }

    pub fn data_type(&self) -> &DataType {
        &self.field.data_type()
    }

    pub fn nullable(&self) -> bool {
        self.nullable
    }
}

impl From<Field> for TableField {
    fn from(f: Field) -> Self {
        TableField::new(f, true)
    }
}

#[cfg(test)]
mod tests_common {
    //
}
