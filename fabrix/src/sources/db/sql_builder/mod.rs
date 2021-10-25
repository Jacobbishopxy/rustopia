//! Fabrix Database SQL builder

pub mod builder;
pub mod ddl;
pub mod dml;
pub mod interface;
pub(crate) mod macros;

pub(crate) use builder::*;
pub(crate) use macros::statement;

use polars::prelude::{DataType, Field};

use crate::{FabrixError, FabrixResult, Series};

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

pub enum SaveStrategy {
    Replace,
    Append,
    Upsert,
    Fail,
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
