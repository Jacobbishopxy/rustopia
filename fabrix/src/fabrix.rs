//! fabrix dataframe

use std::vec::IntoIter;

use polars::prelude::*;
use sea_query::Value;
use uuid::Uuid;

#[derive(Debug, Clone)]
pub enum Index {
    Int(u32),
    BigInt(u64),
    Uuid(Uuid),
}

impl Into<Value> for Index {
    fn into(self) -> Value {
        match self {
            Index::Int(v) => Value::Int(Some(v as i32)),
            Index::BigInt(v) => Value::BigInt(Some(v as i64)),
            Index::Uuid(v) => Value::Uuid(Some(Box::new(v))),
        }
    }
}

impl Into<Value> for &Index {
    fn into(self) -> Value {
        self.clone().into()
    }
}

/// FValue is a wrapper used for containing Polars AnyValue
pub struct FValue<'a>(AnyValue<'a>);

impl<'a> FValue<'a> {
    pub fn new(v: AnyValue<'a>) -> Self {
        FValue(v)
    }
}

impl<'a> Into<Value> for FValue<'a> {
    fn into(self) -> Value {
        match self.0 {
            AnyValue::Null => Value::Bool(None),
            AnyValue::Boolean(v) => Value::Bool(Some(v)),
            AnyValue::Utf8(v) => Value::String(Some(Box::new(v.to_owned()))),
            AnyValue::UInt8(v) => Value::TinyInt(Some(v as i8)),
            AnyValue::UInt16(v) => Value::SmallInt(Some(v as i16)),
            AnyValue::UInt32(v) => Value::Int(Some(v as i32)),
            AnyValue::UInt64(v) => Value::BigInt(Some(v as i64)),
            AnyValue::Int8(v) => Value::TinyInt(Some(v)),
            AnyValue::Int16(v) => Value::SmallInt(Some(v)),
            AnyValue::Int32(v) => Value::Int(Some(v)),
            AnyValue::Int64(v) => Value::BigInt(Some(v)),
            AnyValue::Float32(v) => Value::Float(Some(v)),
            AnyValue::Float64(v) => Value::Double(Some(v)),
            AnyValue::Date32(v) => todo!(),
            AnyValue::Date64(v) => todo!(),
            AnyValue::Time64(_, _) => todo!(),
            AnyValue::Duration(_, _) => todo!(),
            AnyValue::List(_) => todo!(),
        }
    }
}

/// index type of a dataframe
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
}

/// Accessory trait for Fabrix
pub trait DataFrameAccessory {
    fn get_column_schema(&self) -> Vec<Field>;

    fn row_iter(&self) -> IntoIter<Vec<AnyValue>>;

    fn indices(&self) -> Vec<Index>;
}

impl DataFrameAccessory for DataFrame {
    fn get_column_schema(&self) -> Vec<Field> {
        self.schema().fields().clone()
    }

    fn row_iter(&self) -> IntoIter<Vec<AnyValue>> {
        todo!()
    }

    fn indices(&self) -> Vec<Index> {
        todo!()
    }
}

pub trait SeriesAccessory {
    fn iter(&self) -> IntoIter<AnyValue>;
}

impl SeriesAccessory for Series {
    fn iter(&self) -> IntoIter<AnyValue> {
        todo!()
    }
}
