//! fabrix value

use polars::prelude::{AnyValue, DataType};
use sea_query::Value as SQValue;

/// FValue is a wrapper used for holding Polars AnyValue in order to
/// satisfy type conversion between `sea_query::Value`
#[derive(Debug, PartialEq, Clone)]
pub struct Value<'a>(AnyValue<'a>);

impl<'a> Value<'a> {
    pub fn new(v: AnyValue<'a>) -> Self {
        Value(v)
    }
}

/// Type conversion: from `polars` AnyValue to Value
impl<'a> From<AnyValue<'a>> for Value<'a> {
    fn from(val: AnyValue<'a>) -> Self {
        Value(val)
    }
}

/// Type conversion: from Value to `sea-query` Value
impl<'a> From<Value<'a>> for SQValue {
    fn from(val: Value<'a>) -> Self {
        match val.0 {
            AnyValue::Null => SQValue::Bool(None),
            AnyValue::Boolean(v) => SQValue::Bool(Some(v)),
            AnyValue::Utf8(v) => SQValue::String(Some(Box::new(v.to_owned()))),
            AnyValue::UInt8(v) => SQValue::TinyInt(Some(v as i8)),
            AnyValue::UInt16(v) => SQValue::SmallInt(Some(v as i16)),
            AnyValue::UInt32(v) => SQValue::Int(Some(v as i32)),
            AnyValue::UInt64(v) => SQValue::BigInt(Some(v as i64)),
            AnyValue::Int8(v) => SQValue::TinyInt(Some(v)),
            AnyValue::Int16(v) => SQValue::SmallInt(Some(v)),
            AnyValue::Int32(v) => SQValue::Int(Some(v)),
            AnyValue::Int64(v) => SQValue::BigInt(Some(v)),
            AnyValue::Float32(v) => SQValue::Float(Some(v)),
            AnyValue::Float64(v) => SQValue::Double(Some(v)),
            AnyValue::Date32(_) => todo!(),
            AnyValue::Date64(_) => todo!(),
            AnyValue::Time64(_, _) => todo!(),
            AnyValue::Duration(_, _) => todo!(),
            AnyValue::List(_) => todo!(),
        }
    }
}

/// Type conversion: from `polars` AnyValue (wrapped by FValue) to `polars` DataType
impl<'a> From<Value<'a>> for DataType {
    fn from(val: Value<'a>) -> Self {
        match val.0 {
            AnyValue::Null => DataType::Null,
            AnyValue::Boolean(_) => DataType::Boolean,
            AnyValue::Utf8(_) => DataType::Utf8,
            AnyValue::UInt8(_) => DataType::Utf8,
            AnyValue::UInt16(_) => DataType::UInt16,
            AnyValue::UInt32(_) => DataType::UInt32,
            AnyValue::UInt64(_) => DataType::UInt64,
            AnyValue::Int8(_) => DataType::Int8,
            AnyValue::Int16(_) => DataType::Int16,
            AnyValue::Int32(_) => DataType::Int32,
            AnyValue::Int64(_) => DataType::Int64,
            AnyValue::Float32(_) => DataType::Float32,
            AnyValue::Float64(_) => DataType::Float64,
            AnyValue::Date32(_) => DataType::Date32,
            AnyValue::Date64(_) => DataType::Date64,
            AnyValue::Time64(_, tu) => DataType::Time64(tu),
            AnyValue::Duration(_, tu) => DataType::Duration(tu),
            AnyValue::List(_) => unimplemented!(),
        }
    }
}

impl<'a> PartialEq<Value<'a>> for &Value<'a> {
    fn eq(&self, other: &Value<'a>) -> bool {
        self.0 == other.0
    }
}
