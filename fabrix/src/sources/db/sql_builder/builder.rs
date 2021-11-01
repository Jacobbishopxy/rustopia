//! Sql builder

use polars::prelude::DataType;
use sea_query::Value as SValue;

use crate::{Date, DateTime, Decimal, FabrixError, FabrixResult, Time, Uuid, Value};

#[derive(Debug, Clone)]
pub enum SqlBuilder {
    Mysql,
    Postgres,
    Sqlite,
}

impl std::fmt::Display for SqlBuilder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Mysql => write!(f, "mysql"),
            Self::Postgres => write!(f, "postgres"),
            Self::Sqlite => write!(f, "sqlite"),
        }
    }
}

impl From<&str> for SqlBuilder {
    fn from(v: &str) -> Self {
        match &v.to_lowercase()[..] {
            "mysql" | "m" => SqlBuilder::Mysql,
            "postgres" | "p" => SqlBuilder::Postgres,
            _ => SqlBuilder::Sqlite,
        }
    }
}

/// Don't use it in general type conversion, use `try_from_value_to_svalue` instead
impl From<Value> for SValue {
    fn from(v: Value) -> Self {
        match v {
            Value::Bool(v) => SValue::Bool(Some(v)),
            Value::U8(v) => SValue::TinyUnsigned(Some(v)),
            Value::U16(v) => SValue::SmallUnsigned(Some(v)),
            Value::U32(v) => SValue::Unsigned(Some(v)),
            Value::U64(v) => SValue::BigUnsigned(Some(v)),
            Value::I8(v) => SValue::TinyInt(Some(v)),
            Value::I16(v) => SValue::SmallInt(Some(v)),
            Value::I32(v) => SValue::Int(Some(v)),
            Value::I64(v) => SValue::BigInt(Some(v)),
            Value::F32(v) => SValue::Float(Some(v)),
            Value::F64(v) => SValue::Double(Some(v)),
            Value::String(v) => SValue::String(Some(Box::new(v))),
            Value::Date(v) => SValue::Date(Some(Box::new(v.0))),
            Value::Time(v) => SValue::Time(Some(Box::new(v.0))),
            Value::DateTime(v) => SValue::DateTime(Some(Box::new(v.0))),
            Value::Decimal(v) => SValue::Decimal(Some(Box::new(v.0))),
            Value::Uuid(v) => SValue::Uuid(Some(Box::new(v.0))),
            // Temporary workaround
            Value::Null => SValue::Bool(None),
        }
    }
}

impl From<&Value> for SValue {
    fn from(v: &Value) -> Self {
        v.clone().into()
    }
}

/// Type conversion: from polars DataType to SeqQuery Value
fn from_data_type_to_null_svalue(dtype: &DataType) -> SValue {
    match dtype {
        DataType::Boolean => SValue::Bool(None),
        DataType::UInt8 => SValue::TinyUnsigned(None),
        DataType::UInt16 => SValue::SmallUnsigned(None),
        DataType::UInt32 => SValue::Unsigned(None),
        DataType::UInt64 => SValue::BigUnsigned(None),
        DataType::Int8 => SValue::TinyInt(None),
        DataType::Int16 => SValue::SmallInt(None),
        DataType::Int32 => SValue::Int(None),
        DataType::Int64 => SValue::BigInt(None),
        DataType::Float32 => SValue::Float(None),
        DataType::Float64 => SValue::Double(None),
        DataType::Utf8 => SValue::String(None),
        DataType::Object("Date") => SValue::Date(None),
        DataType::Object("Time") => SValue::Time(None),
        DataType::Object("DateTime") => SValue::DateTime(None),
        DataType::Object("Decimal") => SValue::Decimal(None),
        DataType::Object("Uuid") => SValue::Uuid(None),
        DataType::Object(_) => todo!(),
        _ => panic!("unsupported data type conversion"),
    }
}

/// Type conversion: from Value to `sea-query` Value
pub(crate) fn try_from_value_to_svalue(
    value: Value,
    dtype: &DataType,
    nullable: bool,
) -> FabrixResult<SValue> {
    match value {
        Value::Bool(v) => Ok(SValue::Bool(Some(v))),
        Value::U8(v) => Ok(SValue::TinyUnsigned(Some(v))),
        Value::U16(v) => Ok(SValue::SmallUnsigned(Some(v))),
        Value::U32(v) => Ok(SValue::Unsigned(Some(v))),
        Value::U64(v) => Ok(SValue::BigUnsigned(Some(v))),
        Value::I8(v) => Ok(SValue::TinyInt(Some(v))),
        Value::I16(v) => Ok(SValue::SmallInt(Some(v))),
        Value::I32(v) => Ok(SValue::Int(Some(v))),
        Value::I64(v) => Ok(SValue::BigInt(Some(v))),
        Value::F32(v) => Ok(SValue::Float(Some(v))),
        Value::F64(v) => Ok(SValue::Double(Some(v))),
        Value::String(v) => Ok(SValue::String(Some(Box::new(v)))),
        Value::Date(_) => todo!(),
        Value::Time(_) => todo!(),
        Value::DateTime(_) => todo!(),
        Value::Decimal(v) => Ok(SValue::Decimal(Some(Box::new(v.0)))),
        Value::Uuid(v) => Ok(SValue::Uuid(Some(Box::new(v.0)))),
        Value::Null => {
            if nullable {
                Ok(from_data_type_to_null_svalue(dtype))
            } else {
                Err(FabrixError::new_parse_error(value, dtype))
            }
        }
    }
}

/// from `SeaQuery` Value to Value
macro_rules! sv_2_v {
    ($option_value:expr, $nullable:ident) => {
        if $nullable {
            Ok($crate::value!($option_value))
        } else {
            match $option_value {
                Some(v) => Ok($crate::value!(v)),
                None => Err($crate::FabrixError::new_common_error("unsupported type")),
            }
        }
    };
    ($option_value:expr, $null_type:ty, $nullable:ident) => {
        if $nullable {
            match $option_value {
                Some(v) => Ok($crate::value!(*v)),
                None => Ok($crate::value!(None::<$null_type>)),
            }
        } else {
            match $option_value {
                Some(v) => Ok($crate::value!(*v)),
                None => Err($crate::FabrixError::new_common_error("unsupported type")),
            }
        }
    };
}

/// Type conversion: from `SeaQuery` Value to Value
pub(crate) fn from_svalue_to_value(svalue: SValue, nullable: bool) -> FabrixResult<Value> {
    match svalue {
        SValue::Bool(ov) => sv_2_v!(ov, nullable),
        SValue::TinyInt(ov) => sv_2_v!(ov, nullable),
        SValue::SmallInt(ov) => sv_2_v!(ov, nullable),
        SValue::Int(ov) => sv_2_v!(ov, nullable),
        SValue::BigInt(ov) => sv_2_v!(ov, nullable),
        SValue::TinyUnsigned(ov) => sv_2_v!(ov, nullable),
        SValue::SmallUnsigned(ov) => sv_2_v!(ov, nullable),
        SValue::Unsigned(ov) => sv_2_v!(ov, nullable),
        SValue::BigUnsigned(ov) => sv_2_v!(ov, nullable),
        SValue::Float(ov) => sv_2_v!(ov, nullable),
        SValue::Double(ov) => sv_2_v!(ov, nullable),
        SValue::String(ov) => sv_2_v!(ov, String, nullable),
        SValue::Date(ov) => sv_2_v!(ov, Date, nullable),
        SValue::Time(ov) => sv_2_v!(ov, Time, nullable),
        SValue::DateTime(ov) => sv_2_v!(ov, DateTime, nullable),
        SValue::Decimal(ov) => sv_2_v!(ov, Decimal, nullable),
        SValue::Uuid(ov) => sv_2_v!(ov, Uuid, nullable),
        _ => Err(FabrixError::new_common_error("unsupported type")),
    }
}
