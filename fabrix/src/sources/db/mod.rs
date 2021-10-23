//! Db
//! Used for database IO

pub mod sql_builder;

use polars::prelude::DataType;
use sea_query::Value as SValue;

use crate::{value, FabrixError, FabrixResult, Value};

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
        DataType::Date32 => todo!(),
        DataType::Date64 => todo!(),
        DataType::Time64(_) => todo!(),
        DataType::List(_) => todo!(),
        DataType::Duration(_) => todo!(),
        DataType::Null => todo!(),
        DataType::Categorical => todo!(),
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
        Value::Null => {
            if nullable {
                Ok(from_data_type_to_null_svalue(dtype))
            } else {
                Err(FabrixError::new_parse_error(value, dtype))
            }
        }
    }
}

/// Type conversion: from `sea=query` Value to Value
pub(crate) fn _from_svalue_to_value(svalue: SValue, nullable: bool) -> FabrixResult<Value> {
    if nullable {
        match svalue {
            SValue::Bool(ov) => Ok(value!(ov)),
            SValue::TinyInt(ov) => Ok(value!(ov)),
            SValue::SmallInt(ov) => Ok(value!(ov)),
            SValue::Int(ov) => Ok(value!(ov)),
            SValue::BigInt(ov) => Ok(value!(ov)),
            SValue::TinyUnsigned(ov) => Ok(value!(ov)),
            SValue::SmallUnsigned(ov) => Ok(value!(ov)),
            SValue::Unsigned(ov) => Ok(value!(ov)),
            SValue::BigUnsigned(ov) => Ok(value!(ov)),
            SValue::Float(ov) => Ok(value!(ov)),
            SValue::Double(ov) => Ok(value!(ov)),
            SValue::String(ov) => match ov {
                Some(v) => Ok(value!(*v)),
                None => Ok(value!(None::<String>)),
            },
            SValue::Bytes(_) => todo!(),
            SValue::Json(_) => todo!(),
            SValue::Date(_) => todo!(),
            SValue::Time(_) => todo!(),
            SValue::DateTime(_) => todo!(),
            SValue::DateTimeWithTimeZone(_) => todo!(),
            SValue::Uuid(ov) => match ov {
                Some(v) => Ok(value!(v.to_string())),
                None => Ok(value!(None::<String>)),
            },
            SValue::Decimal(_) => todo!(),
            SValue::BigDecimal(_) => todo!(),
        }
    } else {
        match svalue {
            SValue::Bool(_) => todo!(),
            SValue::TinyInt(_) => todo!(),
            SValue::SmallInt(_) => todo!(),
            SValue::Int(_) => todo!(),
            SValue::BigInt(_) => todo!(),
            SValue::TinyUnsigned(_) => todo!(),
            SValue::SmallUnsigned(_) => todo!(),
            SValue::Unsigned(_) => todo!(),
            SValue::BigUnsigned(_) => todo!(),
            SValue::Float(_) => todo!(),
            SValue::Double(_) => todo!(),
            SValue::String(_) => todo!(),
            SValue::Bytes(_) => todo!(),
            SValue::Json(_) => todo!(),
            SValue::Date(_) => todo!(),
            SValue::Time(_) => todo!(),
            SValue::DateTime(_) => todo!(),
            SValue::DateTimeWithTimeZone(_) => todo!(),
            SValue::Uuid(_) => todo!(),
            SValue::Decimal(_) => todo!(),
            SValue::BigDecimal(_) => todo!(),
        }
    }
}
