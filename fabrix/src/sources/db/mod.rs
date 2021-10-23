//! Db
//! Used for database IO

pub mod sql_builder;

use polars::prelude::DataType;
use sea_query::Value as SValue;

use crate::{value, FabrixError, FabrixResult, Value};

/// Type conversion: from Value to `sea-query` Value
pub(crate) fn try_from_value_to_svalue(
    value: Value,
    dtype: &DataType,
    nullable: bool,
) -> FabrixResult<SValue> {
    match value {
        Value::Id(v) => Ok(SValue::BigUnsigned(Some(v))),
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
        Value::Date(v) => todo!(),
        Value::Time(v) => todo!(),
        Value::DateTime(v) => todo!(),
        Value::Null => {
            if nullable {
                todo!()
            } else {
                Err(FabrixError::new_parse_error(value, dtype))
            }
        }
    }
}

/// Type conversion: from `sea=query` Value to Value
pub(crate) fn from_svalue_to_value(svalue: SValue, nullable: bool) -> Value {
    match svalue {
        SValue::Bool(ov) => value!(ov),
        SValue::TinyInt(ov) => value!(ov),
        SValue::SmallInt(ov) => value!(ov),
        SValue::Int(ov) => value!(ov),
        SValue::BigInt(ov) => value!(ov),
        SValue::TinyUnsigned(ov) => value!(ov),
        SValue::SmallUnsigned(ov) => value!(ov),
        SValue::Unsigned(ov) => value!(ov),
        SValue::BigUnsigned(ov) => value!(ov),
        SValue::Float(ov) => value!(ov),
        SValue::Double(ov) => value!(ov),
        SValue::String(ov) => match ov {
            Some(v) => value!(*v),
            None => value!(None::<String>),
        },
        SValue::Bytes(_) => todo!(),
        SValue::Json(_) => todo!(),
        SValue::Date(_) => todo!(),
        SValue::Time(_) => todo!(),
        SValue::DateTime(_) => todo!(),
        SValue::DateTimeWithTimeZone(_) => todo!(),
        SValue::Uuid(ov) => match ov {
            Some(v) => value!(v.to_string()),
            None => value!(None::<String>),
        },
        SValue::Decimal(_) => todo!(),
        SValue::BigDecimal(_) => todo!(),
    }
}
