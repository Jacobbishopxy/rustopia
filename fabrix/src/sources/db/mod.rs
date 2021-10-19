//! Db
//! Used for database IO

pub mod sql;

use polars::prelude::AnyValue;
use sea_query::Value as SValue;

use crate::Value;

/// Type conversion: from Value to `sea-query` Value
pub(crate) fn from_value_to_svalue<'a>(value: Value<'a>, nullable: bool) -> SValue {
    match value.0 {
        AnyValue::Null => todo!(),
        AnyValue::Boolean(_) => todo!(),
        AnyValue::Utf8(_) => todo!(),
        AnyValue::UInt8(_) => todo!(),
        AnyValue::UInt16(_) => todo!(),
        AnyValue::UInt32(_) => todo!(),
        AnyValue::UInt64(_) => todo!(),
        AnyValue::Int8(_) => todo!(),
        AnyValue::Int16(_) => todo!(),
        AnyValue::Int32(_) => todo!(),
        AnyValue::Int64(_) => todo!(),
        AnyValue::Float32(_) => todo!(),
        AnyValue::Float64(_) => todo!(),
        AnyValue::Date32(_) => todo!(),
        AnyValue::Date64(_) => todo!(),
        AnyValue::Time64(_, _) => todo!(),
        AnyValue::Duration(_, _) => todo!(),
        AnyValue::List(_) => todo!(),
    }
}

/// Type conversion: from `sea=query` Value to Value
pub(crate) fn from_svalue_to_value<'a>(svalue: SValue, nullable: bool) -> Value<'a> {
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
