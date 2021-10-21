//! Db
//! Used for database IO

pub mod sql_builder;

use sea_query::Value as SValue;

use crate::{value, Value};

// TODO:
/// Type conversion: from Value to `sea-query` Value
pub(crate) fn from_value_to_svalue(value: Value, nullable: bool) -> SValue {
    match value {
        Value::Id(_) => todo!(),
        Value::Bool(_) => todo!(),
        Value::U8(_) => todo!(),
        Value::U16(_) => todo!(),
        Value::U32(_) => todo!(),
        Value::U64(_) => todo!(),
        Value::I8(_) => todo!(),
        Value::I16(_) => todo!(),
        Value::I32(_) => todo!(),
        Value::I64(_) => todo!(),
        Value::F32(_) => todo!(),
        Value::F64(_) => todo!(),
        Value::String(_) => todo!(),
        Value::Date(_) => todo!(),
        Value::Time(_) => todo!(),
        Value::DateTime(_) => todo!(),
        Value::Null => todo!(),
    }
}

// TODO:
/// Type conversion: from `sea=query` Value to Value
pub(crate) fn from_svalue_to_value(svalue: SValue, nullable: bool) -> Value {
    if nullable {
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
    } else {
        match svalue {
            SValue::Bool(ov) => match ov {
                Some(v) => value!(v),
                None => value!(false),
            },
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
