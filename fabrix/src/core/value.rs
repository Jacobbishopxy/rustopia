//! fabrix value

use chrono::{Datelike, NaiveDate, NaiveDateTime, NaiveTime, Timelike};
use polars::prelude::{AnyValue, DataType, Field, ObjectType, PolarsObject, TimeUnit};
use serde::{Deserialize, Serialize};

/// Custom Value: Decimal
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Eq, Hash, Default)]
pub struct Decimal(pub rust_decimal::Decimal);

pub type ObjectTypeDecimal = ObjectType<Decimal>;

impl std::fmt::Display for Decimal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self.0)
    }
}

impl PolarsObject for Decimal {
    fn type_name() -> &'static str {
        "Decimal"
    }
}

/// Custom Value: Uuid
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Eq, Hash, Default)]
pub struct Uuid(pub uuid::Uuid);

pub type ObjectTypeUuid = ObjectType<Uuid>;

impl std::fmt::Display for Uuid {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self.0)
    }
}

impl PolarsObject for Uuid {
    fn type_name() -> &'static str {
        "Uuid"
    }
}

/// FValue is a wrapper used for holding Polars AnyValue in order to
/// satisfy type conversion between `sea_query::Value`
#[derive(PartialEq, Clone, Debug, Deserialize, Serialize)]
pub enum Value {
    Bool(bool),
    U8(u8),
    U16(u16),
    U32(u32),
    U64(u64),
    I8(i8),
    I16(i16),
    I32(i32),
    I64(i64),
    F32(f32),
    F64(f64),
    String(String),
    Date(NaiveDate),
    Time(NaiveTime),
    DateTime(NaiveDateTime),
    Decimal(Decimal),
    Uuid(Uuid),
    Null,
}

impl std::fmt::Display for Value {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Value::Bool(v) => write!(f, "{:?}", v),
            Value::U8(v) => write!(f, "{:?}", v),
            Value::U16(v) => write!(f, "{:?}", v),
            Value::U32(v) => write!(f, "{:?}", v),
            Value::U64(v) => write!(f, "{:?}", v),
            Value::I8(v) => write!(f, "{:?}", v),
            Value::I16(v) => write!(f, "{:?}", v),
            Value::I32(v) => write!(f, "{:?}", v),
            Value::I64(v) => write!(f, "{:?}", v),
            Value::F32(v) => write!(f, "{:?}", v),
            Value::F64(v) => write!(f, "{:?}", v),
            Value::String(v) => write!(f, "{:?}", v),
            Value::Date(v) => write!(f, "{:?}", v),
            Value::Time(v) => write!(f, "{:?}", v),
            Value::DateTime(v) => write!(f, "{:?}", v),
            Value::Decimal(v) => write!(f, "{:?}", v.0),
            Value::Uuid(v) => write!(f, "{:?}", v.0),
            Value::Null => write!(f, "null"),
        }
    }
}

impl From<&Value> for DataType {
    fn from(v: &Value) -> Self {
        match v {
            Value::Bool(_) => DataType::Boolean,
            Value::U8(_) => DataType::UInt8,
            Value::U16(_) => DataType::UInt32,
            Value::U32(_) => DataType::UInt32,
            Value::U64(_) => DataType::UInt64,
            Value::I8(_) => DataType::Int8,
            Value::I16(_) => DataType::Int32,
            Value::I32(_) => DataType::Int32,
            Value::I64(_) => DataType::Int64,
            Value::F32(_) => DataType::Float32,
            Value::F64(_) => DataType::Float64,
            Value::String(_) => DataType::Utf8,
            Value::Date(_) => DataType::Int64,
            Value::Time(_) => DataType::Int64,
            Value::DateTime(_) => DataType::Int64,
            Value::Decimal(_) => DataType::Object("Decimal"),
            Value::Uuid(_) => DataType::Object("Uuid"),
            Value::Null => DataType::Null,
        }
    }
}

impl From<Value> for DataType {
    fn from(v: Value) -> Self {
        DataType::from(&v)
    }
}

impl From<&Value> for Field {
    fn from(v: &Value) -> Self {
        match v {
            Value::Bool(_) => Field::new("", DataType::Boolean),
            Value::U8(_) => Field::new("", DataType::UInt8),
            Value::U16(_) => Field::new("", DataType::UInt16),
            Value::U32(_) => Field::new("", DataType::UInt32),
            Value::U64(_) => Field::new("", DataType::UInt64),
            Value::I8(_) => Field::new("", DataType::Int8),
            Value::I16(_) => Field::new("", DataType::Int16),
            Value::I32(_) => Field::new("", DataType::Int32),
            Value::I64(_) => Field::new("", DataType::Int64),
            Value::F32(_) => Field::new("", DataType::Float32),
            Value::F64(_) => Field::new("", DataType::Float64),
            Value::String(_) => Field::new("", DataType::Utf8),
            Value::Date(_) => Field::new("", DataType::Date32),
            Value::Time(_) => Field::new("", DataType::Date64),
            Value::DateTime(_) => Field::new("", DataType::Date64),
            Value::Decimal(_) => Field::new("", DataType::Object("Decimal")),
            Value::Uuid(_) => Field::new("", DataType::Object("Uuid")),
            Value::Null => Field::new("", DataType::Null),
        }
    }
}

impl From<Value> for Field {
    fn from(v: Value) -> Self {
        Field::from(&v)
    }
}

impl Value {
    pub fn is_dtype_match(&self, dtype: &DataType) -> bool {
        let vd = DataType::from(self);
        &vd == dtype
    }
}

/// default value: null
impl Default for Value {
    fn default() -> Self {
        Value::Null
    }
}

/// Type conversion: polars' AnyValue -> Value
impl<'a> From<AnyValue<'a>> for Value {
    fn from(av: AnyValue<'a>) -> Self {
        match av {
            AnyValue::Null => Value::Null,
            AnyValue::Boolean(v) => Value::Bool(v),
            AnyValue::Utf8(v) => Value::String(v.to_owned()),
            AnyValue::UInt8(v) => Value::U8(v),
            AnyValue::UInt16(v) => Value::U16(v),
            AnyValue::UInt32(v) => Value::U32(v),
            AnyValue::UInt64(v) => Value::U64(v),
            AnyValue::Int8(v) => Value::I8(v),
            AnyValue::Int16(v) => Value::I16(v),
            AnyValue::Int32(v) => Value::I32(v),
            AnyValue::Int64(v) => Value::I64(v),
            AnyValue::Float32(v) => Value::F32(v),
            AnyValue::Float64(v) => Value::F64(v),
            AnyValue::Date32(_) => todo!(),
            AnyValue::Date64(_) => todo!(),
            _ => unimplemented!(),
        }
    }
}

/// Type conversion: Value -> polars' AnyValue
impl<'a> From<&'a Value> for AnyValue<'a> {
    fn from(v: &'a Value) -> Self {
        match v {
            Value::Bool(v) => AnyValue::Boolean(v.clone()),
            Value::U8(v) => AnyValue::UInt8(v.clone()),
            Value::U16(v) => AnyValue::UInt16(v.clone()),
            Value::U32(v) => AnyValue::UInt32(v.clone()),
            Value::U64(v) => AnyValue::UInt64(v.clone()),
            Value::I8(v) => AnyValue::Int8(v.clone()),
            Value::I16(v) => AnyValue::Int16(v.clone()),
            Value::I32(v) => AnyValue::Int32(v.clone()),
            Value::I64(v) => AnyValue::Int64(v.clone()),
            Value::F32(v) => AnyValue::Float32(v.clone()),
            Value::F64(v) => AnyValue::Float64(v.clone()),
            Value::String(v) => AnyValue::Utf8(v),
            Value::Date(v) => todo!(),
            Value::Time(v) => todo!(),
            Value::DateTime(v) => todo!(),
            Value::Decimal(v) => AnyValue::Object(v),
            Value::Uuid(v) => AnyValue::Object(v),
            Value::Null => AnyValue::Null,
        }
    }
}

/// Type conversion: standard type into Value
///
/// Equivalent to:
///
/// ```rust
/// impl From<Option<bool>> for Value {
///     fn from(ov: Option<bool>) -> Self {
///         match ov {
///             Some(v) => Value::Bool(v)
///             None => Value::Null
///         }
///         Value(Value::Bool(v))
///     }
/// }
/// ```
///
/// and:
///
/// ```rust
/// impl From<bool> for Value {
///     fn from(v: bool) -> Self {
///         Value(Value::Bool(v))
///     }
/// }
/// ```
macro_rules! impl_value_from {
    (Option<$ftype:ty>, $val_var:ident) => {
        impl From<Option<$ftype>> for Value {
            fn from(ov: Option<$ftype>) -> Self {
                match ov {
                    Some(v) => $crate::Value::$val_var(v),
                    None => $crate::Value::Null,
                }
            }
        }
    };
    ($ftype:ty, $val_var:ident) => {
        impl From<$ftype> for Value {
            fn from(v: $ftype) -> Self {
                $crate::Value::$val_var(v)
            }
        }
    };
    (Option<$ftype:ty>, $wrapper:expr, $val_var:ident) => {
        impl From<Option<$ftype>> for Value {
            fn from(ov: Option<$ftype>) -> Self {
                match ov {
                    Some(v) => $crate::Value::$val_var($wrapper(v)),
                    None => $crate::Value::Null,
                }
            }
        }
    };
    ($ftype:ty, $wrapper:expr, $val_var:ident) => {
        impl From<$ftype> for Value {
            fn from(v: $ftype) -> Self {
                $crate::Value::$val_var($wrapper(v))
            }
        }
    };
}

impl_value_from!(bool, Bool);
impl_value_from!(String, String);
impl_value_from!(u8, U8);
impl_value_from!(u16, U16);
impl_value_from!(u32, U32);
impl_value_from!(u64, U64);
impl_value_from!(i8, I8);
impl_value_from!(i16, I16);
impl_value_from!(i32, I32);
impl_value_from!(i64, I64);
impl_value_from!(f32, F32);
impl_value_from!(f64, F64);
impl_value_from!(NaiveDate, Date);
impl_value_from!(NaiveTime, Time);
impl_value_from!(NaiveDateTime, DateTime);
impl_value_from!(Decimal, Decimal);
impl_value_from!(rust_decimal::Decimal, Decimal, Decimal);
impl_value_from!(Uuid, Uuid);
impl_value_from!(uuid::Uuid, Uuid, Uuid);

impl_value_from!(Option<bool>, Bool);
impl_value_from!(Option<String>, String);
impl_value_from!(Option<u8>, U8);
impl_value_from!(Option<u16>, U16);
impl_value_from!(Option<u32>, U32);
impl_value_from!(Option<u64>, U64);
impl_value_from!(Option<i8>, I8);
impl_value_from!(Option<i16>, I16);
impl_value_from!(Option<i32>, I32);
impl_value_from!(Option<i64>, I64);
impl_value_from!(Option<f32>, F32);
impl_value_from!(Option<f64>, F64);
impl_value_from!(Option<NaiveDate>, Date);
impl_value_from!(Option<NaiveTime>, Time);
impl_value_from!(Option<NaiveDateTime>, DateTime);
impl_value_from!(Option<Decimal>, Decimal);
impl_value_from!(Option<rust_decimal::Decimal>, Decimal, Decimal);
impl_value_from!(Option<Uuid>, Uuid);
impl_value_from!(Option<uuid::Uuid>, Uuid, Uuid);

impl From<&str> for Value {
    fn from(v: &str) -> Self {
        Value::String(v.to_owned())
    }
}

impl From<Option<&str>> for Value {
    fn from(ov: Option<&str>) -> Self {
        match ov {
            Some(v) => Value::String(v.to_owned()),
            None => Value::Null,
        }
    }
}

/// Type conversion: Value try_into standard type
///
/// Equivalent to:
///
/// ```rust
/// impl TryFrom<Value> for Option<bool> {
///     type Error = FabrixError;
///     fn try_from(value: Value) -> Result<Self, Self::Error> {
///         match value {
///             Value::Null => Ok(None),
///             Value::Boolean(v) => Ok(Some(v)),
///             _ => Err(FabrixError::new_parse_info_error(value, "bool")),
///         }
///     }
/// }
/// ```
///
/// and:
///
/// ```rust
/// impl TryFrom<Value> for bool {
///     type Error = FabrixError;
///     fn try_from(value: Value) -> Result<Self, Self::Error> {
///         match value {
///             Value::Boolean(v) => Ok(v),
///             _ => Err(FabrixError::new_parse_info_error(value, "bool")),
///         }
///     }
/// }
/// ```
macro_rules! impl_try_from_value {
    ($val_var:ident, Option<$ftype:ty>, $hint:expr) => {
        impl TryFrom<$crate::Value> for Option<$ftype> {
            type Error = $crate::FabrixError;

            fn try_from(value: $crate::Value) -> Result<Self, Self::Error> {
                match value {
                    $crate::Value::Null => Ok(None),
                    $crate::Value::$val_var(v) => Ok(Some(v)),
                    _ => Err($crate::FabrixError::new_parse_info_error(value, $hint)),
                }
            }
        }
    };
    ($val_var:ident, $ftype:ty, $hint:expr) => {
        impl TryFrom<$crate::Value> for $ftype {
            type Error = $crate::FabrixError;

            fn try_from(value: $crate::Value) -> Result<Self, Self::Error> {
                match value {
                    $crate::Value::$val_var(v) => Ok(v),
                    _ => Err($crate::FabrixError::new_parse_info_error(value, $hint)),
                }
            }
        }
    };
}

impl_try_from_value!(Bool, bool, "bool");
impl_try_from_value!(String, String, "String");
impl_try_from_value!(U8, u8, "u8");
impl_try_from_value!(U16, u16, "u16");
impl_try_from_value!(U32, u32, "u32");
impl_try_from_value!(U64, u64, "u64");
impl_try_from_value!(I8, i8, "i8");
impl_try_from_value!(I16, i16, "i16");
impl_try_from_value!(I32, i32, "i32");
impl_try_from_value!(I64, i64, "i64");
impl_try_from_value!(F32, f32, "f32");
impl_try_from_value!(F64, f64, "f64");
impl_try_from_value!(Decimal, Decimal, "Decimal");
impl_try_from_value!(Uuid, Uuid, "Uuid");

impl_try_from_value!(Bool, Option<bool>, "bool");
impl_try_from_value!(String, Option<String>, "String");
impl_try_from_value!(U8, Option<u8>, "u8");
impl_try_from_value!(U16, Option<u16>, "u16");
impl_try_from_value!(U32, Option<u32>, "u32");
impl_try_from_value!(U64, Option<u64>, "u64");
impl_try_from_value!(I8, Option<i8>, "i8");
impl_try_from_value!(I16, Option<i16>, "i16");
impl_try_from_value!(I32, Option<i32>, "i32");
impl_try_from_value!(I64, Option<i64>, "i64");
impl_try_from_value!(F32, Option<f32>, "f32");
impl_try_from_value!(F64, Option<f64>, "f64");
impl_try_from_value!(Decimal, Option<Decimal>, "Decimal");
impl_try_from_value!(Uuid, Option<Uuid>, "Uuid");

#[cfg(test)]
mod test_value {

    use crate::{value, Decimal, Uuid, Value};
    use rust_decimal::Decimal as RDecimal;
    use uuid::Uuid as UUuid;

    #[test]
    fn test_conversion() {
        let v = 123;
        let i = Value::from(v);

        println!("{:?}", i);

        let v = value!(123);
        let i = i32::try_from(v).unwrap();

        println!("{:?}", i);

        let v = value!(Some(123));
        let i = Option::<i32>::try_from(v).unwrap();

        println!("{:?}", i);

        let v = value!(None::<i32>);
        let i = Option::<i32>::try_from(v).unwrap();

        println!("{:?}", i);
    }

    #[test]
    fn test_custom_type_conversion() {
        let v = RDecimal::new(123, 0);
        let v = Some(Decimal(v));
        let v: Value = v.into();

        println!("{:?}", v);

        let v = UUuid::new_v4();
        let v = Some(Uuid(v));
        let v: Value = v.into();

        println!("{:?}", v);
    }
}
