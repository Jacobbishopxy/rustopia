//! fabrix value

use chrono::{NaiveDate, NaiveDateTime, NaiveTime};
use polars::prelude::{AnyValue, DataType};

/// FValue is a wrapper used for holding Polars AnyValue in order to
/// satisfy type conversion between `sea_query::Value`
#[derive(PartialEq, Clone, Debug)]
pub enum Value {
    Id(u64),
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
    Null,
}

impl std::fmt::Display for Value {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Value::Id(v) => write!(f, "{:?}", v),
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
            Value::Null => write!(f, "null"),
        }
    }
}

impl From<&Value> for DataType {
    fn from(v: &Value) -> Self {
        match v {
            Value::Id(_) => DataType::UInt64,
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
            Value::Null => DataType::Null,
        }
    }
}

impl From<Value> for DataType {
    fn from(v: Value) -> Self {
        DataType::from(&v)
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
            Value::Id(v) => AnyValue::UInt64(v.clone()),
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
            Value::Date(_) => todo!(),
            Value::Time(_) => todo!(),
            Value::DateTime(_) => todo!(),
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

#[cfg(test)]
mod test_value {

    use crate::{value, Value};

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
}
