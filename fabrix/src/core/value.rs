//! fabrix value

use polars::prelude::{AnyValue, DataType};
use sea_query::Value as SQValue;

use crate::FabrixError;

/// FValue is a wrapper used for holding Polars AnyValue in order to
/// satisfy type conversion between `sea_query::Value`
#[derive(PartialEq, Clone)]
pub struct Value<'a>(pub(crate) AnyValue<'a>);

impl<'a> Value<'a> {
    pub fn new(v: AnyValue<'a>) -> Self {
        Value(v)
    }
}

impl<'a> std::fmt::Debug for Value<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self.0)
    }
}

/// &Value and Value comparison
impl<'a> PartialEq<Value<'a>> for &Value<'a> {
    fn eq(&self, other: &Value<'a>) -> bool {
        self.0 == other.0
    }
}

/// Value display
impl<'a> std::fmt::Display for Value<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
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

/// default value: null
impl<'a> Default for Value<'a> {
    fn default() -> Self {
        Value(AnyValue::Null)
    }
}

/// Type conversion: standard type into Value
/// same as:
/// impl<'a> From<bool> for Value<'a> {
///     fn from(v: bool) -> Self {
///         Value(AnyValue::Boolean(v))
///     }
/// }
macro_rules! impl_value_from {
    ($type:ty, $any_val_var:ident) => {
        impl<'a> From<$type> for Value<'a> {
            fn from(v: $type) -> Self {
                $crate::Value(polars::prelude::AnyValue::$any_val_var(v))
            }
        }
    };
}

impl_value_from!(bool, Boolean);
impl_value_from!(&'a str, Utf8);
impl_value_from!(u8, UInt8);
impl_value_from!(u16, UInt16);
impl_value_from!(u32, UInt32);
impl_value_from!(u64, UInt64);
impl_value_from!(i8, Int8);
impl_value_from!(i16, Int16);
impl_value_from!(i32, Int32);
impl_value_from!(i64, Int64);
impl_value_from!(f32, Float32);
impl_value_from!(f64, Float64);

/// Type conversion: Value try_into standard type
/// same as:
/// impl<'a> TryFrom<Value<'a>> for bool {
///     type Error = FabrixError;
///     fn try_from(value: Value<'a>) -> Result<Self, Self::Error> {
///         match value.0 {
///             AnyValue::Boolean(v) => Ok(v),
///             _ => Err(FabrixError::new_parse_info_error(value, "bool")),
///         }
///     }
/// }
macro_rules! impl_try_from_value {
    ($any_val_var:ident, $type:ty) => {
        impl<'a> TryFrom<$crate::Value<'a>> for $type {
            type Error = $crate::FabrixError;

            fn try_from(value: $crate::Value<'a>) -> Result<Self, Self::Error> {
                match value.0 {
                    polars::prelude::AnyValue::$any_val_var(v) => Ok(v),
                    _ => Err($crate::FabrixError::new_parse_info_error(value, "bool")),
                }
            }
        }
    };
}

impl_try_from_value!(Boolean, bool);
impl_try_from_value!(Utf8, &'a str);
impl_try_from_value!(UInt8, u8);
impl_try_from_value!(UInt16, u16);
impl_try_from_value!(UInt32, u32);
impl_try_from_value!(UInt64, u64);
impl_try_from_value!(Int8, i8);
impl_try_from_value!(Int16, i16);
impl_try_from_value!(Int32, i32);
impl_try_from_value!(Int64, i64);
impl_try_from_value!(Float32, f32);
impl_try_from_value!(Float64, f64);

impl<'a> TryFrom<Value<'a>> for String {
    type Error = FabrixError;

    fn try_from(value: Value<'a>) -> Result<Self, Self::Error> {
        match value.0 {
            AnyValue::Utf8(v) => Ok(v.to_owned()),
            _ => Err(FabrixError::new_parse_info_error(value, "String")),
        }
    }
}

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
    }
}
