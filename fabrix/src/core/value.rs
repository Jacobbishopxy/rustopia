//! fabrix value

use polars::prelude::AnyValue;

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
    (Option<$ftype:ty>, $any_val_var:ident) => {
        impl<'a> From<Option<$ftype>> for Value<'a> {
            fn from(ov: Option<$ftype>) -> Self {
                match ov {
                    Some(v) => $crate::Value(polars::prelude::AnyValue::$any_val_var(v)),
                    None => $crate::Value(polars::prelude::AnyValue::Null),
                }
            }
        }
    };
    ($ftype:ty, $any_val_var:ident) => {
        impl<'a> From<$ftype> for Value<'a> {
            fn from(v: $ftype) -> Self {
                $crate::Value(polars::prelude::AnyValue::$any_val_var(v))
            }
        }
    };
}

// TODO: check
impl<'a> From<Option<String>> for Value<'a> {
    fn from(ov: Option<String>) -> Self {
        match ov {
            Some(v) => Value(AnyValue::Utf8(Box::leak(v.into_boxed_str()))),
            None => Value(AnyValue::Null),
        }
    }
}

impl<'a> From<String> for Value<'a> {
    fn from(v: String) -> Self {
        Value(AnyValue::Utf8(Box::leak(v.into_boxed_str())))
    }
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
impl_value_from!(Option<bool>, Boolean);
impl_value_from!(Option<&'a str>, Utf8);
impl_value_from!(Option<u8>, UInt8);
impl_value_from!(Option<u16>, UInt16);
impl_value_from!(Option<u32>, UInt32);
impl_value_from!(Option<u64>, UInt64);
impl_value_from!(Option<i8>, Int8);
impl_value_from!(Option<i16>, Int16);
impl_value_from!(Option<i32>, Int32);
impl_value_from!(Option<i64>, Int64);
impl_value_from!(Option<f32>, Float32);
impl_value_from!(Option<f64>, Float64);

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
    ($any_val_var:ident, Option<$ftype:ty>, $hint:expr) => {
        impl<'a> TryFrom<$crate::Value<'a>> for Option<$ftype> {
            type Error = $crate::FabrixError;

            fn try_from(value: $crate::Value<'a>) -> Result<Self, Self::Error> {
                match value.0 {
                    polars::prelude::AnyValue::Null => Ok(None),
                    polars::prelude::AnyValue::$any_val_var(v) => Ok(Some(v)),
                    _ => Err($crate::FabrixError::new_parse_info_error(value, $hint)),
                }
            }
        }
    };
    ($any_val_var:ident, $ftype:ty, $hint:expr) => {
        impl<'a> TryFrom<$crate::Value<'a>> for $ftype {
            type Error = $crate::FabrixError;

            fn try_from(value: $crate::Value<'a>) -> Result<Self, Self::Error> {
                match value.0 {
                    polars::prelude::AnyValue::$any_val_var(v) => Ok(v),
                    _ => Err($crate::FabrixError::new_parse_info_error(value, $hint)),
                }
            }
        }
    };
}

impl_try_from_value!(Boolean, bool, "bool");
impl_try_from_value!(Utf8, &'a str, "&'a str");
impl_try_from_value!(UInt8, u8, "u8");
impl_try_from_value!(UInt16, u16, "u16");
impl_try_from_value!(UInt32, u32, "u32");
impl_try_from_value!(UInt64, u64, "u64");
impl_try_from_value!(Int8, i8, "i8");
impl_try_from_value!(Int16, i16, "i16");
impl_try_from_value!(Int32, i32, "i32");
impl_try_from_value!(Int64, i64, "i64");
impl_try_from_value!(Float32, f32, "f32");
impl_try_from_value!(Float64, f64, "f64");
impl_try_from_value!(Boolean, Option<bool>, "bool");
impl_try_from_value!(Utf8, Option<&'a str>, "&'a str");
impl_try_from_value!(UInt8, Option<u8>, "u8");
impl_try_from_value!(UInt16, Option<u16>, "u16");
impl_try_from_value!(UInt32, Option<u32>, "u32");
impl_try_from_value!(UInt64, Option<u64>, "u64");
impl_try_from_value!(Int8, Option<i8>, "i8");
impl_try_from_value!(Int16, Option<i16>, "i16");
impl_try_from_value!(Int32, Option<i32>, "i32");
impl_try_from_value!(Int64, Option<i64>, "i64");
impl_try_from_value!(Float32, Option<f32>, "f32");
impl_try_from_value!(Float64, Option<f64>, "f64");

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

        let v = value!(Some(123));
        let i = Option::<i32>::try_from(v).unwrap();

        println!("{:?}", i);

        let v = value!(None::<i32>);
        let i = Option::<i32>::try_from(v).unwrap();

        println!("{:?}", i);
    }
}
