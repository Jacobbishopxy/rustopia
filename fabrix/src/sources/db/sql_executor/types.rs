//! Sql types

use std::{collections::HashMap, marker::PhantomData};

use chrono::{NaiveDate, NaiveDateTime, NaiveTime};
use polars::prelude::DataType;
use sqlx::{mysql::MySqlRow, postgres::PgRow, sqlite::SqliteRow, Row as SRow};

use crate::{Decimal, FabrixResult, Uuid, Value};

/// Type of Sql row
pub(crate) enum SqlRow<'a> {
    Mysql(&'a MySqlRow),
    Pg(&'a PgRow),
    Sqlite(&'a SqliteRow),
}

impl<'a> SqlRow<'a> {
    pub(crate) fn len(&self) -> usize {
        match self {
            SqlRow::Mysql(r) => r.len(),
            SqlRow::Pg(r) => r.len(),
            SqlRow::Sqlite(r) => r.len(),
        }
    }
}

impl<'a> From<&'a MySqlRow> for SqlRow<'a> {
    fn from(r: &'a MySqlRow) -> Self {
        Self::Mysql(r)
    }
}

impl<'a> From<&'a PgRow> for SqlRow<'a> {
    fn from(r: &'a PgRow) -> Self {
        Self::Pg(r)
    }
}

impl<'a> From<&'a SqliteRow> for SqlRow<'a> {
    fn from(r: &'a SqliteRow) -> Self {
        Self::Sqlite(r)
    }
}

/// Sql type tag is used to tag static str to Rust primitive type and user customized type
#[derive(Debug)]
pub(crate) struct SqlTypeTag<T>(&'static str, PhantomData<T>)
where
    T: Into<Value>;

impl<T> SqlTypeTag<T>
where
    T: Into<Value>,
{
    pub(crate) fn new(st: &'static str) -> Self {
        SqlTypeTag(st, PhantomData)
    }
}

/// Behavior of SqlTypeTag, used to create trait objects and saving them to the global static HashMap
pub(crate) trait SqlTypeTagMarker: Send + Sync {
    /// to &str
    fn to_str(&self) -> &str;

    /// to polars datatype
    fn to_polars_dtype(&self) -> DataType;

    /// extract Value from sql row
    fn extract_value(&self, sql_row: &SqlRow, idx: usize) -> FabrixResult<Value>;
}

/// tmap value type
pub(crate) type SqlTypeTagKind = Box<dyn SqlTypeTagMarker>;

impl PartialEq<str> for SqlTypeTagKind {
    fn eq(&self, other: &str) -> bool {
        self.to_str() == other
    }
}

impl PartialEq<SqlTypeTagKind> for str {
    fn eq(&self, other: &SqlTypeTagKind) -> bool {
        self == other.to_str()
    }
}

/// impl SqlTypeTagMarker for SqlTypeTag
///
/// Equivalent to:
///
/// ```rust
/// impl SqlTypeTagMarker for SqlTypeTag<bool> {
///     fn to_str(&self) -> &str {
///         self.0
///     }
///
///     fn to_polars_dtype(&self) -> polars::prelude::DataType {
///         polars::prelude::DataType::Boolean
///     }
///
///     fn extract_value(
///         &self,
///         sql_row: &SqlRow,
///         idx: usize,
///     ) -> FabrixResult<Value> {
///         match sql_row {
///             SqlRow::Mysql(r) => {
///                 let v: Option<bool> = r.try_get(idx)?;
///                 match v {
///                     Some(r) => Ok(value!(r)),
///                     None => Ok(Value::Null),
///                 }
///             },
///             SqlRow::Pg(r) => {
///                 let v: Option<bool> = r.try_get(idx)?;
///                 match v {
///                     Some(r) => Ok(value!(r)),
///                     None => Ok(Value::Null),
///                 }
///             },
///             SqlRow::Sqlite(r) => {
///                 let v: Option<bool> = r.try_get(idx)?;
///                 match v {
///                     Some(r) => Ok(value!(r)),
///                     None => Ok(Value::Null),
///                 }
///             },
///         }
///     }
/// }
/// ```
///
/// and custom type:
///
/// ```rust
/// impl SqlTypeTagMarker for SqlTypeTag<Decimal> {
///     fn to_str(&self) -> &str {
///         self.0
///     }
///
///     fn to_polars_dtype(&self) -> DataType {
///         DataType::Object("Decimal")
///     }
///
///     fn extract_value(&self, sql_row: &SqlRow, idx: usize) -> FabrixResult<Value> {
///         match sql_row {
///             SqlRow::Mysql(r) => {
///                 let v: Option<RDecimal> = r.try_get(idx)?;
///                 match v {
///                     Some(r) => Ok(value!(r)),
///                     None => Ok(Value::Null),
///                 }
///             }
///             SqlRow::Pg(r) => {
///                 let v: Option<RDecimal> = r.try_get(idx)?;
///                 match v {
///                     Some(r) => Ok(value!(r)),
///                     None => Ok(Value::Null),
///                 }
///             }
///             _ => Err(FabrixError::new_common_error(MISMATCHED_SQL_ROW)),
///         }
///     }
/// }
/// ```
macro_rules! impl_sql_type_tag_marker {
    ($dtype:ident, $polars_dtype:ident; [$($sql_row_var:ident),*] $(,)* $($residual:expr)?) => {
        impl SqlTypeTagMarker for SqlTypeTag<$dtype> {
            fn to_str(&self) -> &str {
                self.0
            }

            fn to_polars_dtype(&self) -> polars::prelude::DataType {
                polars::prelude::DataType::$polars_dtype
            }

            fn extract_value(
                &self,
                sql_row: &SqlRow,
                idx: usize,
            ) -> $crate::FabrixResult<$crate::Value> {
                match sql_row {
                    $(
                        SqlRow::$sql_row_var(r) => {
                            let v: Option<$dtype> = r.try_get(idx)?;
                            match v {
                                Some(r) => Ok($crate::value!(r)),
                                None => Ok($crate::Value::Null),
                            }
                        },
                    )*
                    $(
                        _ => Err($crate::FabrixError::new_common_error($residual))
                    )?
                }
            }
        }
    };
    ($dtype:ident, $inner_type:ty, $polars_object_name:ident; [$($sql_row_var:ident),*] $(,)* $($residual:expr)?) => {
        impl SqlTypeTagMarker for SqlTypeTag<$dtype> {
            fn to_str(&self) -> &str {
                self.0
            }

            fn to_polars_dtype(&self) -> polars::prelude::DataType {
                polars::prelude::DataType::Object(stringify!($polars_object_name))
            }

            fn extract_value(
                &self,
                sql_row: &SqlRow,
                idx: usize,
            ) -> $crate::FabrixResult<$crate::Value> {
                match sql_row {
                    $(
                        SqlRow::$sql_row_var(r) => {
                            let v: Option<$inner_type> = r.try_get(idx)?;
                            match v {
                                Some(r) => Ok($crate::value!(r)),
                                None => Ok($crate::Value::Null),
                            }
                        },
                    )*
                    $(
                        _ => Err($crate::FabrixError::new_common_error($residual))
                    )?
                }
            }
        }
    };
}

const MISMATCHED_SQL_ROW: &'static str = "mismatched sql row";

impl_sql_type_tag_marker!(bool, Boolean; [Mysql, Pg, Sqlite]);
impl_sql_type_tag_marker!(u8, UInt8; [Mysql], MISMATCHED_SQL_ROW);
impl_sql_type_tag_marker!(u16, UInt16; [Mysql], MISMATCHED_SQL_ROW);
impl_sql_type_tag_marker!(u32, UInt32; [Mysql], MISMATCHED_SQL_ROW);
impl_sql_type_tag_marker!(u64, UInt64; [Mysql], MISMATCHED_SQL_ROW);
impl_sql_type_tag_marker!(i8, Int8; [Mysql, Pg], MISMATCHED_SQL_ROW);
impl_sql_type_tag_marker!(i16, Int16; [Mysql, Pg], MISMATCHED_SQL_ROW);
impl_sql_type_tag_marker!(i32, Int32; [Mysql, Pg], MISMATCHED_SQL_ROW);
impl_sql_type_tag_marker!(i64, Int64; [Mysql, Pg, Sqlite]);
impl_sql_type_tag_marker!(f32, Float32; [Mysql, Pg], MISMATCHED_SQL_ROW);
impl_sql_type_tag_marker!(f64, Float64; [Mysql, Pg, Sqlite]);
impl_sql_type_tag_marker!(String, Utf8; [Mysql, Pg, Sqlite]);
impl_sql_type_tag_marker!(NaiveDateTime, Utf8; [Mysql, Pg, Sqlite]);
impl_sql_type_tag_marker!(NaiveDate, Utf8; [Mysql, Pg], MISMATCHED_SQL_ROW);
impl_sql_type_tag_marker!(NaiveTime, Utf8; [Mysql, Pg], MISMATCHED_SQL_ROW);
impl_sql_type_tag_marker!(Decimal, rust_decimal::Decimal, Decimal; [Mysql, Pg], MISMATCHED_SQL_ROW);
impl_sql_type_tag_marker!(Uuid, uuid::Uuid, Uuid; [Pg], MISMATCHED_SQL_ROW);

/// tmap pair
macro_rules! tmap_pair {
    ($key:expr, $value:ident) => {
        (
            $key,
            Box::new(SqlTypeTag::<$value>::new($key)) as SqlTypeTagKind,
        )
    };
}

lazy_static::lazy_static! {
    /// static Mysql column type mapping
    pub(crate) static ref MYSQL_TMAP: HashMap<&'static str, Box<dyn SqlTypeTagMarker>> = {
        HashMap::from([
            tmap_pair!("TINYINT(1)", bool),
            tmap_pair!("BOOLEAN", bool),
            tmap_pair!("TINYINT UNSIGNED", u8),
            tmap_pair!("SMALLINT UNSIGNED", u16),
            tmap_pair!("INT UNSIGNED", u32),
            tmap_pair!("BIGINT UNSIGNED", u64),
            tmap_pair!("TINYINT", i8),
            tmap_pair!("SMALLINT", i16),
            tmap_pair!("INT", i32),
            tmap_pair!("BIGINT", i64),
            tmap_pair!("FLOAT", f32),
            tmap_pair!("DOUBLE", f64),
            tmap_pair!("VARCHAR", String),
            tmap_pair!("CHAR", String),
            tmap_pair!("TEXT", String),
            tmap_pair!("TIMESTAMP", NaiveDateTime),
            tmap_pair!("DATETIME", NaiveDateTime),
            tmap_pair!("DATE", NaiveDate),
            tmap_pair!("TIME", NaiveTime),
            tmap_pair!("DECIMAL", Decimal),
        ])
    };

    /// static Pg column type mapping
    pub(crate) static ref PG_TMAP: HashMap<&'static str, Box<dyn SqlTypeTagMarker>> = {
        HashMap::from([
            tmap_pair!("BOOL", bool),
            tmap_pair!("CHAR", i8),
            tmap_pair!("TINYINT", i8),
            tmap_pair!("SMALLINT", i16),
            tmap_pair!("SMALLSERIAL", i16),
            tmap_pair!("INT2", i16),
            tmap_pair!("INT", i32),
            tmap_pair!("SERIAL", i32),
            tmap_pair!("INT4", i32),
            tmap_pair!("BIGINT", i64),
            tmap_pair!("BIGSERIAL", i64),
            tmap_pair!("INT8", i64),
            tmap_pair!("REAL", f32),
            tmap_pair!("FLOAT4", f32),
            tmap_pair!("DOUBLE PRECISION", f64),
            tmap_pair!("FLOAT8", f64),
            tmap_pair!("VARCHAR", String),
            tmap_pair!("CHAR(N)", String),
            tmap_pair!("TEXT", String),
            tmap_pair!("NAME", String),
            tmap_pair!("TIMESTAMPTZ", NaiveDateTime),
            tmap_pair!("TIMESTAMP", NaiveDateTime),
            tmap_pair!("DATE", NaiveDate),
            tmap_pair!("TIME", NaiveTime),
            tmap_pair!("NUMERIC", Decimal),
        ])
    };

    /// static Sqlite column type mapping
    pub(crate) static ref SQLITE_TMAP: HashMap<&'static str, Box<dyn SqlTypeTagMarker>> = {
        HashMap::from([
            tmap_pair!("BOOLEAN", bool),
            tmap_pair!("INTEGER", i32),
            tmap_pair!("BIGINT", i64),
            tmap_pair!("INT8", i64),
            tmap_pair!("REAL", f64),
            tmap_pair!("VARCHAR", String),
            tmap_pair!("CHAR(N)", String),
            tmap_pair!("TEXT", String),
            tmap_pair!("DATETIME", NaiveDateTime),
        ])
    };
}

#[cfg(test)]
mod test_types {
    use super::*;

    #[test]
    fn test_cmp() {
        let mysql_bool = MYSQL_TMAP.get("TINYINT(1)").unwrap();

        println!("{:?}", "TINYINT(1)" == mysql_bool);
    }
}
