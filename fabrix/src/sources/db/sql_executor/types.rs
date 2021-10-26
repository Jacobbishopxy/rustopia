//! Sql types

use std::{collections::HashMap, marker::PhantomData};

use chrono::{NaiveDate, NaiveDateTime, NaiveTime};
use polars::prelude::DataType;
use rust_decimal::Decimal;

use crate::Value;

pub(crate) trait SqlTypeTagMarker: Send + Sync {
    fn to_polars_dtype(&self) -> DataType;
}

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

///
macro_rules! impl_sql_type_tag_marker {
    ($dtype:ident, $polars_dtype:ident) => {
        impl SqlTypeTagMarker for SqlTypeTag<$dtype> {
            fn to_polars_dtype(&self) -> polars::prelude::DataType {
                polars::prelude::DataType::$polars_dtype
            }
        }
    };
}

impl_sql_type_tag_marker!(bool, Boolean);
impl_sql_type_tag_marker!(u8, UInt8);
impl_sql_type_tag_marker!(u16, UInt16);
impl_sql_type_tag_marker!(u32, UInt32);
impl_sql_type_tag_marker!(u64, UInt64);
impl_sql_type_tag_marker!(i8, Int8);
impl_sql_type_tag_marker!(i16, Int16);
impl_sql_type_tag_marker!(i32, Int32);
impl_sql_type_tag_marker!(i64, Int64);
impl_sql_type_tag_marker!(f32, Float32);
impl_sql_type_tag_marker!(f64, Float64);
impl_sql_type_tag_marker!(String, Utf8);
impl_sql_type_tag_marker!(NaiveDateTime, Utf8);
impl_sql_type_tag_marker!(NaiveDate, Utf8);
impl_sql_type_tag_marker!(NaiveTime, Utf8);
impl_sql_type_tag_marker!(Decimal, Utf8);

/// tmap value type
pub(crate) type SqlTypeTagKind = Box<dyn SqlTypeTagMarker + Send + Sync>;

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
    static ref MYSQL_TMAP: HashMap<&'static str, Box<dyn SqlTypeTagMarker + Send + Sync>> = {
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

    static ref PG_TMAP: HashMap<&'static str, Box<dyn SqlTypeTagMarker + Send + Sync>> = {
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

    static ref SQLITE_TMAP: HashMap<&'static str, Box<dyn SqlTypeTagMarker + Send + Sync>> = {
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
    fn name() {
        println!(
            "{:?}",
            MYSQL_TMAP.get("TINYINT(1)").unwrap().to_polars_dtype()
        );
    }
}
