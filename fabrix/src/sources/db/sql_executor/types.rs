//! Sql types

use std::{collections::HashMap, marker::PhantomData};

use chrono::{NaiveDate, NaiveDateTime, NaiveTime};
use polars::prelude::DataType;
use rust_decimal::Decimal;
use sqlx::{mysql::MySqlRow, postgres::PgRow, sqlite::SqliteRow, Column, Row as SRow};

use crate::{FabrixResult, Row, Value};

pub(crate) enum SqlRow {
    Mysql(MySqlRow),
    Pg(PgRow),
    Sqlite(SqliteRow),
}

pub(crate) trait SqlTypeTagMarker: Send + Sync {
    fn to_str(&self) -> &str;

    fn to_polars_dtype(&self) -> DataType;

    // fn row_process(&self, sql_row: SqlRow, idx: usize) -> FabrixResult<Value>;
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
            fn to_str(&self) -> &str {
                self.0
            }

            fn to_polars_dtype(&self) -> polars::prelude::DataType {
                polars::prelude::DataType::$polars_dtype
            }

            // fn row_process(
            //     &self,
            //     sql_row: SqlRow,
            //     idx: usize,
            // ) -> $crate::FabrixResult<$crate::Value> {
            //     match sql_row {
            //         SqlRow::Mysql(r) => {
            //             let v: Option<$dtype> = r.try_get(idx)?;
            //             match v {
            //                 Some(r) => Ok($crate::value!(r)),
            //                 None => Ok($crate::Value::Null),
            //             }
            //         }
            //         SqlRow::Pg(r) => {
            //             let v: Option<$dtype> = r.try_get(idx)?;
            //             match v {
            //                 Some(r) => Ok($crate::value!(r)),
            //                 None => Ok($crate::Value::Null),
            //             }
            //         }
            //         SqlRow::Sqlite(r) => {
            //             let v: Option<$dtype> = r.try_get(idx)?;
            //             match v {
            //                 Some(r) => Ok($crate::value!(r)),
            //                 None => Ok($crate::Value::Null),
            //             }
            //         }
            //     }
            // }
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
    static ref MYSQL_TMAP: HashMap<&'static str, Box<dyn SqlTypeTagMarker>> = {
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

    static ref PG_TMAP: HashMap<&'static str, Box<dyn SqlTypeTagMarker>> = {
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

    static ref SQLITE_TMAP: HashMap<&'static str, Box<dyn SqlTypeTagMarker>> = {
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

pub(crate) fn row_processor_mysql(row: MySqlRow) -> FabrixResult<Row> {
    // let len = row.len();
    // let mut res = Vec::with_capacity(len);

    // for i in 0..len {
    //     let type_name = row.column(i).to_string();

    //     match MYSQL_TMAP.get(type_name) {
    //         Some(m) => row.try_get(i),
    //         None => todo!(),
    //     }
    // }

    todo!()
}

pub(crate) fn row_processor_pg(row: PgRow) -> FabrixResult<Row> {
    todo!()
}

pub(crate) fn row_processor_sqlite(row: SqliteRow) -> FabrixResult<Row> {
    todo!()
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
