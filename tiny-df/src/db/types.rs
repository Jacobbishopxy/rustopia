//! types
//!
//! database typing conversion:
//! - [mysql](https://docs.rs/sqlx/0.5.7/sqlx/mysql/types/index.html)
//! - [postgres](https://docs.rs/sqlx/0.5.7/sqlx/postgres/types/index.html)
//! - [sqlite](https://docs.rs/sqlx/0.5.7/sqlx/sqlite/types/index.html)

use std::marker::PhantomData;

use chrono::{NaiveDate, NaiveDateTime, NaiveTime};
use rust_decimal::Decimal;
use sqlx::{mysql::MySqlRow, postgres::PgRow, sqlite::SqliteRow, Column, Row};

use crate::prelude::{DataType, DataframeData, D1};

/// SqlTypeTagMarker
///
/// 1. `fmt`: print sql type tag as `&str`
/// 1. `to_data_type`: convert sql type tag to tiny-df `DataType`
pub(crate) trait SqlTypeTagMarker {
    fn to_data_type(&self) -> DataType;
}

/// concrete struct `SqlTypeTag` (with generic type parameter, used for conjugating Rust real type)
#[derive(Debug)]
pub(crate) struct SqlTypeTag<'a, T>(&'a str, PhantomData<T>)
where
    T: Into<DataframeData>;

impl<'a, T> SqlTypeTag<'a, T>
where
    T: Into<DataframeData>,
{
    pub(crate) fn new(t: &'a str) -> Self {
        SqlTypeTag(t, PhantomData)
    }

    // pub(crate) fn type_tag(&self) -> &str {
    //     self.0
    // }
}

impl SqlTypeTagMarker for SqlTypeTag<'_, bool> {
    fn to_data_type(&self) -> DataType {
        DataType::Bool
    }
}

impl PartialEq<&str> for SqlTypeTag<'_, bool> {
    fn eq(&self, other: &&str) -> bool {
        &self.0 == other
    }
}

impl SqlTypeTagMarker for SqlTypeTag<'_, i8> {
    fn to_data_type(&self) -> DataType {
        DataType::Short
    }
}

impl PartialEq<&str> for SqlTypeTag<'_, i8> {
    fn eq(&self, other: &&str) -> bool {
        &self.0 == other
    }
}

impl SqlTypeTagMarker for SqlTypeTag<'_, i16> {
    fn to_data_type(&self) -> DataType {
        DataType::Short
    }
}

impl PartialEq<&str> for SqlTypeTag<'_, i16> {
    fn eq(&self, other: &&str) -> bool {
        &self.0 == other
    }
}

impl SqlTypeTagMarker for SqlTypeTag<'_, i32> {
    fn to_data_type(&self) -> DataType {
        DataType::Short
    }
}

impl PartialEq<&str> for SqlTypeTag<'_, i32> {
    fn eq(&self, other: &&str) -> bool {
        &self.0 == other
    }
}

impl SqlTypeTagMarker for SqlTypeTag<'_, i64> {
    fn to_data_type(&self) -> DataType {
        DataType::Short
    }
}

impl PartialEq<&str> for SqlTypeTag<'_, i64> {
    fn eq(&self, other: &&str) -> bool {
        &self.0 == other
    }
}

impl SqlTypeTagMarker for SqlTypeTag<'_, u8> {
    fn to_data_type(&self) -> DataType {
        DataType::Long
    }
}

impl PartialEq<&str> for SqlTypeTag<'_, u8> {
    fn eq(&self, other: &&str) -> bool {
        &self.0 == other
    }
}

impl SqlTypeTagMarker for SqlTypeTag<'_, u16> {
    fn to_data_type(&self) -> DataType {
        DataType::UShort
    }
}

impl PartialEq<&str> for SqlTypeTag<'_, u16> {
    fn eq(&self, other: &&str) -> bool {
        &self.0 == other
    }
}

impl SqlTypeTagMarker for SqlTypeTag<'_, u32> {
    fn to_data_type(&self) -> DataType {
        DataType::UShort
    }
}

impl PartialEq<&str> for SqlTypeTag<'_, u32> {
    fn eq(&self, other: &&str) -> bool {
        &self.0 == other
    }
}

impl SqlTypeTagMarker for SqlTypeTag<'_, u64> {
    fn to_data_type(&self) -> DataType {
        DataType::ULong
    }
}

impl PartialEq<&str> for SqlTypeTag<'_, u64> {
    fn eq(&self, other: &&str) -> bool {
        &self.0 == other
    }
}

impl SqlTypeTagMarker for SqlTypeTag<'_, f32> {
    fn to_data_type(&self) -> DataType {
        DataType::Float
    }
}

impl PartialEq<&str> for SqlTypeTag<'_, f32> {
    fn eq(&self, other: &&str) -> bool {
        &self.0 == other
    }
}

impl SqlTypeTagMarker for SqlTypeTag<'_, f64> {
    fn to_data_type(&self) -> DataType {
        DataType::Double
    }
}

impl PartialEq<&str> for SqlTypeTag<'_, f64> {
    fn eq(&self, other: &&str) -> bool {
        &self.0 == other
    }
}

impl SqlTypeTagMarker for SqlTypeTag<'_, String> {
    fn to_data_type(&self) -> DataType {
        DataType::String
    }
}

impl PartialEq<&str> for SqlTypeTag<'_, String> {
    fn eq(&self, other: &&str) -> bool {
        &self.0 == other
    }
}

impl SqlTypeTagMarker for SqlTypeTag<'_, NaiveDate> {
    fn to_data_type(&self) -> DataType {
        DataType::Date
    }
}

impl PartialEq<&str> for SqlTypeTag<'_, NaiveDate> {
    fn eq(&self, other: &&str) -> bool {
        &self.0 == other
    }
}

impl SqlTypeTagMarker for SqlTypeTag<'_, NaiveTime> {
    fn to_data_type(&self) -> DataType {
        DataType::Time
    }
}

impl PartialEq<&str> for SqlTypeTag<'_, NaiveTime> {
    fn eq(&self, other: &&str) -> bool {
        &self.0 == other
    }
}

impl SqlTypeTagMarker for SqlTypeTag<'_, NaiveDateTime> {
    fn to_data_type(&self) -> DataType {
        DataType::DateTime
    }
}

impl PartialEq<&str> for SqlTypeTag<'_, NaiveDateTime> {
    fn eq(&self, other: &&str) -> bool {
        &self.0 == other
    }
}

impl SqlTypeTagMarker for SqlTypeTag<'_, Decimal> {
    fn to_data_type(&self) -> DataType {
        DataType::Decimal
    }
}

impl PartialEq<&str> for SqlTypeTag<'_, Decimal> {
    fn eq(&self, other: &&str) -> bool {
        &self.0 == other
    }
}

#[test]
fn test_sqltype_eq() {
    let foo = "SHORT";
    let bar = SqlTypeTag::<i8>::new("SHORT");
    let qux = SqlTypeTag::<i64>::new("LONG");

    assert_eq!(bar, foo);
    assert_ne!(qux, foo);
}

fn get_mysql_type_tag(t: &str) -> Option<Box<dyn SqlTypeTagMarker>> {
    match &t.to_uppercase()[..] {
        "TINYINT(1)" => Some(Box::new(SqlTypeTag::<bool>::new("TINYINT(1)"))),
        "BOOLEAN" => Some(Box::new(SqlTypeTag::<bool>::new("BOOLEAN"))),
        "TINYINT" => Some(Box::new(SqlTypeTag::<i8>::new("TINYINT"))),
        "SMALLINT" => Some(Box::new(SqlTypeTag::<i16>::new("SMALLINT"))),
        "INT" => Some(Box::new(SqlTypeTag::<i32>::new("INT"))),
        "BIGINT" => Some(Box::new(SqlTypeTag::<i64>::new("BIGINT"))),
        "TINYINT UNSIGNED" => Some(Box::new(SqlTypeTag::<u8>::new("TINYINT UNSIGNED"))),
        "SMALLINT UNSIGNED" => Some(Box::new(SqlTypeTag::<u16>::new("SMALLINT UNSIGNED"))),
        "INT UNSIGNED" => Some(Box::new(SqlTypeTag::<u32>::new("INT UNSIGNED"))),
        "BIGINT UNSIGNED" => Some(Box::new(SqlTypeTag::<u64>::new("BIGINT UNSIGNED"))),
        "FLOAT" => Some(Box::new(SqlTypeTag::<f32>::new("FLOAT"))),
        "DOUBLE" => Some(Box::new(SqlTypeTag::<f64>::new("DOUBLE"))),
        "VARCHAR" => Some(Box::new(SqlTypeTag::<String>::new("VARCHAR"))),
        "CHAR" => Some(Box::new(SqlTypeTag::<String>::new("CHAR"))),
        "TEXT" => Some(Box::new(SqlTypeTag::<String>::new("TEXT"))),
        "TIMESTAMP" => Some(Box::new(SqlTypeTag::<NaiveDateTime>::new("TIMESTAMP"))),
        "DATETIME" => Some(Box::new(SqlTypeTag::<NaiveDateTime>::new("DATETIME"))),
        "DATE" => Some(Box::new(SqlTypeTag::<NaiveDate>::new("DATE"))),
        "TIME" => Some(Box::new(SqlTypeTag::<NaiveTime>::new("TIME"))),
        "DECIMAL" => Some(Box::new(SqlTypeTag::<Decimal>::new("DECIMAL"))),
        _ => None,
    }
}

fn get_pg_type_tag(t: &str) -> Option<Box<dyn SqlTypeTagMarker>> {
    match &t.to_uppercase()[..] {
        "BOOL" => Some(Box::new(SqlTypeTag::<bool>::new("BOOL"))),
        "CHAR" => Some(Box::new(SqlTypeTag::<i8>::new("CHAR"))),
        "SMALLINT" => Some(Box::new(SqlTypeTag::<i16>::new("SMALLINT"))),
        "SMALLSERIAL" => Some(Box::new(SqlTypeTag::<i16>::new("SMALLSERIAL"))),
        "INT2" => Some(Box::new(SqlTypeTag::<i16>::new("INT2"))),
        "INT" => Some(Box::new(SqlTypeTag::<i32>::new("INT"))),
        "SERIAL" => Some(Box::new(SqlTypeTag::<i32>::new("SERIAL"))),
        "INT4" => Some(Box::new(SqlTypeTag::<i32>::new("INT4"))),
        "BIGINT" => Some(Box::new(SqlTypeTag::<i64>::new("BIGINT"))),
        "BIGSERIAL" => Some(Box::new(SqlTypeTag::<i64>::new("BIGSERIAL"))),
        "INT8" => Some(Box::new(SqlTypeTag::<i64>::new("INT8"))),
        "REAL" => Some(Box::new(SqlTypeTag::<f32>::new("REAL"))),
        "FLOAT4" => Some(Box::new(SqlTypeTag::<f32>::new("FLOAT4"))),
        "DOUBLE PRECISION" => Some(Box::new(SqlTypeTag::<f64>::new("DOUBLE PRECISION"))),
        "FLOAT8" => Some(Box::new(SqlTypeTag::<f64>::new("FLOAT8"))),
        "VARCHAR" => Some(Box::new(SqlTypeTag::<String>::new("VARCHAR"))),
        "CHAR(N)" => Some(Box::new(SqlTypeTag::<String>::new("CHAR(N)"))),
        "TEXT" => Some(Box::new(SqlTypeTag::<String>::new("TEXT"))),
        "NAME" => Some(Box::new(SqlTypeTag::<String>::new("NAME"))),
        "TIMESTAMPTZ" => Some(Box::new(SqlTypeTag::<NaiveDateTime>::new("TIMESTAMPTZ"))),
        "TIMESTAMP" => Some(Box::new(SqlTypeTag::<NaiveDateTime>::new("TIMESTAMP"))),
        "DATE" => Some(Box::new(SqlTypeTag::<NaiveDate>::new("DATE"))),
        "TIME" => Some(Box::new(SqlTypeTag::<NaiveTime>::new("TIME"))),
        "NUMERIC" => Some(Box::new(SqlTypeTag::<Decimal>::new("NUMERIC"))),
        _ => None,
    }
}

fn get_sqlite_type_tag(t: &str) -> Option<Box<dyn SqlTypeTagMarker>> {
    match &t.to_uppercase()[..] {
        "BOOLEAN" => Some(Box::new(SqlTypeTag::<bool>::new("BOOLEAN"))),
        "INTEGER" => Some(Box::new(SqlTypeTag::<i32>::new("INTEGER"))),
        "BIGINT" => Some(Box::new(SqlTypeTag::<i64>::new("BIGINT"))),
        "INT8" => Some(Box::new(SqlTypeTag::<i64>::new("INT8"))),
        "REAL" => Some(Box::new(SqlTypeTag::<f64>::new("REAL"))),
        "VARCHAR" => Some(Box::new(SqlTypeTag::<String>::new("VARCHAR"))),
        "TEXT" => Some(Box::new(SqlTypeTag::<String>::new("TEXT"))),
        "DATETIME" => Some(Box::new(SqlTypeTag::<NaiveDateTime>::new("DATETIME"))),
        _ => None,
    }
}

#[test]
fn test_get_sql_type_tag() {
    let foo = get_mysql_type_tag("BOOLEAN").unwrap();
    let bar = foo.to_data_type().try_to_df_data(true).unwrap();

    assert_eq!(bar, DataframeData::Bool(true));

    let foo = get_sqlite_type_tag("REAL").unwrap();
    let bar = foo.to_data_type().try_to_df_data(19.1).unwrap();

    assert_eq!(bar, DataframeData::Double(19.1));
}

/// enum used for getting a `DataType` from a `&str`
pub(crate) enum SqlColumnType<'a> {
    Mysql(&'a str),
    Postgres(&'a str),
    Sqlite(&'a str),
}

impl<'a> SqlColumnType<'a> {
    pub(crate) fn new(t: &'a str, ty: &'a str) -> Self {
        match ty {
            "m" => SqlColumnType::Mysql(t),
            "p" => SqlColumnType::Postgres(t),
            "s" => SqlColumnType::Sqlite(t),
            _ => SqlColumnType::Sqlite(t),
        }
    }

    pub(crate) fn to_datatype(&self) -> DataType {
        match self {
            SqlColumnType::Mysql(v) => match get_mysql_type_tag(v) {
                Some(s) => s.to_data_type(),
                None => DataType::None,
            },
            SqlColumnType::Postgres(v) => match get_pg_type_tag(v) {
                Some(s) => s.to_data_type(),
                None => DataType::None,
            },
            SqlColumnType::Sqlite(v) => match get_sqlite_type_tag(v) {
                Some(s) => s.to_data_type(),
                None => DataType::None,
            },
        }
    }
}

/// macro used to handle raw sql row conversion
macro_rules! res_push {
    ($row:expr, $res:expr, $idx:expr; $cvt:ty) => {{
        let v: Option<$cvt> = $row.try_get($idx)?;
        match v {
            Some(r) => $res.push(r.into()),
            None => $res.push(DataframeData::None),
        }
    }};
}

pub(crate) fn row_cols_name_mysql(row: &MySqlRow) -> D1 {
    row.columns()
        .iter()
        .map(|c| DataframeData::String(c.name().to_owned()))
        .collect()
}

pub(crate) fn row_to_d1_mysql(row: MySqlRow) -> Result<D1, sqlx::Error> {
    let mut res = vec![];
    let len = row.columns().len();

    for i in 0..len {
        let type_name = row.column(i).type_info().to_string();

        match type_name {
            s if ["TINYINT(1)", "BOOLEAN"].contains(&&s[..]) => {
                res_push!(row, res, i; bool);
            }
            s if s == "TINYINT" => {
                res_push!(row, res, i; i8);
            }
            s if s == "SMALLINT" => {
                res_push!(row, res, i; i16);
            }
            s if s == "INT" => {
                res_push!(row, res, i; i32);
            }
            s if s == "BIGINT" => {
                res_push!(row, res, i; i64);
            }
            s if s == "TINYINT UNSIGNED" => {
                res_push!(row, res, i; u8);
            }
            s if s == "SMALLINT UNSIGNED" => {
                res_push!(row, res, i; u16);
            }
            s if s == "INT UNSIGNED" => {
                res_push!(row, res, i; u32);
            }
            s if s == "BIGINT UNSIGNED" => {
                res_push!(row, res, i; u64);
            }
            s if s == "FLOAT" => {
                res_push!(row, res, i; f32);
            }
            s if s == "DOUBLE" => {
                res_push!(row, res, i; f64);
            }
            s if ["VARCHAR", "CHAR", "TEXT"].contains(&&s[..]) => {
                res_push!(row, res, i; String);
            }
            s if ["TIMESTAMP", "DATETIME"].contains(&&s[..]) => {
                res_push!(row, res, i; NaiveDateTime);
            }
            s if s == "DATE" => {
                res_push!(row, res, i; NaiveDate);
            }
            s if s == "TIME" => {
                res_push!(row, res, i; NaiveTime);
            }
            s if s == "DECIMAL" => {
                res_push!(row, res, i; Decimal);
            }
            _ => {
                res.push(DataframeData::None);
            }
        }
    }

    Ok(res)
}

pub(crate) fn row_cols_name_pg(row: &PgRow) -> D1 {
    row.columns()
        .iter()
        .map(|c| DataframeData::String(c.name().to_owned()))
        .collect()
}

pub(crate) fn row_to_d1_pg(row: PgRow) -> Result<D1, sqlx::Error> {
    let mut res = vec![];
    let len = row.columns().len();

    for i in 0..len {
        let type_name = row.column(i).type_info().to_string();

        match type_name {
            s if s == "BOOL" => {
                res_push!(row, res, i; bool);
            }
            s if s == "CHAR" => {
                res_push!(row, res, i; i8);
            }
            s if ["SMALLINT", "SMALLSERIAL", "INT2"].contains(&&s[..]) => {
                res_push!(row, res, i; i16);
            }
            s if ["INT", "SERIAL", "INT4"].contains(&&s[..]) => {
                res_push!(row, res, i; i32);
            }
            s if ["BIGINT", "BIGSERIAL", "INT8"].contains(&&s[..]) => {
                res_push!(row, res, i; i64);
            }
            s if ["REAL", "FLOAT4"].contains(&&s[..]) => {
                res_push!(row, res, i; f32);
            }
            s if ["DOUBLE PRECISION", "FLOAT8"].contains(&&s[..]) => {
                res_push!(row, res, i; f64);
            }
            s if ["VARCHAR", "CHAR(N)", "TEXT", "NAME"].contains(&&s[..]) => {
                res_push!(row, res, i; String);
            }
            s if ["TIMESTAMPTZ", "TIMESTAMP"].contains(&&s[..]) => {
                res_push!(row, res, i; NaiveDateTime);
            }
            s if s == "DATE" => {
                res_push!(row, res, i; NaiveDate);
            }
            s if s == "TIME" => {
                res_push!(row, res, i; NaiveTime);
            }
            s if s == "NUMERIC" => {
                res_push!(row, res, i; Decimal);
            }
            _ => {
                res.push(DataframeData::None);
            }
        }
    }

    Ok(res)
}

pub(crate) fn row_cols_name_sqlite(row: &SqliteRow) -> D1 {
    row.columns()
        .iter()
        .map(|c| DataframeData::String(c.name().to_owned()))
        .collect()
}

pub(crate) fn row_to_d1_sqlite(row: SqliteRow) -> Result<D1, sqlx::Error> {
    let mut res = vec![];
    let len = row.columns().len();

    for i in 0..len {
        let type_name = row.column(i).type_info().to_string();

        match type_name {
            s if s == "BOOLEAN" => {
                res_push!(row, res, i; bool);
            }
            s if s == "INTEGER" => {
                res_push!(row, res, i; i32);
            }
            s if ["BIGINT", "INT8"].contains(&&s[..]) => {
                res_push!(row, res, i; i64);
            }
            s if s == "REAL" => {
                res_push!(row, res, i; f64);
            }
            s if s == "VARCHAR" => {
                res_push!(row, res, i; String);
            }
            s if s == "TEXT" => {
                res_push!(row, res, i; String);
            }
            s if s == "DATETIME" => {
                res_push!(row, res, i; NaiveDateTime);
            }
            _ => {
                res.push(DataframeData::None);
            }
        }
    }

    Ok(res)
}
