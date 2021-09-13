//! types
//!
//! database typing conversion:
//! - [mysql](https://docs.rs/sqlx/0.5.7/sqlx/mysql/types/index.html)
//! - [postgres](https://docs.rs/sqlx/0.5.7/sqlx/postgres/types/index.html)
//! - [sqlite](https://docs.rs/sqlx/0.5.7/sqlx/sqlite/types/index.html)

use chrono::{NaiveDate, NaiveDateTime, NaiveTime};
use rust_decimal::Decimal;
use sqlx::{mysql::MySqlRow, postgres::PgRow, sqlite::SqliteRow, Column, Row};

use crate::prelude::{DataType, DataframeData, D1};

/// enum used for getting a `DataType` from a `&str`
pub(crate) enum SqlColumnType<'a> {
    Mysql(&'a str),
    Postgres(&'a str),
    Sqlite(&'a str),
}

impl<'a> From<SqlColumnType<'a>> for DataType {
    fn from(v: SqlColumnType<'a>) -> Self {
        match v {
            SqlColumnType::Mysql(t) => match &t.to_uppercase()[..] {
                "TINYINT(1)" => DataType::Bool,
                "BOOLEAN" => DataType::Bool,
                "TINYINT" => DataType::Short,
                "SMALLINT" => DataType::Short,
                "INT" => DataType::Short,
                "BIGINT" => DataType::Long,
                "TINYINT UNSIGNED" => DataType::UShort,
                "SMALLINT UNSIGNED" => DataType::UShort,
                "INT UNSIGNED" => DataType::UShort,
                "BIGINT UNSIGNED" => DataType::ULong,
                "FLOAT" => DataType::Float,
                "DOUBLE" => DataType::Double,
                "VARCHAR" => DataType::String,
                "CHAR" => DataType::String,
                "TEXT" => DataType::String,
                "TIMESTAMP" => DataType::DateTime,
                "DATETIME" => DataType::DateTime,
                "DATE" => DataType::Date,
                "TIME" => DataType::Time,
                "DECIMAL" => DataType::Decimal,
                _ => DataType::None,
            },
            SqlColumnType::Postgres(t) => match &t.to_uppercase()[..] {
                "BOOL" => DataType::Bool,
                "CHAR" => DataType::Short,
                "SMALLINT" => DataType::Short,
                "SMALLSERIAL" => DataType::Short,
                "INT2" => DataType::Short,
                "INT" => DataType::Short,
                "SERIAL" => DataType::Short,
                "INT4" => DataType::Short,
                "BIGINT" => DataType::Long,
                "BIGSERIAL" => DataType::Long,
                "INT8" => DataType::Long,
                "REAL" => DataType::Float,
                "FLOAT4" => DataType::Float,
                "DOUBLE PRECISION" => DataType::Double,
                "FLOAT8" => DataType::Double,
                "VARCHAR" => DataType::String,
                "CHAR(N)" => DataType::String,
                "TEXT" => DataType::String,
                "NAME" => DataType::String,
                "TIMESTAMPTZ" => DataType::DateTime,
                "TIMESTAMP" => DataType::DateTime,
                "DATE" => DataType::Date,
                "TIME" => DataType::Time,
                "NUMERIC" => DataType::Decimal,
                _ => DataType::None,
            },
            SqlColumnType::Sqlite(t) => match &t.to_uppercase()[..] {
                "BOOLEAN" => DataType::Bool,
                "INTEGER" => DataType::Short,
                "BIGINT" => DataType::Long,
                "INT8" => DataType::Long,
                "REAL" => DataType::Double,
                "VARCHAR" => DataType::String,
                "TEXT" => DataType::String,
                "DATETIME" => DataType::DateTime,
                _ => DataType::None,
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

// TODO: `row_cols_name_xxx` when data is empty, then row is empty, hence no `D1` for column name
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

        // TODO: is it possible to be simplified? &str + type(from db) + T(value) -> DataframeData
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
