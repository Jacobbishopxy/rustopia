//! types
//!
//! database typing conversion:
//! - [mysql](https://docs.rs/sqlx/0.5.7/sqlx/mysql/types/index.html)
//! - [postgres](https://docs.rs/sqlx/0.5.7/sqlx/postgres/types/index.html)
//! - [sqlite](https://docs.rs/sqlx/0.5.7/sqlx/sqlite/types/index.html)

use chrono::{NaiveDate, NaiveDateTime, NaiveTime};
use sqlx::{mysql::MySqlRow, postgres::PgRow, sqlite::SqliteRow, Column, Row};

use crate::prelude::{DataframeData, D1};

/// macro used to handle raw sql row conversion
macro_rules! res_push {
    ($row:expr, $res:expr, $idx:expr; $cvt:ty => $dfd:path) => {{
        let v: Option<$cvt> = $row.try_get($idx)?;
        match v {
            Some(v) => $res.push($dfd(v.into())),
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

        match type_name {
            s if ["TINYINT(1)", "BOOLEAN"].contains(&&s[..]) => {
                res_push!(row, res, i; bool => DataframeData::Bool);
            }
            s if s == "TINYINT" => {
                res_push!(row, res, i; i8 => DataframeData::Short);
            }
            s if s == "SMALLINT" => {
                res_push!(row, res, i; i16 => DataframeData::Short);
            }
            s if s == "INT" => {
                res_push!(row, res, i; i32 => DataframeData::Short);
            }
            s if s == "BIGINT" => {
                res_push!(row, res, i; i64 => DataframeData::Long);
            }
            s if s == "TINYINT UNSIGNED" => {
                res_push!(row, res, i; u8 => DataframeData::Short);
            }
            s if s == "SMALLINT UNSIGNED" => {
                res_push!(row, res, i; u16 => DataframeData::Short);
            }
            s if s == "INT UNSIGNED" => {
                res_push!(row, res, i; u32 => DataframeData::UShort);
            }
            s if s == "BIGINT UNSIGNED" => {
                res_push!(row, res, i; u64 => DataframeData::ULong);
            }
            s if s == "FLOAT" => {
                res_push!(row, res, i; f32 => DataframeData::Float);
            }
            s if s == "DOUBLE" => {
                res_push!(row, res, i; f64 => DataframeData::Double);
            }
            s if ["VARCHAR", "CHAR", "TEXT"].contains(&&s[..]) => {
                res_push!(row, res, i; String => DataframeData::String);
            }
            s if ["TIMESTAMP", "DATETIME"].contains(&&s[..]) => {
                res_push!(row, res, i; NaiveDateTime => DataframeData::DateTime);
            }
            s if s == "DATE" => {
                res_push!(row, res, i; NaiveDate => DataframeData::Date);
            }
            s if s == "TIME" => {
                res_push!(row, res, i; NaiveTime => DataframeData::Time);
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
                res_push!(row, res, i; bool => DataframeData::Bool);
            }
            s if s == "CHAR" => {
                res_push!(row, res, i; i8 => DataframeData::Short);
            }
            s if ["SMALLINT", "SMALLSERIAL", "INT2"].contains(&&s[..]) => {
                res_push!(row, res, i; i16 => DataframeData::Short);
            }
            s if ["INT", "SERIAL", "INT4"].contains(&&s[..]) => {
                res_push!(row, res, i; i32 => DataframeData::Short);
            }
            s if ["BIGINT", "BIGSERIAL", "INT8"].contains(&&s[..]) => {
                res_push!(row, res, i; i64 => DataframeData::Long);
            }
            s if ["REAL", "FLOAT4"].contains(&&s[..]) => {
                res_push!(row, res, i; f32 => DataframeData::Float);
            }
            s if ["DOUBLE PRECISION", "FLOAT8"].contains(&&s[..]) => {
                res_push!(row, res, i; f64 => DataframeData::Double);
            }
            s if ["VARCHAR", "CHAR(N)", "TEXT", "NAME"].contains(&&s[..]) => {
                res_push!(row, res, i; String => DataframeData::String);
            }
            s if ["TIMESTAMPTZ", "TIMESTAMP"].contains(&&s[..]) => {
                res_push!(row, res, i; NaiveDateTime => DataframeData::DateTime);
            }
            s if s == "DATE" => {
                res_push!(row, res, i; NaiveDate => DataframeData::Date);
            }
            s if s == "TIME" => {
                res_push!(row, res, i; NaiveTime => DataframeData::Time);
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
                res_push!(row, res, i; bool => DataframeData::Bool);
            }
            s if s == "INTEGER" => {
                res_push!(row, res, i; i32 => DataframeData::Short);
            }
            s if ["BIGINT", "INT8"].contains(&&s[..]) => {
                res_push!(row, res, i; i64 => DataframeData::Long);
            }
            s if s == "REAL" => {
                res_push!(row, res, i; f64 => DataframeData::Double);
            }
            s if s == "TEXT" => {
                res_push!(row, res, i; String => DataframeData::String);
            }
            s if s == "DATETIME" => {
                res_push!(row, res, i; NaiveDateTime => DataframeData::DateTime);
            }
            _ => {
                res.push(DataframeData::None);
            }
        }
    }

    Ok(res)
}
