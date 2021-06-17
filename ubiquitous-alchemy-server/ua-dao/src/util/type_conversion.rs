use std::collections::HashMap;

use sqlx::{mysql::MySqlRow, postgres::PgRow, Column, Row};

use super::general::DataEnum;

/// temporary workaround for converting Database value to domain structure
pub fn row_to_map_mysql(
    row: MySqlRow,
    columns: &Vec<String>,
) -> Result<HashMap<String, DataEnum>, sqlx::Error> {
    let mut res = HashMap::new();

    for (i, k) in columns.iter().enumerate() {
        let type_name = row.column(i).type_info().to_string();

        match type_name {
            s if ["TINYINT(1)", "BOOLEAN"].contains(&&s[..]) => {
                res.insert(k.to_owned(), DataEnum::Bool(row.try_get(i)?));
            }
            s if s == "TINYINT" => {
                let v: i8 = row.try_get(i)?;
                res.insert(k.to_owned(), DataEnum::Integer(v as i64));
            }
            s if s == "SMALLINT" => {
                let v: i16 = row.try_get(i)?;
                res.insert(k.to_owned(), DataEnum::Integer(v as i64));
            }
            s if s == "INT" => {
                let v: i32 = row.try_get(i)?;
                res.insert(k.to_owned(), DataEnum::Integer(v as i64));
            }
            s if s == "BIGINT" => {
                res.insert(k.to_owned(), DataEnum::Integer(row.try_get(i)?));
            }
            s if s == "TINYINT UNSIGNED" => {
                let v: u8 = row.try_get(i)?;
                res.insert(k.to_owned(), DataEnum::Integer(v as i64));
            }
            s if s == "SMALLINT UNSIGNED" => {
                let v: u16 = row.try_get(i)?;
                res.insert(k.to_owned(), DataEnum::Integer(v as i64));
            }
            s if s == "INT UNSIGNED" => {
                let v: u32 = row.try_get(i)?;
                res.insert(k.to_owned(), DataEnum::Integer(v as i64));
            }
            s if s == "BIGINT UNSIGNED" => {
                let v: u64 = row.try_get(i)?;
                res.insert(k.to_owned(), DataEnum::Integer(v as i64));
            }
            s if s == "FLOAT" => {
                let v: f32 = row.try_get(i)?;
                res.insert(k.to_owned(), DataEnum::Float(v as f64));
            }
            s if s == "DOUBLE" => {
                let v: f64 = row.try_get(i)?;
                res.insert(k.to_owned(), DataEnum::Float(v as f64));
            }
            s if ["VARCHAR", "CHAR", "TEXT"].contains(&&s[..]) => {
                res.insert(k.to_owned(), DataEnum::String(row.try_get(i)?));
            }
            _ => {
                res.insert(k.to_owned(), DataEnum::Null);
            }
        }
    }

    Ok(res)
}

/// temporary workaround for converting Database value to domain structure
pub fn row_to_map_postgres(
    row: PgRow,
    columns: &Vec<String>,
) -> Result<HashMap<String, DataEnum>, sqlx::Error> {
    let mut res = HashMap::new();

    for (i, k) in columns.iter().enumerate() {
        let type_name = row.column(i).type_info().to_string();

        match type_name {
            s if s == "BOOL" => {
                res.insert(k.to_owned(), DataEnum::Bool(row.try_get(i)?));
            }
            s if s == "CHAR" => {
                let v: i8 = row.try_get(i)?;
                res.insert(k.to_owned(), DataEnum::Integer(v as i64));
            }
            s if ["SMALLINT", "SMALLSERIAL", "INT2"].contains(&&s[..]) => {
                let v: i16 = row.try_get(i)?;
                res.insert(k.to_owned(), DataEnum::Integer(v as i64));
            }
            s if ["INT", "SERIAL", "INT4"].contains(&&s[..]) => {
                let v: i32 = row.try_get(i)?;
                res.insert(k.to_owned(), DataEnum::Integer(v as i64));
            }
            s if ["BIGINT", "BIGSERIAL", "INT8"].contains(&&s[..]) => {
                res.insert(k.to_owned(), DataEnum::Integer(row.try_get(i)?));
            }
            s if ["REAL", "FLOAT4"].contains(&&s[..]) => {
                let v: f32 = row.try_get(i)?;
                res.insert(k.to_owned(), DataEnum::Float(v as f64));
            }
            s if ["DOUBLE PRECISION", "FLOAT8"].contains(&&s[..]) => {
                res.insert(k.to_owned(), DataEnum::Float(row.try_get(i)?));
            }
            s if ["VARCHAR", "CHAR(N)", "TEXT", "NAME"].contains(&&s[..]) => {
                res.insert(k.to_owned(), DataEnum::String(row.try_get(i)?));
            }
            _ => {
                res.insert(k.to_owned(), DataEnum::Null);
            }
        }
    }

    Ok(res)
}
