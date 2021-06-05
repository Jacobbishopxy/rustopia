use std::collections::HashMap;

use sqlx::{postgres::PgRow, Column, Row};

use super::general::DataEnum;

// todo: Error handling

/// temporary workaround for converting Database value to domain structure
pub fn row_to_map(row: PgRow, columns: &Vec<String>) -> HashMap<String, DataEnum> {
    let mut res = HashMap::new();

    for (i, k) in columns.iter().enumerate() {
        let type_name = row.column(i).type_info().to_string();

        match type_name {
            s if s == "BOOL" => {
                res.insert(k.to_owned(), DataEnum::Bool(row.get(i)));
            }
            s if s == "CHAR" => {
                let v: i8 = row.get(i);
                res.insert(k.to_owned(), DataEnum::Integer(v as i64));
            }
            s if ["SMALLINT", "SMALLSERIAL", "INT2"].contains(&&s[..]) => {
                let v: i16 = row.get(i);
                res.insert(k.to_owned(), DataEnum::Integer(v as i64));
            }
            s if ["INT", "SERIAL", "INT4"].contains(&&s[..]) => {
                let v: i32 = row.get(i);
                res.insert(k.to_owned(), DataEnum::Integer(v as i64));
            }
            s if ["BIGINT", "BIGSERIAL", "INT8"].contains(&&s[..]) => {
                res.insert(k.to_owned(), DataEnum::Integer(row.get(i)));
            }
            s if ["REAL", "FLOAT4"].contains(&&s[..]) => {
                let v: f32 = row.get(i);
                res.insert(k.to_owned(), DataEnum::Float(v as f64));
            }
            s if ["DOUBLE PRECISION", "FLOAT8"].contains(&&s[..]) => {
                res.insert(k.to_owned(), DataEnum::Float(row.get(i)));
            }
            s if ["VARCHAR", "CHAR(N)", "TEXT", "NAME"].contains(&&s[..]) => {
                res.insert(k.to_owned(), DataEnum::String(row.get(i)));
            }
            _ => {
                res.insert(k.to_owned(), DataEnum::Null);
            }
        }
    }

    res
}
