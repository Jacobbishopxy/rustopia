//! Sql row processor

use itertools::Itertools;
use sqlx::{Column, Row as SRow};

use super::types::{SqlRow, SqlTypeTagMarker, MYSQL_TMAP, PG_TMAP, SQLITE_TMAP};
use crate::{FabrixResult, Row, Value};

/// SqlRowProcessor is the core struct for processing different types of SqlRow
pub(crate) struct SqlRowProcessor {
    cache: Option<Vec<Option<&'static Box<dyn SqlTypeTagMarker>>>>,
}

impl SqlRowProcessor {
    pub(crate) fn new() -> Self {
        SqlRowProcessor { cache: None }
    }

    fn caching(&mut self, sql_row: &SqlRow) {
        if let None = self.cache {
            match sql_row {
                SqlRow::Mysql(row) => {
                    let ct = row
                        .columns()
                        .iter()
                        .map(|c| {
                            let t = c.type_info().to_string();
                            MYSQL_TMAP.get(&t[..])
                        })
                        .collect_vec();
                    self.cache = Some(ct);
                }
                SqlRow::Pg(row) => {
                    let ct = row
                        .columns()
                        .iter()
                        .map(|c| {
                            let t = c.type_info().to_string();
                            PG_TMAP.get(&t[..])
                        })
                        .collect_vec();
                    self.cache = Some(ct);
                }
                SqlRow::Sqlite(row) => {
                    let ct = row
                        .columns()
                        .iter()
                        .map(|c| {
                            let t = c.type_info().to_string();
                            SQLITE_TMAP.get(&t[..])
                        })
                        .collect_vec();
                    self.cache = Some(ct);
                }
            }
        }
    }

    pub(crate) fn process<'a, T>(&mut self, sql_row: T) -> FabrixResult<Vec<Value>>
    where
        T: Into<SqlRow<'a>>,
    {
        let sql_row: SqlRow = sql_row.into();
        self.caching(&sql_row);
        let mut res = Vec::with_capacity(sql_row.len());

        for (idx, c) in self.cache.as_ref().unwrap().iter().enumerate() {
            match c {
                Some(m) => {
                    res.push(m.extract_value(&sql_row, idx)?);
                }
                None => {
                    res.push(Value::Null);
                }
            }
        }

        Ok(res)
    }

    pub(crate) fn process_to_row<'a, T>(&mut self, sql_row: T) -> FabrixResult<Row>
    where
        T: Into<SqlRow<'a>>,
    {
        todo!()
    }
}
