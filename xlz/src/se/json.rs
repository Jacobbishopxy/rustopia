use chrono::{NaiveDate, NaiveDateTime, NaiveTime};
use serde::Serialize;

#[derive(Debug, Serialize)]
pub enum XValue {
    Bool(bool),
    Date(NaiveDate),
    DateTime(NaiveDateTime),
    Error(String),
    None,
    Number(f64),
    String(String),
    Time(NaiveTime),
}
