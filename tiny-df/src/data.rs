//! Dataframe base elements
//!
//! - DataType
//! - DataframeData
//! - DataDirection
//! - DataframeColDef

use std::fmt::Display;

use chrono::{NaiveDate, NaiveDateTime, NaiveTime};
use serde::Serialize;

/// datatype
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DataType {
    Id,
    Bool,
    Short,
    Long,
    Float,
    Double,
    String,
    Date,
    Time,
    DateTime,
    Error,
    None,
}

/// dataframe data
#[derive(Debug, Clone, Serialize)]
#[serde(untagged)]
pub enum DataframeData {
    Id(u64),
    Bool(bool),
    Short(i32),
    Long(i64),
    Float(f32),
    Double(f64),
    String(String),
    Date(NaiveDate),
    Time(NaiveTime),
    DateTime(NaiveDateTime),
    Error,
    None,
}

impl DataframeData {
    pub fn as_ref(&mut self) -> &Self {
        self
    }
}

impl From<&DataframeData> for DataType {
    fn from(d: &DataframeData) -> Self {
        match d {
            DataframeData::Id(_) => DataType::Id,
            DataframeData::Bool(_) => DataType::Bool,
            DataframeData::Short(_) => DataType::Short,
            DataframeData::Long(_) => DataType::Long,
            DataframeData::Float(_) => DataType::Float,
            DataframeData::Double(_) => DataType::Double,
            DataframeData::String(_) => DataType::String,
            DataframeData::Date(_) => DataType::Date,
            DataframeData::Time(_) => DataType::Time,
            DataframeData::DateTime(_) => DataType::DateTime,
            DataframeData::Error => DataType::Error,
            DataframeData::None => DataType::None,
        }
    }
}

impl Display for DataframeData {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Debug::fmt(&self, f)
    }
}

impl From<bool> for DataframeData {
    fn from(v: bool) -> Self {
        DataframeData::Bool(v)
    }
}

impl From<i32> for DataframeData {
    fn from(v: i32) -> Self {
        DataframeData::Short(v)
    }
}

impl From<u32> for DataframeData {
    fn from(v: u32) -> Self {
        DataframeData::Short(v as i32)
    }
}

impl From<i64> for DataframeData {
    fn from(v: i64) -> Self {
        DataframeData::Long(v)
    }
}

impl From<u64> for DataframeData {
    fn from(v: u64) -> Self {
        DataframeData::Long(v as i64)
    }
}

impl From<f32> for DataframeData {
    fn from(v: f32) -> Self {
        DataframeData::Float(v)
    }
}

impl From<f64> for DataframeData {
    fn from(v: f64) -> Self {
        DataframeData::Double(v)
    }
}

impl From<String> for DataframeData {
    fn from(v: String) -> Self {
        DataframeData::String(v)
    }
}

impl From<&str> for DataframeData {
    fn from(v: &str) -> Self {
        DataframeData::String(v.to_owned())
    }
}

impl From<NaiveDate> for DataframeData {
    fn from(v: NaiveDate) -> Self {
        DataframeData::Date(v)
    }
}

impl From<NaiveTime> for DataframeData {
    fn from(v: NaiveTime) -> Self {
        DataframeData::Time(v)
    }
}

impl From<NaiveDateTime> for DataframeData {
    fn from(v: NaiveDateTime) -> Self {
        DataframeData::DateTime(v)
    }
}

pub type Series = Vec<DataframeData>;
pub type DF = Vec<Series>;

/// direction of storing data
#[derive(Debug, PartialEq, Eq)]
pub enum DataDirection {
    Horizontal,
    Vertical,
    None,
}

impl Default for DataDirection {
    fn default() -> Self {
        Self::None
    }
}

impl From<&str> for DataDirection {
    fn from(v: &str) -> Self {
        match &v.to_lowercase()[..] {
            "horizontal" | "h" => DataDirection::Horizontal,
            "vertical" | "v" => DataDirection::Vertical,
            _ => DataDirection::None,
        }
    }
}

/// A dataframe columns definition
#[derive(Debug, Clone)]
pub struct DataframeColDef {
    pub name: String,
    pub col_type: DataType,
}

impl DataframeColDef {
    pub fn new<T>(name: T, col_type: DataType) -> Self
    where
        T: Into<String>,
    {
        DataframeColDef {
            name: name.into(),
            col_type,
        }
    }
}
