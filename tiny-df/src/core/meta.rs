//! Dataframe base elements
//!
//! - DataType
//! - DataframeData
//! - DataDirection
//! - DataframeColDef

use std::fmt::Display;

use chrono::{NaiveDate, NaiveDateTime, NaiveTime};
use serde::{Deserialize, Serialize};

/// Series
pub type Series = Vec<DataframeData>;
/// DF
pub type DF = Vec<Series>;
/// DataframeIndex
pub type DataframeIndex = DataframeData;

/// datatype
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
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

impl Default for DataType {
    fn default() -> Self {
        DataType::None
    }
}

/// dataframe data
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
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

// Custom `to_string` method
impl Display for DataframeData {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DataframeData::Id(v) => write!(f, "{}", v),
            DataframeData::Bool(v) => write!(f, "{}", v),
            DataframeData::Short(v) => write!(f, "{}", v),
            DataframeData::Long(v) => write!(f, "{}", v),
            DataframeData::Float(v) => write!(f, "{}", v),
            DataframeData::Double(v) => write!(f, "{}", v),
            DataframeData::String(v) => write!(f, "{}", v),
            DataframeData::Date(v) => write!(f, "{}", v),
            DataframeData::Time(v) => write!(f, "{}", v),
            DataframeData::DateTime(v) => write!(f, "{}", v),
            DataframeData::Error => write!(f, "error"),
            DataframeData::None => write!(f, "null"),
        }
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

/// direction of storing data
#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum DataOrientation {
    Horizontal,
    Vertical,
    Raw,
}

impl Default for DataOrientation {
    fn default() -> Self {
        Self::Raw
    }
}

impl From<&str> for DataOrientation {
    fn from(v: &str) -> Self {
        match &v.to_lowercase()[..] {
            "horizontal" | "h" => DataOrientation::Horizontal,
            "vertical" | "v" => DataOrientation::Vertical,
            _ => DataOrientation::Raw,
        }
    }
}

/// A dataframe columns definition
#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct DataframeColumn {
    pub name: String,
    pub col_type: DataType,
}

impl DataframeColumn {
    // constructor
    pub fn new<T>(name: T, col_type: DataType) -> Self
    where
        T: Into<String>,
    {
        DataframeColumn {
            name: name.into(),
            col_type,
        }
    }
}
