//! A row based Dataframe structure

use std::{fmt::Display, mem};

use chrono::{NaiveDate, NaiveDateTime, NaiveTime};
use serde::Serialize;

/// datatype
#[derive(Debug, PartialEq, Eq)]
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
    fn as_ref(&mut self) -> &Self {
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

pub type DataframeRow = Vec<DataframeData>;

/// direction of storing data
#[derive(Debug)]
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

/// A dataframe column definition
#[derive(Debug)]
pub struct DataframeColDef {
    pub name: String,
    pub col_type: DataType,
}

/// Dataframe
/// Core struct of this lib crate
#[derive(Debug, Default)]
pub struct Dataframe {
    pub data: Vec<DataframeRow>,
    column: Vec<DataframeColDef>,
    data_direction: DataDirection,
    size: (usize, usize),
}

impl Dataframe {
    /// New dataframe if data_direction is none
    fn new_dataframe_n_dir(data: Vec<DataframeRow>) -> Self {
        Dataframe {
            data,
            ..Default::default()
        }
    }

    /// New dataframe if data_direction is horizontal
    // TODO: extract columns defining operation
    fn new_dataframe_h_dir(data: Vec<DataframeRow>) -> Self {
        if Dataframe::is_empty(&data) {
            return Dataframe::default();
        }

        let mut data_iter = data.into_iter();

        let column_name = data_iter
            .next()
            .unwrap()
            .into_iter()
            .map(|d| d.to_string())
            .collect::<Vec<String>>();

        let length_of_a_row = column_name.len();

        let mut column_type: Vec<DataType> = Vec::new();
        let mut res = Vec::new();
        let mut row0 = Vec::new();

        match data_iter.next() {
            Some(vd) => {
                for (i, d) in vd.into_iter().enumerate() {
                    column_type.push((&d).into());
                    row0.push(d);

                    if i == length_of_a_row - 1 {
                        break;
                    }
                }
            }
            None => return Dataframe::default(),
        }

        res.push(row0);

        for mut d in data_iter {
            let mut buf = Vec::new();
            for i in 0..length_of_a_row {
                match d.get_mut(i) {
                    Some(v) => {
                        let mut tmp = DataframeData::None;
                        let v_type: DataType = v.as_ref().into();
                        if column_type.get(i).unwrap() == &v_type {
                            mem::swap(&mut tmp, v);
                        }
                        buf.push(tmp);
                    }
                    None => buf.push(DataframeData::None),
                }
            }
            res.push(buf);
        }

        let length_of_res = res.len();

        Dataframe {
            data: res,
            column: column_name
                .into_iter()
                .zip(column_type.into_iter())
                .map(|(name, col_type)| DataframeColDef { name, col_type })
                .collect(),
            data_direction: DataDirection::Horizontal,
            size: (length_of_res, length_of_a_row),
        }
    }

    /// New dataframe if data_direction is horizontal
    fn new_dataframe_v_dir(data: Vec<DataframeRow>) -> Self {
        // TODO:
        // 1. each row's 1st cell is its column name, and 2nd cell determines column type
        // 2. check the remaining data of the row, if unmatched set error
        todo!()
    }

    /// Dataframe constructor
    /// Accepting tree kinds of data:
    /// 1. in horizontal direction, columns name is the first row
    /// 2. in vertical direction, columns name is the first column
    /// 3. none direction, raw data
    pub fn new(data: Vec<DataframeRow>, data_direction: DataDirection) -> Self {
        match data_direction {
            DataDirection::Horizontal => Dataframe::new_dataframe_h_dir(data),
            DataDirection::Vertical => Dataframe::new_dataframe_v_dir(data),
            DataDirection::None => Dataframe::new_dataframe_n_dir(data),
        }
    }

    /// Dataframe constructor
    /// From a 2d vector
    pub fn from_2d_vec(
        data: Vec<DataframeRow>,
        data_direction: DataDirection,
        columns: Vec<DataframeColDef>,
    ) -> Self {
        todo!()
    }

    pub fn is_empty(data: &Vec<DataframeRow>) -> bool {
        if data.is_empty() {
            true
        } else {
            data[0].is_empty()
        }
    }

    pub fn size(&self) -> (usize, usize) {
        self.size
    }

    pub fn append(&mut self, data: DataframeRow) {
        // TODO:
        // 1. data direction
        // 2. check type, if unmatched set error
        todo!()
    }

    pub fn concat(&mut self, data: Vec<DataframeRow>) {
        // TODO:
        // 1. data direction
        // 2. iter and check type, if unmatched set error
        todo!()
    }
}

#[cfg(test)]
mod tiny_df_test {
    use chrono::NaiveDate;

    use super::*;

    const DIVIDER: &'static str = "-------------------------------------------------------------";

    #[test]
    fn test_df_new_h() {
        let data: Vec<DataframeRow> = vec![
            vec!["date".into(), "object".into(), "value".into()],
            vec![NaiveDate::from_ymd(2000, 1, 1).into(), "A".into(), 5.into()],
            vec![
                NaiveDate::from_ymd(2010, 6, 1).into(),
                "B".into(),
                23.into(),
            ],
            vec![NaiveDate::from_ymd(2020, 10, 1).into(), "C".into()],
        ];
        let df = Dataframe::new(data, "h".into());
        println!("{:#?}", df);
        println!("{:?}", DIVIDER);

        let data: Vec<DataframeRow> = vec![
            vec!["date".into(), "object".into()],
            vec![NaiveDate::from_ymd(2000, 1, 1).into(), "A".into(), 5.into()],
            vec![
                NaiveDate::from_ymd(2010, 6, 1).into(),
                "B".into(),
                23.into(),
                "out of bound".into(),
            ],
            vec![
                NaiveDate::from_ymd(2020, 10, 1).into(),
                22.into(),
                38.into(),
            ],
        ];
        let df = Dataframe::new(data, "h".into());
        println!("{:#?}", df);
        println!("{:?}", DIVIDER);
    }
}
