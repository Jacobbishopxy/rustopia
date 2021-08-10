//! A row based Dataframe structure

use std::{fmt::Display, mem};

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

/// A dataframe columns definition
#[derive(Debug)]
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

/// Dataframe
/// Core struct of this lib crate
#[derive(Debug, Default)]
pub struct Dataframe {
    pub data: Vec<DataframeRow>,
    columns: Vec<DataframeColDef>,
    data_direction: DataDirection,
    size: (usize, usize),
}

impl Dataframe {
    /// New dataframe if data_direction is none
    fn new_df_dir_n(data: Vec<DataframeRow>) -> Self {
        Dataframe {
            data,
            ..Default::default()
        }
    }

    /// New dataframe if data_direction is horizontal and columns has been given
    /// columns length equals dataframe column size
    fn new_df_dir_h_col(data: Vec<DataframeRow>, columns: Vec<DataframeColDef>) -> Self {
        let length_of_head_row = columns.len();

        let mut res = Vec::new();

        // processing the rest of rows, if exceeded then trim, if insufficient then filling with None
        for mut d in data {
            let mut buf = Vec::new();
            for i in 0..length_of_head_row {
                match d.get_mut(i) {
                    Some(v) => {
                        let mut tmp = DataframeData::None;
                        let value_type: DataType = v.as_ref().into();
                        if columns.get(i).unwrap().col_type == value_type {
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
            columns: columns,
            data_direction: DataDirection::Horizontal,
            size: (length_of_res, length_of_head_row),
        }
    }

    /// New dataframe if data_direction is vertical and columns has been given
    /// columns length equals dataframe row size
    fn new_df_dir_v_col(data: Vec<DataframeRow>, columns: Vec<DataframeColDef>) -> Self {
        let length_of_head_row = match data.get(0) {
            Some(l) => l.len(),
            None => return Dataframe::default(),
        };
        let length_of_res = columns.len();

        let mut res = Vec::new();

        for (row_idx, mut d) in data.into_iter().enumerate() {
            let mut buf = Vec::new();
            for i in 0..length_of_head_row {
                match d.get_mut(i) {
                    Some(v) => {
                        let mut tmp = DataframeData::None;
                        let value_type: DataType = v.as_ref().into();
                        if columns.get(row_idx).unwrap().col_type == value_type {
                            mem::swap(&mut tmp, v);
                        }
                        buf.push(tmp);
                    }
                    None => buf.push(DataframeData::None),
                }
            }
            res.push(buf);
            if row_idx == length_of_res - 1 {
                break;
            }
        }

        Dataframe {
            data: res,
            columns: columns,
            data_direction: DataDirection::Vertical,
            size: (length_of_res, length_of_head_row),
        }
    }

    /// New dataframe if data_direction is horizontal and columns is included in data
    fn new_df_dir_h(data: Vec<DataframeRow>) -> Self {
        let mut data_iter = data.iter().peekable();
        // take the 1st row as the columns name row
        let columns_name = data_iter
            .next()
            .unwrap()
            .into_iter()
            .map(|d| d.to_string())
            .collect::<Vec<String>>();

        // make sure each row has the same length
        let length_of_head_row = columns_name.len();

        // using the second row to determine columns' type
        let mut column_type: Vec<DataType> = Vec::new();

        // peek iterator only to get the next row and use it to determine columns type
        match data_iter.peek() {
            Some(vd) => {
                for (i, d) in vd.iter().enumerate() {
                    column_type.push(d.into());

                    if i == length_of_head_row - 1 {
                        break;
                    }
                }
            }
            None => return Dataframe::default(),
        }

        // generate`Vec<DataframeColDef>` and pass it to `new_dataframe_h_dir_col_given`
        let columns = columns_name
            .into_iter()
            .zip(column_type.into_iter())
            .map(|(name, col_type)| DataframeColDef { name, col_type })
            .collect();

        let mut data = data;
        data.remove(0);
        Dataframe::new_df_dir_h_col(data, columns)
    }

    /// New dataframe if data_direction is horizontal
    fn new_df_dir_v(data: Vec<DataframeRow>) -> Self {
        // take the 1st row length, data row length is subtracted by 1,
        // since the first element must be column name
        let length_of_head_row = match data.get(0) {
            Some(l) => {
                let l = l.len();
                if l == 1 {
                    return Dataframe::default();
                }
                l
            }
            None => return Dataframe::default(),
        };

        // init columns & data
        let (mut columns, mut res) = (Vec::new(), Vec::new());

        for mut d in data {
            let mut buf = Vec::new();
            let mut column_name = "".to_owned();
            let mut column_type = DataType::None;

            for i in 0..length_of_head_row {
                match d.get_mut(i) {
                    Some(v) => {
                        if i == 0 {
                            column_name = v.to_string();
                            continue;
                        }
                        if i == 1 {
                            column_type = v.as_ref().into();
                        }
                        let mut tmp = DataframeData::None;
                        let value_type: DataType = v.as_ref().into();
                        if value_type == column_type {
                            mem::swap(&mut tmp, v);
                        }
                        buf.push(tmp);
                    }
                    None => buf.push(DataframeData::None),
                }
            }
            columns.push(DataframeColDef::new(
                column_name.clone(),
                column_type.clone(),
            ));
            res.push(buf);
        }

        let length_of_res = res.len();

        Dataframe {
            data: res,
            columns: columns,
            data_direction: DataDirection::Vertical,
            size: (length_of_res, length_of_head_row - 1),
        }
    }

    /// Dataframe constructor
    /// Accepting tree kinds of data:
    /// 1. in horizontal direction, columns name is the first row
    /// 2. in vertical direction, columns name is the first columns
    /// 3. none direction, raw data
    pub fn new(data: Vec<DataframeRow>, data_direction: DataDirection) -> Self {
        if Dataframe::is_empty(&data) {
            return Dataframe::default();
        }
        match data_direction {
            DataDirection::Horizontal => Dataframe::new_df_dir_h(data),
            DataDirection::Vertical => Dataframe::new_df_dir_v(data),
            DataDirection::None => Dataframe::new_df_dir_n(data),
        }
    }

    /// Dataframe constructor
    /// From a 2d vector
    pub fn from_2d_vec(
        data: Vec<DataframeRow>,
        data_direction: DataDirection,
        columns: Vec<DataframeColDef>,
    ) -> Self {
        if Dataframe::is_empty(&data) || columns.len() == 0 {
            return Dataframe::default();
        }
        match data_direction {
            DataDirection::Horizontal => Dataframe::new_df_dir_h_col(data, columns),
            DataDirection::Vertical => Dataframe::new_df_dir_v_col(data, columns),
            DataDirection::None => Dataframe::new_df_dir_n(data),
        }
    }

    /// check if input data is empty
    pub fn is_empty(data: &Vec<DataframeRow>) -> bool {
        if data.is_empty() {
            true
        } else {
            data[0].is_empty()
        }
    }

    /// get dataframe sized
    pub fn size(&self) -> (usize, usize) {
        self.size
    }

    /// get dataframe columns
    pub fn columns(&self) -> &Vec<DataframeColDef> {
        &self.columns
    }

    /// get dataframe direction
    pub fn data_direction(&self) -> &DataDirection {
        &self.data_direction
    }

    /// transpose dataframe
    pub fn transpose(&mut self) {
        let (m, n) = self.size;
        let mut res = Vec::with_capacity(n);
        for j in 0..n {
            let mut row = Vec::with_capacity(m);
            for i in 0..m {
                let mut tmp = DataframeData::None;
                mem::swap(&mut tmp, &mut self.data[i][j]);
                row.push(tmp);
            }
            res.push(row);
        }
        self.data = res;
        self.size = (n, m);
        self.data_direction = match self.data_direction {
            DataDirection::Horizontal => DataDirection::Vertical,
            DataDirection::Vertical => DataDirection::Horizontal,
            DataDirection::None => DataDirection::None,
        }
    }

    pub fn append(&mut self, _data: DataframeRow) {
        // TODO:
        // 1. data direction
        // 2. check type, if unmatched set error
        todo!()
    }

    pub fn concat(&mut self, _data: Vec<DataframeRow>) {
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

    #[test]
    fn test_df_new_v() {
        let data: Vec<DataframeRow> = vec![
            vec![
                "date".into(),
                NaiveDate::from_ymd(2000, 1, 1).into(),
                NaiveDate::from_ymd(2010, 6, 1).into(),
                NaiveDate::from_ymd(2020, 10, 1).into(),
            ],
            vec!["object".into(), "A".into(), "B".into(), "C".into()],
            vec!["value".into(), 5.into(), "wrong num".into(), 23.into()],
        ];
        let df = Dataframe::new(data, "V".into());
        println!("{:#?}", df);
        println!("{:?}", DIVIDER);

        let data: Vec<DataframeRow> = vec![
            vec![
                "date".into(),
                NaiveDate::from_ymd(2000, 1, 1).into(),
                NaiveDate::from_ymd(2010, 6, 1).into(),
            ],
            vec!["object".into(), "A".into(), "B".into(), "C".into()],
            vec!["value".into(), 5.into(), 23.into()],
        ];
        let df = Dataframe::new(data, "v".into());
        println!("{:#?}", df);
        println!("{:?}", DIVIDER);
    }

    #[test]
    fn test_df_new_h_col() {
        let data: Vec<DataframeRow> = vec![
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
            vec![
                NaiveDate::from_ymd(2030, 5, 1).into(),
                DataframeData::None,
                3.into(),
            ],
        ];
        let col = vec![
            DataframeColDef::new("date", DataType::Date),
            DataframeColDef::new("object", DataType::String),
            DataframeColDef::new("value", DataType::Short),
        ];
        let df = Dataframe::from_2d_vec(data, "h".into(), col);
        println!("{:#?}", df);
        println!("{:?}", DIVIDER);
    }

    #[test]
    fn test_df_transpose() {
        let data: Vec<DataframeRow> = vec![
            vec![
                "date".into(),
                NaiveDate::from_ymd(2000, 1, 1).into(),
                NaiveDate::from_ymd(2010, 6, 1).into(),
                NaiveDate::from_ymd(2020, 10, 1).into(),
                NaiveDate::from_ymd(2030, 1, 1).into(),
            ],
            vec![
                "object".into(),
                "A".into(),
                "B".into(),
                "C".into(),
                "D".into(),
            ],
            vec![
                "value".into(),
                5.into(),
                "wrong num".into(),
                23.into(),
                0.into(),
            ],
        ];
        let mut df = Dataframe::new(data, "V".into());
        println!("{:#?}", df);
        println!("{:?}", DIVIDER);

        df.transpose();
        println!("{:#?}", df);
    }
}
