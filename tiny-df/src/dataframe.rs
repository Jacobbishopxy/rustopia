//! Dataframe
//! Core struct
//!
//! Main function:
//! 1. `new`
//! 1. `from_2d_vec`
//! 1. `transpose`
//! 1. `append`
//! 1. `concat`

use std::mem;

use crate::data::*;

/// Columns definition
/// 1. D: dynamic column
/// 1. R: reference
enum RefCols<'a> {
    D,
    R(&'a Vec<DataframeColDef>),
}

/// process series (dataframe row) data
struct DataframeRowProcessor<'a> {
    data: Series,
    columns: RefCols<'a>,
    _cache_col_name: Option<String>,
    _cache_col: Option<DataframeColDef>,
}

impl<'a> DataframeRowProcessor<'a> {
    /// dataframe row processor constructor
    fn new(ref_col: RefCols<'a>) -> Self {
        DataframeRowProcessor {
            data: Vec::new(),
            columns: ref_col,
            _cache_col_name: None,
            _cache_col: None,
        }
    }

    /// check data type, if matching push the data to buf else push None to buf
    fn exec(&mut self, type_idx: usize, data: &mut DataframeData) {
        match self.columns {
            RefCols::D => {
                if type_idx == 0 {
                    // get column name
                    self._cache_col_name = Some(data.to_string());
                    return;
                }
                if type_idx == 1 {
                    // until now (the 2nd cell) we can know the type of this row
                    // create `DataframeColDef` and push to `columns`
                    let cd = DataframeColDef::new(
                        self._cache_col_name.clone().unwrap(),
                        data.as_ref().into(),
                    );

                    self._cache_col = Some(cd);
                }

                // check type and wrap
                let mut tmp = DataframeData::None;
                let value_type: DataType = data.as_ref().into();
                if self._cache_col.as_ref().unwrap().col_type == value_type {
                    mem::swap(&mut tmp, data);
                }

                self.data.push(tmp)
            }
            RefCols::R(r) => {
                // check type and wrap
                let mut tmp = DataframeData::None;
                let value_type: DataType = data.as_ref().into();
                if r.get(type_idx).unwrap().col_type == value_type {
                    mem::swap(&mut tmp, data);
                }

                self.data.push(tmp)
            }
        }
    }

    /// push None to buf
    fn skip(&mut self) {
        self.data.push(DataframeData::None);
    }

    /// get cached column, used for vertical data direction processing
    fn get_cache_col(&self) -> DataframeColDef {
        self._cache_col.clone().unwrap_or_default()
    }
}

/// Dataframe
/// Core struct of this lib crate
///
/// A dataframe can store three kinds of data, which is determined by its direction:
/// - horizontal presence: each row means one record, certified data size
/// - vertical presence: each column means one record, certified data size
/// - none: raw data, uncertified data size (each row can have different size)
#[derive(Debug, Default)]
pub struct Dataframe {
    pub data: DF,
    columns: Vec<DataframeColDef>,
    data_direction: DataDirection,
    size: (usize, usize),
}

/// New dataframe if data_direction is none
fn new_df_dir_n(data: DF) -> Dataframe {
    Dataframe {
        data,
        ..Default::default()
    }
}

/// New dataframe if data_direction is horizontal and columns has been given
/// columns length equals dataframe column size
fn new_df_dir_h_col(data: DF, columns: Vec<DataframeColDef>) -> Dataframe {
    let length_of_head_row = columns.len();

    // result init
    let mut res = Vec::new();

    // processing the rest of rows, if exceeded then trim, if insufficient then filling with None
    for mut d in data {
        // each row init a row processor
        let mut processor = DataframeRowProcessor::new(RefCols::R(&columns));

        for i in 0..length_of_head_row {
            match d.get_mut(i) {
                Some(v) => processor.exec(i, v),
                None => processor.skip(),
            }
        }
        res.push(processor.data);
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
fn new_df_dir_v_col(data: DF, columns: Vec<DataframeColDef>) -> Dataframe {
    let length_of_head_row = match data.get(0) {
        Some(l) => l.len(),
        None => return Dataframe::default(),
    };
    let length_of_res = columns.len();

    let mut res = Vec::new();

    // processing the rest of rows, if exceeded then trim, if insufficient then filling with None
    for (row_idx, mut d) in data.into_iter().enumerate() {
        let mut processor = DataframeRowProcessor::new(RefCols::R(&columns));
        for i in 0..length_of_head_row {
            match d.get_mut(i) {
                Some(v) => processor.exec(row_idx, v),
                None => processor.skip(),
            }
        }
        res.push(processor.data);
        // break, align to column name
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
fn new_df_dir_h(data: DF) -> Dataframe {
    let mut data_iter = data.iter();
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

    // determine columns type
    match data_iter.next() {
        Some(vd) => {
            for (i, d) in vd.iter().enumerate() {
                column_type.push(d.into());
                // break, align to column name
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
    new_df_dir_h_col(data, columns)
}

/// New dataframe if data_direction is horizontal
fn new_df_dir_v(data: DF) -> Dataframe {
    // take the 1st row length, data row length is subtracted by 1,
    // since the first element must be column name
    let length_of_head_row = data.get(0).unwrap().len();
    if length_of_head_row == 1 {
        return Dataframe::default();
    }

    // init columns & data
    let (mut columns, mut res) = (Vec::new(), Vec::new());

    // unlike `new_df_dir_h_col`, `new_df_dir_v_col` & `new_df_dir_h`,
    // columns type definition is not given, hence needs to iterate through the whole data
    // and dynamically construct it
    for mut d in data.into_iter() {
        let mut processor = DataframeRowProcessor::new(RefCols::D);

        for i in 0..length_of_head_row {
            match d.get_mut(i) {
                Some(v) => {
                    processor.exec(i, v);
                }
                None => {
                    processor.skip();
                }
            }
        }
        columns.push(processor.get_cache_col());
        res.push(processor.data);
    }

    let length_of_res = res.len();

    Dataframe {
        data: res,
        columns: columns,
        data_direction: DataDirection::Vertical,
        size: (length_of_res, length_of_head_row - 1),
    }
}

impl Dataframe {
    /// Dataframe constructor
    /// Accepting tree kinds of data:
    /// 1. in horizontal direction, columns name is the first row
    /// 2. in vertical direction, columns name is the first columns
    /// 3. none direction, raw data
    pub fn new<T, P>(data: T, data_direction: P) -> Self
    where
        T: Into<DF>,
        P: Into<DataDirection>,
    {
        let data = data.into();
        if Dataframe::is_empty(&data) {
            return Dataframe::default();
        }
        match data_direction.into() {
            DataDirection::Horizontal => new_df_dir_h(data),
            DataDirection::Vertical => new_df_dir_v(data),
            DataDirection::None => new_df_dir_n(data),
        }
    }

    /// Dataframe constructor
    /// From a 2d vector
    pub fn from_2d_vec<T, P>(data: T, data_direction: P, columns: Vec<DataframeColDef>) -> Self
    where
        T: Into<DF>,
        P: Into<DataDirection>,
    {
        let data = data.into();
        if Dataframe::is_empty(&data) || columns.len() == 0 {
            return Dataframe::default();
        }
        match data_direction.into() {
            DataDirection::Horizontal => new_df_dir_h_col(data, columns),
            DataDirection::Vertical => new_df_dir_v_col(data, columns),
            DataDirection::None => new_df_dir_n(data),
        }
    }

    /// check if input data is empty
    pub fn is_empty(data: &DF) -> bool {
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
        // None direction's data cannot be transposed
        if self.data_direction == DataDirection::None {
            return;
        }
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

    /// append a new row to `self.data`
    pub fn append(&mut self, data: Series) {
        let mut data = data;

        match self.data_direction {
            DataDirection::Horizontal => {
                let mut processor = DataframeRowProcessor::new(RefCols::R(&self.columns));
                for i in 0..self.size.1 {
                    match data.get_mut(i) {
                        Some(v) => processor.exec(i, v),
                        None => processor.skip(),
                    }
                }
                self.data.push(processor.data);
                self.size.0 += 1;
            }
            DataDirection::Vertical => {
                let mut processor = DataframeRowProcessor::new(RefCols::D);
                // +1 means the first cell representing column name
                for i in 0..self.size.1 + 1 {
                    match data.get_mut(i) {
                        Some(v) => processor.exec(i, v),
                        None => processor.skip(),
                    }
                }
                self.columns.push(processor.get_cache_col());
                self.data.push(processor.data);
                self.size.0 += 1;
            }
            DataDirection::None => {
                self.data.push(data);
            }
        }
    }

    /// concat new data to `self.data`
    pub fn concat(&mut self, data: DF) {
        let mut data = data;

        match self.data_direction {
            DataDirection::Horizontal => {
                for row in data {
                    self.append(row);
                }
            }
            DataDirection::Vertical => {
                for row in data {
                    self.append(row);
                }
            }
            DataDirection::None => {
                self.data.append(&mut data);
            }
        }
    }
}

#[cfg(test)]
mod tiny_df_test {
    use chrono::NaiveDate;

    use crate::{df, series};

    use super::*;

    const DIVIDER: &'static str = "-------------------------------------------------------------";

    #[test]
    fn test_df_new_h() {
        let data: DF = df![
            ["date", "object", "value"],
            [NaiveDate::from_ymd(2000, 1, 1), "A", 5],
        ];
        let df = Dataframe::new(data, "h");
        println!("{:#?}", df);
        println!("{:?}", DIVIDER);

        let data: DF = df![
            ["date", "object"],
            [NaiveDate::from_ymd(2000, 1, 1), "A", 5],
            [NaiveDate::from_ymd(2010, 6, 1), "B", 23, "out of bound",],
            [NaiveDate::from_ymd(2020, 10, 1), 22, 38,],
        ];
        let df = Dataframe::new(data, "h");
        println!("{:#?}", df);
        println!("{:?}", DIVIDER);
    }

    #[test]
    fn test_df_new_v() {
        let data: DF = df![
            [
                "date",
                NaiveDate::from_ymd(2000, 1, 1),
                NaiveDate::from_ymd(2010, 6, 1),
                NaiveDate::from_ymd(2020, 10, 1),
            ],
            ["object", "A", "B", "C"],
            ["value", 5, "wrong num", 23],
        ];
        let df = Dataframe::new(data, "V");
        println!("{:#?}", df);
        println!("{:?}", DIVIDER);

        let data: DF = df![
            [
                "date",
                NaiveDate::from_ymd(2000, 1, 1),
                NaiveDate::from_ymd(2010, 6, 1),
            ],
            ["object", "A", "B", "C"],
            ["value", 5, 23],
        ];
        let df = Dataframe::new(data, "v");
        println!("{:#?}", df);
        println!("{:?}", DIVIDER);

        let data: DF = df![["date",], ["object",], ["value",],];
        let df = Dataframe::new(data, "v");
        println!("{:#?}", df);
        println!("{:?}", DIVIDER);
    }

    #[test]
    fn test_df_new_h_col() {
        let data: DF = df![
            [NaiveDate::from_ymd(2000, 1, 1), "A", 5],
            [NaiveDate::from_ymd(2010, 6, 1), "B", 23, "out of bound",],
            [NaiveDate::from_ymd(2020, 10, 1), 22, 38,],
            [NaiveDate::from_ymd(2030, 5, 1), DataframeData::None, 3,],
        ];
        let col = vec![
            DataframeColDef::new("date", DataType::Date),
            DataframeColDef::new("object", DataType::String),
            DataframeColDef::new("value", DataType::Short),
        ];
        let df = Dataframe::from_2d_vec(data, "h", col);
        println!("{:#?}", df);
        println!("{:?}", DIVIDER);
    }

    #[test]
    fn test_df_new_v_col() {
        let data: DF = df![
            [
                NaiveDate::from_ymd(2000, 1, 1),
                NaiveDate::from_ymd(2010, 6, 1),
                NaiveDate::from_ymd(2020, 10, 1),
                NaiveDate::from_ymd(2030, 10, 1),
            ],
            ["A", "B", "C"],
            [5, "wrong num", 23],
        ];
        let col = vec![
            DataframeColDef::new("date", DataType::Date),
            DataframeColDef::new("object", DataType::String),
            DataframeColDef::new("value", DataType::Short),
        ];
        let df = Dataframe::from_2d_vec(data, "v", col);
        println!("{:#?}", df);
        println!("{:?}", DIVIDER);
    }

    #[test]
    fn test_df_transpose() {
        let data: DF = df![
            [
                "date",
                NaiveDate::from_ymd(2000, 1, 1),
                NaiveDate::from_ymd(2010, 6, 1),
                NaiveDate::from_ymd(2020, 10, 1),
                NaiveDate::from_ymd(2030, 1, 1),
            ],
            ["object", "A", "B", "C", "D",],
            ["value", 5, "wrong num", 23, 0,],
        ];
        let mut df = Dataframe::new(data, "V");
        println!("{:#?}", df);
        println!("{:?}", DIVIDER);

        df.transpose();
        println!("{:#?}", df);
    }

    #[test]
    fn test_df_h_append() {
        let data = df![
            ["date", "object", "value"],
            [NaiveDate::from_ymd(2000, 1, 1), "A", 5],
            [NaiveDate::from_ymd(2010, 6, 1), "B", 23, "out of bound",],
            [NaiveDate::from_ymd(2020, 10, 1), 22, 38,],
        ];
        let mut df = Dataframe::new(data, "H");
        let extra = series![
            NaiveDate::from_ymd(2030, 1, 1),
            "K",
            "wrong type",
            "out of bound",
        ];

        df.append(extra);

        println!("{:#?}", df);
        println!("{:?}", DIVIDER);
    }

    #[test]
    fn test_df_v_append() {
        let data: DF = df![
            [
                "date",
                NaiveDate::from_ymd(2000, 1, 1),
                NaiveDate::from_ymd(2010, 6, 1),
                NaiveDate::from_ymd(2020, 10, 1),
            ],
            ["object", "A", "B", "C"],
            ["value", 5, "wrong num", 23],
        ];
        let mut df = Dataframe::new(data, "v");
        let extra = series!["Note", "K", "B", "A",];

        df.append(extra);

        println!("{:#?}", df);
        println!("{:?}", DIVIDER);
    }

    #[test]
    fn test_df_h_concat() {
        let data = df![
            ["date", "object", "value"],
            [NaiveDate::from_ymd(2000, 1, 1), "A", 5],
            [NaiveDate::from_ymd(2010, 6, 1), "B", 23, "out of bound",],
            [NaiveDate::from_ymd(2020, 10, 1), 22, 38,],
        ];
        let mut df = Dataframe::new(data, "H");
        let extra = df![
            [
                NaiveDate::from_ymd(2030, 1, 1),
                "K",
                "wrong type",
                "out of bound",
            ],
            [NaiveDate::from_ymd(2040, 3, 1), "Q", 18, "out of bound",]
        ];

        df.concat(extra);

        println!("{:#?}", df);
        println!("{:?}", DIVIDER);
    }

    #[test]
    fn test_df_v_concat() {
        let data: DF = df![
            [
                "date",
                NaiveDate::from_ymd(2000, 1, 1),
                NaiveDate::from_ymd(2010, 6, 1),
                NaiveDate::from_ymd(2020, 10, 1),
            ],
            ["object", "A", "B", "C"],
            ["value", 5, "wrong num", 23],
        ];
        let mut df = Dataframe::new(data, "v");
        let extra = df![["Note", "K", "B", "A",], ["PS", 1, "worong type", 2,],];

        df.concat(extra);

        println!("{:#?}", df);
        println!("{:?}", DIVIDER);
    }
}
