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

/// Dataframe
/// Core struct of this lib crate
#[derive(Debug, Default)]
pub struct Dataframe {
    pub data: DF,
    columns: Vec<DataframeColDef>,
    data_direction: DataDirection,
    size: (usize, usize),
}

impl Dataframe {
    /// New dataframe if data_direction is none
    fn new_df_dir_n(data: DF) -> Self {
        Dataframe {
            data,
            ..Default::default()
        }
    }

    /// New dataframe if data_direction is horizontal and columns has been given
    /// columns length equals dataframe column size
    fn new_df_dir_h_col(data: DF, columns: Vec<DataframeColDef>) -> Self {
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
    fn new_df_dir_v_col(data: DF, columns: Vec<DataframeColDef>) -> Self {
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
    fn new_df_dir_h(data: DF) -> Self {
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
    fn new_df_dir_v(data: DF) -> Self {
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
            DataDirection::Horizontal => Dataframe::new_df_dir_h(data),
            DataDirection::Vertical => Dataframe::new_df_dir_v(data),
            DataDirection::None => Dataframe::new_df_dir_n(data),
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
            DataDirection::Horizontal => Dataframe::new_df_dir_h_col(data, columns),
            DataDirection::Vertical => Dataframe::new_df_dir_v_col(data, columns),
            DataDirection::None => Dataframe::new_df_dir_n(data),
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

    pub fn append(&mut self, _data: Series) {
        // TODO:
        // 1. data direction
        // 2. check type, if unmatched set error
        todo!()
    }

    pub fn concat(&mut self, _data: DF) {
        // TODO:
        // 1. data direction
        // 2. iter and check type, if unmatched set error
        todo!()
    }
}

#[cfg(test)]
mod tiny_df_test {
    use chrono::NaiveDate;

    use crate::df;

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
}
