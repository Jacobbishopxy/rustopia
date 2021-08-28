use std::mem;

use crate::dataframe::{Dataframe, DataframeRowProcessor, RefCols};
use crate::meta::*;

impl Dataframe {
    /// transpose dataframe
    pub fn transpose(&mut self) {
        // None direction's data cannot be transposed
        if self.data_orientation == DataOrientation::Raw {
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
        self.data_orientation = match self.data_orientation {
            DataOrientation::Horizontal => DataOrientation::Vertical,
            DataOrientation::Vertical => DataOrientation::Horizontal,
            DataOrientation::Raw => DataOrientation::Raw,
        }
    }

    /// executed when append a new row to `self.data`
    fn push_indices(&mut self) {
        self.size.0 += 1;
        self.indices.push(DataframeIndex::Id(self.size.0 as u64));
    }

    /// append a new row to `self.data`
    pub fn append(&mut self, data: Series) {
        let mut data = data;

        match self.data_orientation {
            DataOrientation::Horizontal => {
                let mut processor = DataframeRowProcessor::new(RefCols::R(&self.columns));
                for i in 0..self.size.1 {
                    match data.get_mut(i) {
                        Some(v) => processor.exec(i, v),
                        None => processor.skip(),
                    }
                }
                self.data.push(processor.data);
                self.push_indices();
            }
            DataOrientation::Vertical => {
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
                self.push_indices();
            }
            DataOrientation::Raw => {
                self.data.push(data);
            }
        }
    }

    /// concat new data to `self.data`
    pub fn concat(&mut self, data: DF) {
        let mut data = data;

        match self.data_orientation {
            DataOrientation::Horizontal => {
                for row in data {
                    self.append(row);
                }
            }
            DataOrientation::Vertical => {
                for row in data {
                    self.append(row);
                }
            }
            DataOrientation::Raw => {
                self.data.append(&mut data);
            }
        }
    }

    /// executed when insert a new row to `self.data`
    fn insert_indices(&mut self, index: usize, orient: DataOrientation) {
        match orient {
            DataOrientation::Horizontal => {
                self.indices
                    .insert(index, DataframeData::Id(self.size.0 as u64));
                self.size.0 += 1;
            }
            DataOrientation::Vertical => {
                self.indices
                    .insert(index, DataframeData::Id(self.size.1 as u64));
                self.size.1 += 1;
            }
            DataOrientation::Raw => (),
        }
    }

    /// insert a series to a horizontal orientation dataframe
    fn insert_h<T>(&mut self, index: usize, series: Series, orient: T)
    where
        T: Into<DataOrientation>,
    {
        let mut series = series;
        let orient: DataOrientation = orient.into();

        match orient {
            // inserted series as row-wise
            DataOrientation::Horizontal => {
                let mut processor = DataframeRowProcessor::new(RefCols::R(&self.columns));

                for i in 0..self.size.1 {
                    match series.get_mut(i) {
                        Some(v) => processor.exec(i, v),
                        None => processor.skip(),
                    }
                }

                self.data.insert(index, processor.data);
                self.insert_indices(index, orient);
            }
            // inserted series as column-wise
            DataOrientation::Vertical => {
                let mut processor = DataframeRowProcessor::new(RefCols::D);

                for i in 0..self.size.0 + 1 {
                    match series.get_mut(i) {
                        Some(v) => processor.exec(i, v),
                        None => processor.skip(),
                    }

                    if i > 0 {
                        self.data
                            .get_mut(i - 1)
                            .unwrap()
                            .insert(index, processor.data.pop().unwrap());
                    }
                }
                self.columns.insert(index, processor.get_cache_col());
                self.size.1 += 1;
            }
            DataOrientation::Raw => (),
        }
    }

    /// insert a series to a vertical orientation dataframe
    fn insert_v<T>(&mut self, index: usize, series: Series, orient: T)
    where
        T: Into<DataOrientation>,
    {
        let mut series = series;
        let orient: DataOrientation = orient.into();

        match orient {
            DataOrientation::Horizontal => {
                let mut processor = DataframeRowProcessor::new(RefCols::D);

                for i in 0..self.size.1 + 1 {
                    match series.get_mut(i) {
                        Some(v) => processor.exec(i, v),
                        None => processor.skip(),
                    }
                }

                self.columns.insert(index, processor.get_cache_col());
                self.size.0 += 1;
                self.data.insert(index, processor.data);
            }
            DataOrientation::Vertical => {
                let mut processor = DataframeRowProcessor::new(RefCols::R(&self.columns));

                for i in 0..self.size.0 {
                    match series.get_mut(i) {
                        Some(v) => processor.exec(i, v),
                        None => processor.skip(),
                    }

                    self.data
                        .get_mut(i)
                        .unwrap()
                        .insert(index, processor.data.pop().unwrap());
                }

                self.insert_indices(index, orient);
            }
            DataOrientation::Raw => (),
        }
    }

    /// insert a series to a raw dataframe
    fn insert_r<T>(&mut self, index: usize, series: Series, orient: T)
    where
        T: Into<DataOrientation>,
    {
        let orient: DataOrientation = orient.into();

        match orient {
            DataOrientation::Horizontal => self.data.insert(index, series),
            DataOrientation::Vertical => {
                self.data
                    .iter_mut()
                    .zip(series.into_iter())
                    .for_each(|(v, i)| {
                        v.insert(index, i);
                    })
            }
            DataOrientation::Raw => (),
        }
    }

    /// insert data
    pub fn insert<T>(&mut self, index: usize, series: Series, orient: T)
    where
        T: Into<DataOrientation>,
    {
        if series.len() == 0 {
            return;
        }
        match self.data_orientation {
            DataOrientation::Horizontal => self.insert_h(index, series, orient),
            DataOrientation::Vertical => self.insert_v(index, series, orient),
            DataOrientation::Raw => self.insert_r(index, series, orient),
        }
    }

    /// batch insert
    pub fn insert_many<T>(&mut self, index: usize, dataframe: DF, orient: T)
    where
        T: Into<DataOrientation>,
    {
        let orient: DataOrientation = orient.into();

        for (i, v) in dataframe.into_iter().enumerate() {
            self.insert(i + index, v, orient.clone());
        }
    }

    /// truncate, clear all data but columns and data_orientation
    pub fn truncate(&mut self) {
        self.data = vec![];
        self.indices = vec![];
        self.size = (0, 0);
    }

    /// delete a series from a horizontal orientation dataframe
    fn delete_h<T>(&mut self, index: usize, orient: T)
    where
        T: Into<DataOrientation>,
    {
        let orient: DataOrientation = orient.into();

        match orient {
            DataOrientation::Horizontal => {
                if index > self.size.0 {
                    return;
                }
                self.data.remove(index);
                self.indices.remove(index);
                self.size.0 -= 1;
            }
            DataOrientation::Vertical => {
                if index > self.size.1 {
                    return;
                }
                self.data.iter_mut().for_each(|v| {
                    v.remove(index);
                });
                self.columns.remove(index);
                self.size.1 -= 1;
            }
            DataOrientation::Raw => (),
        }
    }

    /// delete a series from a vertical orientation dataframe
    fn delete_v<T>(&mut self, index: usize, orient: T)
    where
        T: Into<DataOrientation>,
    {
        let orient: DataOrientation = orient.into();

        match orient {
            DataOrientation::Horizontal => {
                if index > self.size.0 {
                    return;
                }
                self.data.remove(index);
                self.columns.remove(index);
                self.size.0 -= 1;
            }
            DataOrientation::Vertical => {
                if index > self.size.1 {
                    return;
                }
                self.data.iter_mut().for_each(|v| {
                    v.remove(index);
                });
                self.indices.remove(index);
                self.size.1 -= 1;
            }
            DataOrientation::Raw => (),
        }
    }

    /// delete a series from a raw dataframe
    fn delete_r<T>(&mut self, index: usize, orient: T)
    where
        T: Into<DataOrientation>,
    {
        let orient: DataOrientation = orient.into();

        match orient {
            DataOrientation::Horizontal => {
                self.data.remove(index);
            }
            DataOrientation::Vertical => {
                for v in self.data.iter_mut() {
                    v.remove(index);
                }
            }
            DataOrientation::Raw => (),
        }
    }

    /// delete a specific series, row-wise or column-wise
    pub fn delete<T>(&mut self, index: usize, orient: T)
    where
        T: Into<DataOrientation>,
    {
        let orient: DataOrientation = orient.into();

        match orient {
            DataOrientation::Horizontal => self.delete_h(index, orient),
            DataOrientation::Vertical => self.delete_v(index, orient),
            DataOrientation::Raw => self.delete_r(index, orient),
        }
    }

    /// batch delete
    pub fn delete_many<T>(&mut self, indices: &[usize], orient: T)
    where
        T: Into<DataOrientation>,
    {
        let orient: DataOrientation = orient.into();
        let mut indices = indices.to_vec();
        indices.sort_by(|a, b| b.cmp(a));

        for i in indices {
            self.delete(i, orient.clone());
        }
    }
}

#[cfg(test)]
mod test_manipulator {
    use chrono::NaiveDate;

    use crate::*;

    const DIVIDER: &'static str = "-------------------------------------------------------------";

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

    #[test]
    fn test_df_truncate() {
        let data = df![
            ["idx", "name", "tag"],
            [0, "Jacob", "Cool"],
            [1, "Sam", "Mellow"],
        ];

        let mut df = Dataframe::new(data, "h");

        println!("{:#?}", df);

        df.truncate();
        println!("{:#?}", df);
    }

    #[test]
    fn test_df_h_insert_h() {
        let data = df![
            ["idx", "name", "tag"],
            [0, "Jacob", "Cool"],
            [1, "Sam", "Mellow"],
        ];

        let mut df = Dataframe::new(data, "h");

        let s = series![2, "Box", "Pure"];

        df.insert(1, s, "h");

        println!("{:#?}", df);
    }

    #[test]
    fn test_df_h_insert_v() {
        let data = df![
            ["idx", "name", "tag"],
            [0, "Jacob", "Cool"],
            [1, "Sam", "Mellow"],
        ];

        let mut df = Dataframe::new(data, "h");

        let s = series!["note", "#1", "#2"];

        df.insert(2, s, "v");

        println!("{:#?}", df);
    }

    #[test]
    fn test_df_v_insert_h() {
        let data = df![
            ["idx", 0, 1, 2],
            ["name", "Jacob", "Sam", "Mia"],
            ["tag", "Cool", "Mellow", "Enthusiastic"],
        ];

        let mut df = Dataframe::new(data, "v");

        let s = series!["note", "#1", "#2"];

        df.insert(1, s, "h");

        println!("{:#?}", df);
    }

    #[test]
    fn test_df_v_insert_v() {
        let data = df![
            ["idx", 0, 1],
            ["name", "Jacob", "Sam"],
            ["tag", "Cool", "Mellow"],
        ];

        let mut df = Dataframe::new(data, "V");

        let s = series![2, "Box", "Pure", "OoB"];

        df.insert(2, s, "V");

        println!("{:#?}", df);
    }

    #[test]
    fn test_df_h_delete_h() {
        let data = df![
            ["idx", "name", "tag"],
            [0, "Jacob", "Cool"],
            [1, "Sam", "Mellow"],
            [2, "Mia", "Soft"],
        ];

        let mut df = Dataframe::new(data, "h");

        df.delete(1, "h");

        println!("{:#?}", df);
    }

    #[test]
    fn test_df_h_delete_v() {
        let data = df![
            ["idx", "name", "tag"],
            [0, "Jacob", "Cool"],
            [1, "Sam", "Mellow"],
            [2, "Mia", "Soft"],
        ];

        let mut df = Dataframe::new(data, "h");

        df.delete(1, "v");

        println!("{:#?}", df);
    }

    #[test]
    fn test_df_v_delete_h() {
        let data = df![
            ["idx", 0, 1, 2],
            ["name", "Jacob", "Sam", "Mia"],
            ["tag", "Cool", "Mellow", "Enthusiastic"],
        ];

        let mut df = Dataframe::new(data, "v");

        df.delete(1, "h");

        println!("{:#?}", df);
    }

    #[test]
    fn test_df_v_delete_v() {
        let data = df![
            ["idx", 0, 1, 2],
            ["name", "Jacob", "Sam", "Mia"],
            ["tag", "Cool", "Mellow", "Enthusiastic"],
        ];

        let mut df = Dataframe::new(data, "V");

        df.delete(2, "V");

        println!("{:#?}", df);
    }
}
