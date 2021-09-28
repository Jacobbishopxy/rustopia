//! Dataframe constructor

use crate::core::util::{RefCols, SeriesDataProcessor};
use crate::prelude::*;

/// New dataframe if data_orientation is none
fn new_df_n(data: D2) -> Dataframe {
    Dataframe {
        data,
        ..Default::default()
    }
}

/// New dataframe if data_orientation is horizontal and columns has been given
/// columns length equals dataframe column size
fn new_df_h_col(data: D2, columns: Vec<DataframeColumn>) -> Dataframe {
    let length_of_head_row = columns.len();

    // result init
    let mut res = Vec::new();

    // processing the rest of rows, if exceeded then trim, if insufficient then filling with None
    for mut d in data {
        // each row init a row processor
        let mut processor = SeriesDataProcessor::new(RefCols::R(&columns));

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
        indices: create_dataframe_indices(length_of_res),
        data_orientation: DataOrientation::Horizontal,
        size: (length_of_res, length_of_head_row),
    }
}

/// New dataframe if data_orientation is vertical and columns has been given
/// columns length equals dataframe row size
fn new_df_v_col(data: D2, columns: Vec<DataframeColumn>) -> Dataframe {
    let length_of_head_row = match data.get(0) {
        Some(l) => l.len(),
        None => return Dataframe::default(),
    };
    let length_of_res = columns.len();

    let mut res = Vec::new();

    // processing the rest of rows, if exceeded then trim, if insufficient then filling with None
    for (row_idx, mut d) in data.into_iter().enumerate() {
        let mut processor = SeriesDataProcessor::new(RefCols::R(&columns));
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
        indices: create_dataframe_indices(length_of_head_row),
        data_orientation: DataOrientation::Vertical,
        size: (length_of_res, length_of_head_row),
    }
}

/// New dataframe if data_orientation is strict mode
/// Please make sure the first element of data is an index series.
///
/// example:
/// ```rust,ignore
/// use tiny_df::prelude::*;
/// let data = vec![
///     vec![DataframeData::Id(0), DataframeData::Id(2), DataframeData::Id(5)],
///     vec![DataframeData::Short(10), DataframeData::Short(11), DataframeData::Short(12)],
/// ];
/// let columns = vec![DataframeColumn::new("value", DataType::Short)];
/// let df = new_df_strict_col(data, columns);
/// println!("{:#?}", df);
/// ```
#[cfg(feature = "strict")]
fn new_df_strict_col(data: D2, columns: Vec<DataframeColumn>) -> Dataframe {
    let col_len = columns.len();
    let mut res = vec![Vec::<DataframeData>::new(); col_len];

    // data iterator
    let mut itr = data.into_iter();

    let indices = itr.next().unwrap();
    let indices_len = indices.len();

    for (idx, d) in itr.enumerate() {
        res[idx] = d;
    }

    Dataframe {
        data: res,
        columns,
        indices,
        data_orientation: DataOrientation::Strict,
        size: (indices_len, col_len),
    }
}

/// New dataframe if data_orientation is horizontal and columns is included in data
fn df_from_vec_h(data: D2) -> Dataframe {
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

    // take the 2nd row and determine columns type
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
        .map(|(name, col_type)| DataframeColumn { name, col_type })
        .collect();

    let mut data = data;
    data.remove(0);
    new_df_h_col(data, columns)
}

/// New dataframe if data_orientation is horizontal
fn df_from_vec_v(data: D2) -> Dataframe {
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
        let mut processor = SeriesDataProcessor::new(RefCols::D);

        for i in 0..length_of_head_row {
            match d.get_mut(i) {
                Some(v) => processor.exec(i, v),
                None => processor.skip(),
            }
        }
        columns.push(processor.get_cache_col());
        res.push(processor.data);
    }

    let length_of_res = res.len();

    Dataframe {
        data: res,
        columns: columns,
        indices: create_dataframe_indices(length_of_head_row - 1),
        data_orientation: DataOrientation::Vertical,
        size: (length_of_res, length_of_head_row - 1),
    }
}

/// New dataframe if data_orientation is strict mode
/// Please make sure the first element of data is an index series.
///
/// example:
/// ```rust,ignore
/// use tiny_df::prelude::*;
/// let data = vec![
///     vec![DataframeData::String("index".into()), DataframeData::Id(0), DataframeData::Id(2), DataframeData::Id(5)],
///     vec![DataframeData::String("v".into()), DataframeData::Short(10), DataframeData::Short(11), DataframeData::Short(12)],
/// ];
/// let df = df_from_vec_s(data);
/// println!("{:#?}", df);
/// ```
#[cfg(feature = "strict")]
fn df_from_vec_s(data: D2) -> Dataframe {
    let col_len = data.len() - 1;
    let mut res = vec![Vec::<DataframeData>::new(); col_len];

    // data iterator
    let mut itr = data.into_iter();

    // 1st element of data must be indices
    let mut indices = itr.next().unwrap();
    // 1st element of indices is a tag
    indices.remove(0);
    let indices_len = indices.len();

    let mut columns_name = vec![];
    let mut columns_type: Vec<DataType> = vec![];

    for (idx, mut d) in itr.enumerate() {
        columns_name.push(d.remove(0));
        columns_type.push(d.iter().next().unwrap().into());
        res[idx] = d.drain(..indices_len).collect();
    }

    let columns = columns_name
        .into_iter()
        .zip(columns_type.into_iter())
        .map(|(n, t)| DataframeColumn::new(n.to_string(), t))
        .collect();

    Dataframe {
        data: res,
        columns,
        indices,
        data_orientation: DataOrientation::Strict,
        size: (indices_len, col_len),
    }
}

/// create an indices for a dataframe
fn create_dataframe_indices(len: usize) -> Vec<Index> {
    (0..len).map(|i| Index::Id(i as u64)).collect()
}

impl Dataframe {
    /// Dataframe constructor
    /// Accepting tree kinds of data:
    /// 1. in horizontal direction, columns name is the first row
    /// 2. in vertical direction, columns name is the first columns
    /// 3. none direction, raw data
    pub fn new<T>(data: D2, data_orientation: T, columns: Vec<DataframeColumn>) -> Self
    where
        T: Into<DataOrientation>,
    {
        if Dataframe::is_empty(&data) || columns.len() == 0 {
            return Dataframe::default();
        }
        match data_orientation.into() {
            DataOrientation::Horizontal => new_df_h_col(data, columns),
            DataOrientation::Vertical => new_df_v_col(data, columns),
            #[cfg(feature = "strict")]
            DataOrientation::Strict => new_df_strict_col(data, columns),
            DataOrientation::Raw => new_df_n(data),
        }
    }

    /// Dataframe constructor
    /// From a 2d vector
    pub fn from_vec<T>(data: D2, data_orientation: T) -> Self
    where
        T: Into<DataOrientation>,
    {
        if Dataframe::is_empty(&data) {
            return Dataframe::default();
        }
        match data_orientation.into() {
            DataOrientation::Horizontal => df_from_vec_h(data),
            DataOrientation::Vertical => df_from_vec_v(data),
            #[cfg(feature = "strict")]
            DataOrientation::Strict => df_from_vec_s(data),
            DataOrientation::Raw => new_df_n(data),
        }
    }

    /// Dataframe constructor
    /// From a Series vector. When using series to construct a Dataframe,
    /// horizontal data orientation is not recommend, due to an extra transpose step.
    pub fn from_series<T>(series: Vec<Series>, data_orientation: T) -> Self
    where
        T: Into<DataOrientation>,
    {
        let data = series.into_iter().map(|i| i.into()).collect();
        if Dataframe::is_empty(&data) {
            return Dataframe::default();
        }
        match data_orientation.into() {
            DataOrientation::Horizontal => {
                let mut r = df_from_vec_v(data);
                r.transpose();
                r
            }
            DataOrientation::Vertical => df_from_vec_v(data),
            #[cfg(feature = "strict")]
            DataOrientation::Strict => todo!(),
            DataOrientation::Raw => new_df_n(data),
        }
    }
}

#[cfg(test)]
mod test_constructor {
    use chrono::NaiveDate;

    use crate::prelude::*;
    use crate::{d2, df};

    const DIVIDER: &'static str = "-------------------------------------------------------------";

    #[test]
    fn test_df_new_h() {
        let data: D2 = d2![
            ["date", "object", "value"],
            [NaiveDate::from_ymd(2000, 1, 1), "A", 5],
        ];
        let df = Dataframe::from_vec(data, "h");
        println!("{:#?}", df);
        println!("{:?}", DIVIDER);

        let data: D2 = d2![
            ["date", "object"],
            [NaiveDate::from_ymd(2000, 1, 1), "A", 5],
            [NaiveDate::from_ymd(2010, 6, 1), "B", 23, "out of bound",],
            [NaiveDate::from_ymd(2020, 10, 1), 22, 38,],
        ];
        let df = Dataframe::from_vec(data, "h");
        println!("{:#?}", df);
        println!("{:?}", DIVIDER);
    }

    #[test]
    fn test_df_new_v() {
        let data: D2 = d2![
            [
                "date",
                NaiveDate::from_ymd(2000, 1, 1),
                NaiveDate::from_ymd(2010, 6, 1),
                NaiveDate::from_ymd(2020, 10, 1),
            ],
            ["object", "A", "B", "C"],
            ["value", 5, "wrong num", 23],
        ];
        let df = Dataframe::from_vec(data, "V");
        println!("{:#?}", df);
        println!("{:?}", DIVIDER);

        let data: D2 = d2![
            [
                "date",
                NaiveDate::from_ymd(2000, 1, 1),
                NaiveDate::from_ymd(2010, 6, 1),
            ],
            ["object", "A", "B", "C"],
            ["value", 5, 23],
        ];
        let df = Dataframe::from_vec(data, "v");
        println!("{:#?}", df);
        println!("{:?}", DIVIDER);

        let data: D2 = d2![["date",], ["object",], ["value",],];
        let df = Dataframe::from_vec(data, "v");
        println!("{:#?}", df);
        println!("{:?}", DIVIDER);
    }

    #[test]
    fn test_df_new_h_col() {
        let data: D2 = d2![
            [NaiveDate::from_ymd(2000, 1, 1), "A", 5],
            [NaiveDate::from_ymd(2010, 6, 1), "B", 23, "out of bound",],
            [NaiveDate::from_ymd(2020, 10, 1), 22, 38,],
            [NaiveDate::from_ymd(2030, 5, 1), DataframeData::None, 3,],
        ];
        let col = vec![
            DataframeColumn::new("date", DataType::Date),
            DataframeColumn::new("object", DataType::String),
            DataframeColumn::new("value", DataType::Short),
        ];
        let df = Dataframe::new(data, "h", col);
        println!("{:#?}", df);
        println!("{:?}", DIVIDER);
    }

    #[test]
    fn test_df_new_v_col() {
        let data: D2 = d2![
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
            DataframeColumn::new("date", DataType::Date),
            DataframeColumn::new("object", DataType::String),
            DataframeColumn::new("value", DataType::Short),
        ];
        let df = Dataframe::new(data, "v", col);
        println!("{:#?}", df);
        println!("{:?}", DIVIDER);
    }

    #[test]
    fn test_df_macros() {
        let df = df![
            "v";
            "date" => [
                NaiveDate::from_ymd(2000, 1, 1),
                NaiveDate::from_ymd(2010, 6, 1),
                NaiveDate::from_ymd(2020, 10, 1),
                NaiveDate::from_ymd(2030, 10, 1),
            ],
            "val" => [1,2,3,4]
        ];

        println!("{:#?}", df);

        let df = df![
            "h";
            "date" => [
                NaiveDate::from_ymd(2000, 1, 1),
                NaiveDate::from_ymd(2010, 6, 1),
                NaiveDate::from_ymd(2020, 10, 1),
                NaiveDate::from_ymd(2030, 10, 1),
            ],
            "val" => [1,2,3,4],
        ];

        println!("{:#?}", df);

        let df = df![
            "date" => [
                NaiveDate::from_ymd(2000, 1, 1),
                NaiveDate::from_ymd(2010, 6, 1),
                NaiveDate::from_ymd(2020, 10, 1),
                NaiveDate::from_ymd(2030, 10, 1),
            ],
            "val" => [1,2,3,4],
        ];

        println!("{:#?}", df);

        let df = df![
            "h";
            ["date", "TAG", "NUM"],
            [NaiveDate::from_ymd(2000, 1, 1), "A", 5],
            [NaiveDate::from_ymd(2010, 6, 1), "B", 23, "out of bound",],
            [NaiveDate::from_ymd(2020, 10, 1), 22, 38,],
            [NaiveDate::from_ymd(2030, 5, 1), DataframeData::None, 3,],
        ];

        println!("{:#?}", df);

        let df = df![
            [
                "date",
                NaiveDate::from_ymd(2000, 1, 1),
                NaiveDate::from_ymd(2010, 6, 1),
            ],
            ["object", "A", "B", "C"],
            ["value", 5, 23],
        ];

        println!("{:#?}", df);
    }
}
