//! fabrix dataframe

use std::vec::IntoIter;

use itertools::Itertools;
use polars::{frame::select::Selection, prelude::*};
use sea_query::Value;

use crate::FResult;

/// a general naming for a default FDataFrame index
const IDX: &'static str = "index";

/// FValue is a wrapper used for holding Polars AnyValue in order to
/// satisfy type conversion between `sea_query::Value`
#[derive(Debug, PartialEq, Clone)]
pub struct FValue<'a>(AnyValue<'a>);

impl<'a> FValue<'a> {
    pub fn new(v: AnyValue<'a>) -> Self {
        FValue(v)
    }
}

/// Type conversion: from `polars` AnyValue (wrapped by FValue) to `sea-query` Value
impl<'a> From<FValue<'a>> for Value {
    fn from(val: FValue<'a>) -> Self {
        match val.0 {
            AnyValue::Null => Value::Bool(None),
            AnyValue::Boolean(v) => Value::Bool(Some(v)),
            AnyValue::Utf8(v) => Value::String(Some(Box::new(v.to_owned()))),
            AnyValue::UInt8(v) => Value::TinyInt(Some(v as i8)),
            AnyValue::UInt16(v) => Value::SmallInt(Some(v as i16)),
            AnyValue::UInt32(v) => Value::Int(Some(v as i32)),
            AnyValue::UInt64(v) => Value::BigInt(Some(v as i64)),
            AnyValue::Int8(v) => Value::TinyInt(Some(v)),
            AnyValue::Int16(v) => Value::SmallInt(Some(v)),
            AnyValue::Int32(v) => Value::Int(Some(v)),
            AnyValue::Int64(v) => Value::BigInt(Some(v)),
            AnyValue::Float32(v) => Value::Float(Some(v)),
            AnyValue::Float64(v) => Value::Double(Some(v)),
            AnyValue::Date32(_) => todo!(),
            AnyValue::Date64(_) => todo!(),
            AnyValue::Time64(_, _) => todo!(),
            AnyValue::Duration(_, _) => todo!(),
            AnyValue::List(_) => todo!(),
        }
    }
}

/// Type conversion: from `polars` AnyValue (wrapped by FValue) to `polars` DataType
impl<'a> From<FValue<'a>> for DataType {
    fn from(val: FValue<'a>) -> Self {
        match val.0 {
            AnyValue::Null => DataType::Null,
            AnyValue::Boolean(_) => DataType::Boolean,
            AnyValue::Utf8(_) => DataType::Utf8,
            AnyValue::UInt8(_) => DataType::Utf8,
            AnyValue::UInt16(_) => DataType::UInt16,
            AnyValue::UInt32(_) => DataType::UInt32,
            AnyValue::UInt64(_) => DataType::UInt64,
            AnyValue::Int8(_) => DataType::Int8,
            AnyValue::Int16(_) => DataType::Int16,
            AnyValue::Int32(_) => DataType::Int32,
            AnyValue::Int64(_) => DataType::Int64,
            AnyValue::Float32(_) => DataType::Float32,
            AnyValue::Float64(_) => DataType::Float64,
            AnyValue::Date32(_) => DataType::Date32,
            AnyValue::Date64(_) => DataType::Date64,
            AnyValue::Time64(_, tu) => DataType::Time64(tu),
            AnyValue::Duration(_, tu) => DataType::Duration(tu),
            AnyValue::List(_) => unimplemented!(),
        }
    }
}

impl<'a> PartialEq<FValue<'a>> for &FValue<'a> {
    fn eq(&self, other: &FValue<'a>) -> bool {
        self.0 == other.0
    }
}

/// FValue utilities
impl<'a> FValue<'a> {
    /// default value for every variant
    pub fn new_default() -> Self {
        todo!()
    }
}

/// FSeries is a Series structure used for Fabrix crate, it wrapped `polars` Series and provides
/// additional customized functionalities
#[derive(Debug, Clone)]
pub struct FSeries(pub(crate) Series);

impl FSeries {
    /// new from an existed Series
    pub fn new(s: Series) -> Self {
        FSeries(s)
    }

    /// new FSeries from an integer type (Rust standard type)
    pub fn from_integer<'a, I>(value: &I) -> Self
    where
        I: Into<AnyValue<'a>> + Copy,
    {
        from_integer(value.clone().into())
    }

    /// new FSeries from a range
    pub fn from_range<'a, I>(range: &[I; 2]) -> Self
    where
        I: Into<AnyValue<'a>> + Copy,
    {
        from_range([range[0].into(), range[1].into()])
    }

    /// get series' name
    pub fn name(&self) -> &str {
        self.0.name()
    }

    /// rename series
    pub fn rename(&mut self, name: &str) -> &mut Self {
        self.0.rename(name);
        self
    }

    /// show data
    pub fn data(&self) -> &Series {
        &self.0
    }

    /// show data length
    pub fn len(&self) -> usize {
        self.data().len()
    }

    /// show series type
    pub fn dtype(&self) -> &DataType {
        &self.0.dtype()
    }

    /// take a cloned slice by an indices array
    pub fn take(&self, indices: &[u32]) -> FResult<FSeries> {
        let rng = UInt32Chunked::new_from_slice(IDX, indices);
        Ok(FSeries::new(self.0.take(&rng)?))
    }

    /// check FSeries whether contains a value
    pub fn contains<'a>(&self, val: &FValue<'a>) -> bool {
        self.into_iter().contains(&Some(val.clone()))
    }

    /// find index
    pub fn find_index<'a>(&self, val: &FValue<'a>) -> Option<usize> {
        self.into_iter().position(|e| {
            if let Some(v) = e.as_ref() {
                if v == val {
                    return true;
                }
            }
            false
        })
    }

    /// find indices array
    pub fn find_indices(&self, series: &FSeries) -> Vec<usize> {
        self.into_iter().enumerate().fold(vec![], |sum, (idx, e)| {
            let mut sum = sum;
            if series.into_iter().contains(&e) {
                sum.push(idx);
            }
            sum
        })
    }
}

/// new FSeries from an AnyValue (integer specific)
fn from_integer<'a>(val: AnyValue<'a>) -> FSeries {
    match val {
        AnyValue::UInt8(e) => {
            let s: Vec<_> = (0..e).collect();
            FSeries(Series::new(IDX, s))
        }
        AnyValue::UInt16(e) => {
            let s: Vec<_> = (0..e).collect();
            FSeries(Series::new(IDX, s))
        }
        AnyValue::UInt32(e) => {
            let s: Vec<_> = (0..e).collect();
            FSeries(Series::new(IDX, s))
        }
        AnyValue::UInt64(e) => {
            let s: Vec<_> = (0..e).collect();
            FSeries(Series::new(IDX, s))
        }
        AnyValue::Int8(e) => {
            let s: Vec<_> = (0..e).collect();
            FSeries(Series::new(IDX, s))
        }
        AnyValue::Int16(e) => {
            let s: Vec<_> = (0..e).collect();
            FSeries(Series::new(IDX, s))
        }
        AnyValue::Int32(e) => {
            let s: Vec<_> = (0..e).collect();
            FSeries(Series::new(IDX, s))
        }
        AnyValue::Int64(e) => {
            let s: Vec<_> = (0..e).collect();
            FSeries(Series::new(IDX, s))
        }
        _ => unimplemented!(),
    }
}

/// new FSeries from a range of AnyValue (integer specific)
fn from_range<'a>(rng: [AnyValue<'a>; 2]) -> FSeries {
    match rng {
        [AnyValue::UInt8(s), AnyValue::UInt8(e)] => {
            let s: Vec<_> = (s..e).collect();
            FSeries(Series::new(IDX, s))
        }
        [AnyValue::UInt16(s), AnyValue::UInt16(e)] => {
            let s: Vec<_> = (s..e).collect();
            FSeries(Series::new(IDX, s))
        }
        [AnyValue::UInt32(s), AnyValue::UInt32(e)] => {
            let s: Vec<_> = (s..e).collect();
            FSeries(Series::new(IDX, s))
        }
        [AnyValue::UInt64(s), AnyValue::UInt64(e)] => {
            let s: Vec<_> = (s..e).collect();
            FSeries(Series::new(IDX, s))
        }
        [AnyValue::Int8(s), AnyValue::Int8(e)] => {
            let s: Vec<_> = (s..e).collect();
            FSeries(Series::new(IDX, s))
        }
        [AnyValue::Int16(s), AnyValue::Int16(e)] => {
            let s: Vec<_> = (s..e).collect();
            FSeries(Series::new(IDX, s))
        }
        [AnyValue::Int32(s), AnyValue::Int32(e)] => {
            let s: Vec<_> = (s..e).collect();
            FSeries(Series::new(IDX, s))
        }
        [AnyValue::Int64(s), AnyValue::Int64(e)] => {
            let s: Vec<_> = (s..e).collect();
            FSeries(Series::new(IDX, s))
        }
        _ => unimplemented!(),
    }
}

/// fs_iter macro: converting a Series to an iterator of Vec<FValue> and store it into FSeriesIntoIterator
macro_rules! fs_iter {
    ($state:expr, $any_val:expr) => {{
        let iter = $state
            .unwrap()
            .into_iter()
            .map(|v| v.map(|i| FValue::new($any_val(i))))
            .collect::<Vec<_>>()
            .into_iter();

        FSeriesIntoIterator { iter }
    }};
    ($state1:expr, $state2:expr, $any_val:expr) => {{
        let iter = $state1
            .unwrap()
            .into_iter()
            .map(|v| v.map(|i| FValue::new($any_val(i, $state2))))
            .collect::<Vec<_>>()
            .into_iter();

        FSeriesIntoIterator { iter }
    }};
}

/// FSeries IntoIterator implementation
impl<'a> IntoIterator for &'a FSeries {
    type Item = Option<FValue<'a>>;

    type IntoIter = FSeriesIntoIterator<'a>;

    fn into_iter(self) -> Self::IntoIter {
        match self.dtype() {
            DataType::Boolean => fs_iter!(self.0.bool(), AnyValue::Boolean),
            DataType::UInt8 => fs_iter!(self.0.u8(), AnyValue::UInt8),
            DataType::UInt16 => fs_iter!(self.0.u16(), AnyValue::UInt16),
            DataType::UInt32 => fs_iter!(self.0.u32(), AnyValue::UInt32),
            DataType::UInt64 => fs_iter!(self.0.u64(), AnyValue::UInt64),
            DataType::Int8 => fs_iter!(self.0.i8(), AnyValue::Int8),
            DataType::Int16 => fs_iter!(self.0.i16(), AnyValue::Int16),
            DataType::Int32 => fs_iter!(self.0.i32(), AnyValue::Int32),
            DataType::Int64 => fs_iter!(self.0.i64(), AnyValue::Int64),
            DataType::Float32 => fs_iter!(self.0.f32(), AnyValue::Float32),
            DataType::Float64 => fs_iter!(self.0.f64(), AnyValue::Float64),
            DataType::Utf8 => fs_iter!(self.0.utf8(), AnyValue::Utf8),
            DataType::Date32 => fs_iter!(self.0.date32(), AnyValue::Date32),
            DataType::Date64 => fs_iter!(self.0.date64(), AnyValue::Date64),
            DataType::Time64(tu) => fs_iter!(self.0.time64_nanosecond(), *tu, AnyValue::Time64),
            _ => unimplemented!(),
        }
    }
}

pub struct FSeriesIntoIterator<'a> {
    iter: std::vec::IntoIter<Option<FValue<'a>>>,
}

impl<'a> Iterator for FSeriesIntoIterator<'a> {
    type Item = Option<FValue<'a>>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.iter.next() {
            Some(i) => match i {
                Some(a) => Some(Some(a)),
                None => None,
            },
            None => None,
        }
    }
}

/// FDataFrame is a DataFrame structure used for Fabrix crate, it wrapped `polars` Series as DF index and
/// `polars` DataFrame for holding data
#[derive(Debug, Clone)]
pub struct FDataFrame {
    data: DataFrame,
    index: FSeries,
}

impl FDataFrame {
    /// FDataFrame constructor.
    /// Index column must be given in the DataFrame, and it will be removed consequently.
    pub fn new(df: DataFrame, index_name: &str) -> FResult<Self> {
        let idx = df.column(index_name)?.clone();
        let mut df = df;
        df.drop_in_place(index_name)?;

        Ok(FDataFrame {
            data: df,
            index: FSeries::new(idx),
        })
    }

    /// FDataFrame constructor.
    /// From a DataFrame, auto generate index
    pub fn from_df(df: DataFrame) -> Self {
        let h = df.height() as u64;

        let index = FSeries::from_integer(&h);

        FDataFrame { data: df, index }
    }

    /// get a cloned column
    pub fn get_column(&self, name: &str) -> Option<FSeries> {
        match self.data.column(name) {
            Ok(s) => Some(FSeries::new(s.clone())),
            Err(_) => None,
        }
    }

    /// get a vector of cloned columns
    pub fn get_columns<'a, S>(&self, names: S) -> Option<Vec<FSeries>>
    where
        S: Selection<'a, &'a str>,
    {
        match self.data.select_series(names) {
            Ok(r) => Some(r.into_iter().map(|s| FSeries::new(s)).collect()),
            Err(_) => None,
        }
    }

    /// get a reference of FDataFrame's data
    pub fn data(&self) -> &DataFrame {
        &self.data
    }

    /// get a reference of FDataFrame's index
    pub fn index(&self) -> &FSeries {
        &self.index
    }

    /// get FDataFrame column info
    pub fn get_column_schema(&self) -> Vec<Field> {
        self.data.schema().fields().clone()
    }

    /// take cloned rows by an indices array
    pub fn take_rows_by_indices(&self, indices: &[u32]) -> FResult<FDataFrame> {
        let idx = UInt32Chunked::new_from_slice(IDX, indices);

        let data = self.data.take(&idx)?;

        Ok(FDataFrame {
            data,
            index: self.index.take(indices)?,
        })
    }

    /// take cloned FDataFrame by an index FSeries
    pub fn take_rows(&self, index: &FSeries) -> FResult<FDataFrame> {
        let idx = self.index.find_indices(index);
        let idx = idx.into_iter().map(|i| i as u32).collect::<Vec<_>>();

        Ok(self.take_rows_by_indices(&idx[..])?)
    }

    /// take cloned FDataFrame by column names
    pub fn take_cols<'a, S>(&self, cols: S) -> FResult<FDataFrame>
    where
        S: Selection<'a, &'a str>,
    {
        let data = self.data.select(cols)?;
        Ok(FDataFrame {
            data,
            index: self.index.clone(),
        })
    }

    pub fn row_iter(&self) -> IntoIter<Vec<AnyValue>> {
        todo!()
    }
}

#[cfg(test)]
mod test_fabrix {

    use super::*;
    use polars::df;
    // use polars::prelude::*;

    #[test]
    fn test_series() {
        let s = FSeries::from_integer(&10u32);

        println!("{:?}", s);

        println!("{:?}", s.dtype());

        println!("{:?}", s.take(&[0, 3, 9]));

        let s = FSeries::from_range(&[3u8, 9u8]);

        println!("{:?}", s);

        println!("{:?}", s.dtype());

        println!("{:?}", s.take(&[0, 4]));
    }

    #[test]
    fn test_series_op() {
        let s = Series::new("dollars", &["Jacob", "Sam", "James", "April"]);
        let s = FSeries::new(s);

        let flt = FSeries::new(Series::new("cmp", &["Jacob", "Bob"]));
        println!("{:?}", s.find_indices(&flt));

        let flt = FValue::new(AnyValue::Utf8("April"));
        println!("{:?}", s.find_index(&flt));
    }

    #[test]
    fn test_df() {
        let df = df![
            "names" => ["Jacob", "Sam", "Jason"],
            "ord" => [1,2,3],
            "val" => [Some(10), None, Some(8)]
        ]
        .unwrap();
        let fdf = FDataFrame::from_df(df);

        println!("{:?}", fdf);

        println!("{:?}", fdf.get_column_schema());

        println!("{:?}", fdf.get_column("names").unwrap());
    }

    #[test]
    fn test_df_new() {
        let df = df![
            "names" => ["Jacob", "Sam", "Jason"],
            "ord" => [1,2,3],
            "val" => [Some(10), None, Some(8)]
        ]
        .unwrap();
        let fdf = FDataFrame::new(df, "ord").unwrap();

        println!("{:?}", fdf);

        println!("{:?}", fdf.get_column_schema());

        println!("{:?}", fdf.get_column("names").unwrap());
    }

    #[test]
    fn test_polars_df() {
        let df = df![
            "names" => ["Jacob", "Sam", "Jason"],
            "ord" => [1,2,3],
            "val" => [Some(10), None, Some(8)]
        ]
        .unwrap();

        let iterator = [0, 2].into_iter();

        println!("{:?}", df.take_iter(iterator).unwrap());

        let idx = UInt32Chunked::new_from_slice("index", &[0, 2]);

        println!("{:?}", df.take(&idx).unwrap());
    }

    #[test]
    fn test_df_op() {
        let df = df![
            "names" => ["Jacob", "Sam", "James"],
            "ord" => [1,2,3],
            "val" => [Some(10), None, Some(8)]
        ]
        .unwrap();

        let fdf = FDataFrame::new(df, "ord").unwrap();

        println!("{:?}", fdf.get_columns(&["names", "val"]).unwrap());

        println!("{:?}", fdf.take_rows_by_indices(&[0, 2]));

        println!("{:?}", fdf.take_cols(&["names", "val"]).unwrap());

        let flt = FSeries::new(Series::new(IDX, &[1, 3]));

        println!("{:?}", fdf.take_rows(&flt));
    }
}
