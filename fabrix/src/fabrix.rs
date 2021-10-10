//! fabrix dataframe

use std::vec::IntoIter;

use polars::{frame::select::Selection, prelude::*};
use sea_query::Value;

use crate::FResult;

/// a general naming for a default FDataFrame index
const IDX: &'static str = "index";

/// FValue is a wrapper used for holding Polars AnyValue in order to
/// satisfy type conversion between `sea_query::Value`
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
pub struct FSeries(Series);

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

    pub fn name(&self) -> &str {
        self.0.name()
    }

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

    pub fn get_columns<'a, S>(&self, names: S) -> Option<Vec<FSeries>>
    where
        S: Selection<'a, &'a str>,
    {
        match self.data.select_series(names) {
            Ok(r) => Some(r.into_iter().map(|s| FSeries::new(s)).collect()),
            Err(_) => None,
        }
    }

    pub fn data(&self) -> &DataFrame {
        &self.data
    }

    pub fn index(&self) -> &FSeries {
        &self.index
    }

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
        todo!()
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
    fn test_series_1() {
        let mut s = FSeries::from_integer(&3u32);

        println!("{:?}", s.name());

        s.rename("order");

        println!("{:?}", s.name());
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
    }

    #[test]
    fn test_polars_series() {
        let s = Series::new("dollars", &["Jacob", "Sam", "James", "April"]);

        // let iter = BooleanChunked::new_from_slice(IDX, &[true, false]);

        // println!("{:?}", s.filter_threaded(&iter, false));

        let flt = ["April", "Jacob"];

        // TODO: use this to complete `take_rows` method in FDataFrame
        let res = s
            .utf8()
            .unwrap()
            .into_iter()
            .enumerate()
            .fold(vec![], |sum, (idx, e)| {
                let mut sum = sum;
                if let Some(s) = e.as_ref() {
                    if flt.contains(s) {
                        sum.push(idx);
                    }
                }
                sum
            });

        println!("{:?}", res);
    }
}
