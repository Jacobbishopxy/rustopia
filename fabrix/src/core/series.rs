//! Fabrix Series

use itertools::Itertools;
use polars::prelude::{
    AnyValue, DataType, NamedFrom, NewChunkedArray, Series as PSeries, UInt32Chunked,
};

use super::{Value, IDX};
use crate::{FabrixError, FabrixResult};

/// Series is a data structure used in Fabrix crate, it wrapped `polars` Series and provides
/// additional customized functionalities
#[derive(Debug, Clone)]
pub struct Series(pub(crate) PSeries);

impl Series {
    /// new from an existed PSeries
    pub fn new(s: PSeries) -> Self {
        Series(s)
    }

    /// new Series from an integer type (Rust standard type)
    pub fn from_integer<'a, I>(value: &I) -> Self
    where
        I: Into<AnyValue<'a>> + Copy,
    {
        from_integer(value.clone().into())
    }

    /// new Series from a range
    pub fn from_range<'a, I>(range: &[I; 2]) -> Self
    where
        I: Into<AnyValue<'a>> + Copy,
    {
        from_range([range[0].into(), range[1].into()])
    }

    /// get Series' name
    pub fn name(&self) -> &str {
        self.0.name()
    }

    /// rename Series
    pub fn rename(&mut self, name: &str) -> &mut Self {
        self.0.rename(name);
        self
    }

    /// show data
    pub fn data(&self) -> &PSeries {
        &self.0
    }

    /// show data length
    pub fn len(&self) -> usize {
        self.data().len()
    }

    /// show PSeries type
    pub fn dtype(&self) -> &DataType {
        &self.0.dtype()
    }

    /// head, if length is `None`, return a series only contains the first element
    pub fn head(&self, length: Option<usize>) -> FabrixResult<Series> {
        let len = self.len();

        match length {
            Some(l) => {
                if l >= self.len() {
                    Err(FabrixError::new_common_error(format!(
                        "length {:?} our of len {:?} boundary",
                        length, len
                    )))
                } else {
                    Ok(self.0.head(length).into())
                }
            }
            None => Ok(self.0.head(Some(1)).into()),
        }
    }

    /// tail, if length is `None`, return a series only contains the last element
    pub fn tail(&self, length: Option<usize>) -> FabrixResult<Series> {
        let len = self.len();

        match length {
            Some(l) => {
                if l >= self.len() {
                    Err(FabrixError::new_common_error(format!(
                        "length {:?} our of len {:?} boundary",
                        length, len
                    )))
                } else {
                    Ok(self.0.tail(length).into())
                }
            }
            None => Ok(self.0.tail(Some(1)).into()),
        }
    }

    /// get a cloned value by idx
    pub fn get(&self, idx: usize) -> FabrixResult<Value> {
        let len = self.len();
        if idx >= len {
            return Err(FabrixError::new_common_error(format!(
                "index {:?} out of len {:?} boundary",
                idx, len
            )));
        }
        Ok(self.0.get(idx).into())
    }

    /// take a cloned slice by an indices array
    pub fn take(&self, indices: &[u32]) -> FabrixResult<Series> {
        let rng = UInt32Chunked::new_from_slice(IDX, indices);
        Ok(Series::new(self.0.take(&rng)?))
    }

    /// check Series whether contains a value
    pub fn contains<'a>(&self, val: &Value<'a>) -> bool {
        self.into_iter().contains(&Some(val.clone()))
    }

    /// find index
    pub fn find_index<'a>(&self, val: &Value<'a>) -> Option<usize> {
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
    pub fn find_indices(&self, series: &Series) -> Vec<usize> {
        self.into_iter().enumerate().fold(vec![], |sum, (idx, e)| {
            let mut sum = sum;
            if series.into_iter().contains(&e) {
                sum.push(idx);
            }
            sum
        })
    }

    /// concat another series to current series
    pub fn concat(&mut self, series: &Series) -> FabrixResult<()> {
        self.0.append(&series.0)?;
        Ok(())
    }

    /// push a value at the end of the series, self mutating
    pub fn push<'a>(&mut self, value: &Value<'a>) {
        todo!()
    }

    /// insert a value into the series by idx, self mutating
    pub fn insert<'a>(&mut self, idx: usize, value: &Value<'a>) {
        todo!()
    }
}

/// new Series from an AnyValue (integer specific)
fn from_integer<'a>(val: AnyValue<'a>) -> Series {
    match val {
        AnyValue::UInt8(e) => {
            let s: Vec<_> = (0..e).collect();
            Series(PSeries::new(IDX, s))
        }
        AnyValue::UInt16(e) => {
            let s: Vec<_> = (0..e).collect();
            Series(PSeries::new(IDX, s))
        }
        AnyValue::UInt32(e) => {
            let s: Vec<_> = (0..e).collect();
            Series(PSeries::new(IDX, s))
        }
        AnyValue::UInt64(e) => {
            let s: Vec<_> = (0..e).collect();
            Series(PSeries::new(IDX, s))
        }
        AnyValue::Int8(e) => {
            let s: Vec<_> = (0..e).collect();
            Series(PSeries::new(IDX, s))
        }
        AnyValue::Int16(e) => {
            let s: Vec<_> = (0..e).collect();
            Series(PSeries::new(IDX, s))
        }
        AnyValue::Int32(e) => {
            let s: Vec<_> = (0..e).collect();
            Series(PSeries::new(IDX, s))
        }
        AnyValue::Int64(e) => {
            let s: Vec<_> = (0..e).collect();
            Series(PSeries::new(IDX, s))
        }
        _ => unimplemented!(),
    }
}

/// new Series from a range of AnyValue (integer specific)
fn from_range<'a>(rng: [AnyValue<'a>; 2]) -> Series {
    match rng {
        [AnyValue::UInt8(s), AnyValue::UInt8(e)] => {
            let s: Vec<_> = (s..e).collect();
            Series(PSeries::new(IDX, s))
        }
        [AnyValue::UInt16(s), AnyValue::UInt16(e)] => {
            let s: Vec<_> = (s..e).collect();
            Series(PSeries::new(IDX, s))
        }
        [AnyValue::UInt32(s), AnyValue::UInt32(e)] => {
            let s: Vec<_> = (s..e).collect();
            Series(PSeries::new(IDX, s))
        }
        [AnyValue::UInt64(s), AnyValue::UInt64(e)] => {
            let s: Vec<_> = (s..e).collect();
            Series(PSeries::new(IDX, s))
        }
        [AnyValue::Int8(s), AnyValue::Int8(e)] => {
            let s: Vec<_> = (s..e).collect();
            Series(PSeries::new(IDX, s))
        }
        [AnyValue::Int16(s), AnyValue::Int16(e)] => {
            let s: Vec<_> = (s..e).collect();
            Series(PSeries::new(IDX, s))
        }
        [AnyValue::Int32(s), AnyValue::Int32(e)] => {
            let s: Vec<_> = (s..e).collect();
            Series(PSeries::new(IDX, s))
        }
        [AnyValue::Int64(s), AnyValue::Int64(e)] => {
            let s: Vec<_> = (s..e).collect();
            Series(PSeries::new(IDX, s))
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
            .map(|v| v.map(|i| Value::new($any_val(i))))
            .collect::<Vec<_>>()
            .into_iter();

        SeriesIntoIterator { iter }
    }};
    ($state1:expr, $state2:expr, $any_val:expr) => {{
        let iter = $state1
            .unwrap()
            .into_iter()
            .map(|v| v.map(|i| Value::new($any_val(i, $state2))))
            .collect::<Vec<_>>()
            .into_iter();

        SeriesIntoIterator { iter }
    }};
}

/// FSeries IntoIterator implementation
impl<'a> IntoIterator for &'a Series {
    type Item = Option<Value<'a>>;

    type IntoIter = SeriesIntoIterator<'a>;

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

pub struct SeriesIntoIterator<'a> {
    iter: std::vec::IntoIter<Option<Value<'a>>>,
}

impl<'a> Iterator for SeriesIntoIterator<'a> {
    type Item = Option<Value<'a>>;

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

impl From<PSeries> for Series {
    fn from(s: PSeries) -> Self {
        Series::new(s)
    }
}

#[cfg(test)]
mod test_fabrix_series {
    use super::*;
    use crate::series;

    #[test]
    fn test_series_creation() {
        let s = Series::from_integer(&10u32);

        println!("{:?}", s);
        println!("{:?}", s.dtype());
        println!("{:?}", s.get(9));
        println!("{:?}", s.take(&[0, 3, 9]).unwrap());

        let s = Series::from_range(&[3u8, 9u8]);

        println!("{:?}", s);
        println!("{:?}", s.dtype());
        println!("{:?}", s.get(100));
        println!("{:?}", s.take(&[0, 4]).unwrap());
    }

    #[test]
    fn test_series_get() {
        let s = series!("dollars" => &["Jacob", "Sam", "James", "April"]);

        println!("{:?}", s.head(None));
        println!("{:?}", s.head(Some(2)));
        println!("{:?}", s.head(Some(10)));

        println!("{:?}", s.tail(None));
        println!("{:?}", s.tail(Some(2)));
        println!("{:?}", s.tail(Some(10)));
    }

    #[test]
    fn test_series_op() {
        let s = series!("dollars" => &["Jacob", "Sam", "James", "April"]);

        let flt = series!("cmp" => &["Jacob", "Bob"]);
        println!("{:?}", s.find_indices(&flt));

        let flt = Value::new(AnyValue::Utf8("April"));
        println!("{:?}", s.find_index(&flt));
    }

    #[test]
    fn test_series_concat() {
        let mut s1 = series!("dollars" => &["Jacob", "Sam", "James", "April"]);
        let s2 = series!("other" => &["Julia", "Jack", "John"]);

        s1.concat(&s2).unwrap();

        println!("{:?}", s1);
    }
}
