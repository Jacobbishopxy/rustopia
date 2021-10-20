//! Fabrix Series

use itertools::Itertools;
use polars::prelude::{
    AnyValue, DataType, Field, IntoSeries, NewChunkedArray, Series as PSeries, UInt32Chunked,
};
use polars::prelude::{
    BooleanType, Float32Type, Float64Type, Int16Type, Int32Type, Int64Type, Int8Type, UInt16Type,
    UInt32Type, UInt64Type, UInt8Type, Utf8Type,
};

use super::{oob_err, IDX};
use crate::{series, FabrixError, FabrixResult, Value};

/// Series is a data structure used in Fabrix crate, it wrapped `polars` Series and provides
/// additional customized functionalities
#[derive(Debug, Clone)]
pub struct Series(pub(crate) PSeries);

impl Series {
    /// new from an existed PSeries
    pub fn from_polars_series(s: PSeries) -> Self {
        Series(s)
    }

    /// new Series from an integer type (Rust standard type)
    pub fn from_integer<'a, I>(value: &I) -> Self
    where
        I: Into<Value<'a>> + Copy,
    {
        from_integer(value.clone().into())
    }

    /// new Series from a range
    pub fn from_range<'a, I>(range: &[I; 2]) -> Self
    where
        I: Into<Value<'a>> + Copy,
    {
        from_range([range[0].into(), range[1].into()])
    }

    /// new Series from Vec<Value>
    pub fn from_values<'a>(values: Vec<Value<'a>>, nullable: bool) -> FabrixResult<Self> {
        Ok(from_values(values, nullable)?)
    }

    /// new empty Series from field
    pub fn empty_series_from_field(field: Field, nullable: bool) -> FabrixResult<Self> {
        Ok(empty_series_from_field(field, nullable)?)
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
        self.0.dtype()
    }

    /// get series field
    pub fn field(&self) -> &Field {
        self.0.field()
    }

    /// check whether the series is empty
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// check if contains null value
    pub fn has_null(&self) -> bool {
        !self.0.is_not_null().all_true()
    }

    /// head, if length is `None`, return a series only contains the first element
    pub fn head(&self, length: Option<usize>) -> FabrixResult<Series> {
        let len = self.len();

        match length {
            Some(l) => {
                if l >= self.len() {
                    Err(oob_err(l, len))
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
                if l >= len {
                    Err(oob_err(l, len))
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
            Err(oob_err(idx, len))
        } else {
            Ok(self.0.get(idx).into())
        }
    }

    /// take a cloned slice by an indices array
    pub fn take(&self, indices: &[u32]) -> FabrixResult<Series> {
        let rng = UInt32Chunked::new_from_slice(IDX, indices);
        Ok(Series::from_polars_series(self.0.take(&rng)?))
    }

    /// slice the Series
    pub fn slice(&self, offset: i64, length: usize) -> Series {
        self.0.slice(offset, length).into()
    }

    /// check Series whether contains a value (`self.into_iter` is not zero copy)
    pub fn contains<'a>(&self, val: &Value<'a>) -> bool {
        self.into_iter().contains(val)
    }

    /// find idx by a Value (`self.into_iter` is not zero copy)
    pub fn find_index<'a>(&self, val: &Value<'a>) -> Option<usize> {
        self.into_iter().position(|ref e| e == val)
    }

    /// find idx vector by a Series (`self.into_iter` is not zero copy)
    pub fn find_indices(&self, series: &Series) -> Vec<usize> {
        self.into_iter().enumerate().fold(vec![], |sum, (idx, e)| {
            let mut sum = sum;
            if series.into_iter().contains(&e) {
                sum.push(idx);
            }
            sum
        })
    }

    /// drop nulls
    pub fn drop_nulls(&mut self) -> &mut Self {
        self.0 = self.0.drop_nulls();
        self
    }

    /// concat another series to current series
    pub fn concat(&mut self, series: Series) -> FabrixResult<&mut Self> {
        self.0.append(&series.0)?;
        Ok(self)
    }

    /// split into two series
    pub fn split(&self, idx: usize) -> FabrixResult<(Series, Series)> {
        let len = self.len();

        if idx >= len {
            Err(oob_err(idx, len))
        } else {
            let (len1, len2) = (idx, len - idx);
            Ok((self.slice(0, len1), self.slice(idx as i64, len2)))
        }
    }

    /// push a value at the end of the series, self mutation
    pub fn push<'a>(&mut self, value: Value<'a>) -> FabrixResult<&mut Self> {
        let s = from_values(vec![value], true)?;
        self.concat(s)?;
        Ok(self)
    }

    /// insert a value into the series by idx, self mutation
    pub fn insert<'a>(&mut self, idx: usize, value: Value<'a>) -> FabrixResult<&mut Self> {
        let (mut s1, s2) = self.split(idx)?;

        s1.push(value)?.concat(s2)?;
        *self = s1;

        Ok(self)
    }

    /// insert a series at a specified idx, self mutation
    pub fn insert_many<'a>(&mut self, idx: usize, series: Series) -> FabrixResult<&mut Self> {
        let (mut s1, s2) = self.split(idx)?;

        s1.concat(series)?.concat(s2)?;
        *self = s1;

        Ok(self)
    }

    /// pop the last element from the series, self mutation
    pub fn pop(&mut self) -> FabrixResult<&mut Self> {
        let len = self.len();
        if len == 0 {
            return Err(FabrixError::new_common_error("series is empty"));
        }

        *self = self.slice(0, len - 1);

        Ok(self)
    }

    /// remove a value from the series, self mutation
    pub fn remove<'a>(&mut self, idx: usize) -> FabrixResult<&mut Self> {
        let len = self.len();
        if idx >= len {
            return Err(oob_err(idx, len));
        }
        let (mut s1, s2) = (self.slice(0, idx), self.slice(idx as i64 + 1, len));

        s1.concat(s2)?;
        *self = s1;

        Ok(self)
    }

    /// remove a slice from the series, self mutation
    pub fn remove_slice<'a>(&mut self, offset: i64, length: usize) -> FabrixResult<&mut Self> {
        let len = self.len();
        let offset = if offset >= 0 {
            offset
        } else {
            len as i64 + offset
        };
        let (mut s1, s2) = (
            self.slice(0, offset as usize),
            self.slice(offset + length as i64, len),
        );

        s1.concat(s2)?;
        *self = s1;

        Ok(self)
    }
}

/// new Series from an AnyValue (integer specific)
fn from_integer<'a>(val: Value<'a>) -> Series {
    match val.0 {
        AnyValue::UInt8(e) => series!(IDX => (0..e).collect::<Vec<_>>()),
        AnyValue::UInt16(e) => series!(IDX => (0..e).collect::<Vec<_>>()),
        AnyValue::UInt32(e) => series!(IDX => (0..e).collect::<Vec<_>>()),
        AnyValue::UInt64(e) => series!(IDX => (0..e).collect::<Vec<_>>()),
        AnyValue::Int8(e) => series!(IDX => (0..e).collect::<Vec<_>>()),
        AnyValue::Int16(e) => series!(IDX => (0..e).collect::<Vec<_>>()),
        AnyValue::Int32(e) => series!(IDX => (0..e).collect::<Vec<_>>()),
        AnyValue::Int64(e) => series!(IDX => (0..e).collect::<Vec<_>>()),
        _ => unimplemented!(),
    }
}

/// new Series from a range of AnyValue (integer specific)
fn from_range<'a>(rng: [Value<'a>; 2]) -> Series {
    let [r0, r1] = rng;
    match [r0.0, r1.0] {
        [AnyValue::UInt8(s), AnyValue::UInt8(e)] => series!(IDX => (s..e).collect::<Vec<_>>()),
        [AnyValue::UInt16(s), AnyValue::UInt16(e)] => series!(IDX => (s..e).collect::<Vec<_>>()),
        [AnyValue::UInt32(s), AnyValue::UInt32(e)] => series!(IDX => (s..e).collect::<Vec<_>>()),
        [AnyValue::UInt64(s), AnyValue::UInt64(e)] => series!(IDX => (s..e).collect::<Vec<_>>()),
        [AnyValue::Int8(s), AnyValue::Int8(e)] => series!(IDX => (s..e).collect::<Vec<_>>()),
        [AnyValue::Int16(s), AnyValue::Int16(e)] => series!(IDX => (s..e).collect::<Vec<_>>()),
        [AnyValue::Int32(s), AnyValue::Int32(e)] => series!(IDX => (s..e).collect::<Vec<_>>()),
        [AnyValue::Int64(s), AnyValue::Int64(e)] => series!(IDX => (s..e).collect::<Vec<_>>()),
        _ => unimplemented!(),
    }
}

/// new Series from Vec<Value>
/// let r = values
///     .into_iter()
///     .map(|v| bool::try_from(v))
///     .collect::<FabrixResult<Vec<_>>>()?;
/// let s = ChunkedArray::<BooleanType>::new_from_slice(IDX, &r[..]);
/// Ok(Series(s.into_series()))
macro_rules! series_from_values {
    ($name:expr, $values:expr; Option<$ftype:ty>, $polars_type:ident) => {{
        let r = $values
            .into_iter()
            .map(|v| Option::<$ftype>::try_from(v))
            .collect::<$crate::FabrixResult<Vec<_>>>()?;

        let s = polars::prelude::ChunkedArray::<$polars_type>::new_from_opt_slice($name, &r[..]);
        Ok(Series(s.into_series()))
    }};
    ($name:expr, $values:expr; $ftype:ty, $polars_type:ident) => {{
        let r = $values
            .into_iter()
            .map(|v| <$ftype>::try_from(v))
            .collect::<$crate::FabrixResult<Vec<_>>>()?;

        let s = polars::prelude::ChunkedArray::<$polars_type>::new_from_slice($name, &r[..]);
        Ok(Series(s.into_series()))
    }};
    ($name:expr; Option<$ftype:ty>, $polars_type:ident) => {{
        let vec: Vec<Option<$ftype>> = vec![];
        let s = polars::prelude::ChunkedArray::<$polars_type>::new_from_opt_slice($name, &vec);
        Ok(Series(s.into_series()))
    }};
    ($name:expr; $ftype:ty, $polars_type:ident) => {{
        let vec: Vec<$ftype> = vec![];
        let s = polars::prelude::ChunkedArray::<$polars_type>::new_from_slice($name, &vec);
        Ok(Series(s.into_series()))
    }};
}

// TODO: if first element in values is Null, this function will crash
/// series from values
fn from_values<'a>(values: Vec<Value<'a>>, nullable: bool) -> FabrixResult<Series> {
    if values.len() == 0 {
        return Err(FabrixError::new_common_error("values' length is 0!"));
    }

    let dtype = values[0].0.clone();

    match dtype {
        AnyValue::Boolean(_) => match nullable {
            true => series_from_values!(IDX, values; Option<bool>, BooleanType),
            false => series_from_values!(IDX, values; bool, BooleanType),
        },
        AnyValue::Utf8(_) => match nullable {
            true => series_from_values!(IDX, values; Option<&str>, Utf8Type),
            false => series_from_values!(IDX, values; &str, Utf8Type),
        },
        AnyValue::UInt8(_) => match nullable {
            true => series_from_values!(IDX, values; Option<u8>, UInt8Type),
            false => series_from_values!(IDX, values; u8, UInt8Type),
        },
        AnyValue::UInt16(_) => match nullable {
            true => series_from_values!(IDX, values; Option<u16>, UInt16Type),
            false => series_from_values!(IDX, values; u16, UInt16Type),
        },
        AnyValue::UInt32(_) => match nullable {
            true => series_from_values!(IDX, values; Option<u32>, UInt32Type),
            false => series_from_values!(IDX, values; u32, UInt32Type),
        },
        AnyValue::UInt64(_) => match nullable {
            true => series_from_values!(IDX, values; Option<u64>, UInt64Type),
            false => series_from_values!(IDX, values; u64, UInt64Type),
        },
        AnyValue::Int8(_) => match nullable {
            true => series_from_values!(IDX, values; Option<i8>, Int8Type),
            false => series_from_values!(IDX, values; i8, Int8Type),
        },
        AnyValue::Int16(_) => match nullable {
            true => series_from_values!(IDX, values; Option<i16>, Int16Type),
            false => series_from_values!(IDX, values; i16, Int16Type),
        },
        AnyValue::Int32(_) => match nullable {
            true => series_from_values!(IDX, values; Option<i32>, Int32Type),
            false => series_from_values!(IDX, values; i32, Int32Type),
        },
        AnyValue::Int64(_) => match nullable {
            true => series_from_values!(IDX, values; Option<i64>, Int64Type),
            false => series_from_values!(IDX, values; i64, Int64Type),
        },
        AnyValue::Float32(_) => match nullable {
            true => series_from_values!(IDX, values; Option<f32>, Float32Type),
            false => series_from_values!(IDX, values; f32, Float32Type),
        },
        AnyValue::Float64(_) => match nullable {
            true => series_from_values!(IDX, values; Option<f64>, Float64Type),
            false => series_from_values!(IDX, values; f64, Float64Type),
        },
        AnyValue::Date32(_) => todo!(),
        AnyValue::Date64(_) => todo!(),
        AnyValue::Time64(_, _) => todo!(),
        _ => unimplemented!(),
    }
}

/// empty series from field
fn empty_series_from_field(field: Field, nullable: bool) -> FabrixResult<Series> {
    match field.data_type() {
        DataType::Boolean => match nullable {
            true => series_from_values!(field.name(); Option<bool>, BooleanType),
            false => series_from_values!(field.name(); bool, BooleanType),
        },
        DataType::Utf8 => match nullable {
            true => series_from_values!(field.name(); Option<String>, Utf8Type),
            false => series_from_values!(field.name(); String, Utf8Type),
        },
        DataType::UInt8 => match nullable {
            true => series_from_values!(field.name(); Option<u8>, UInt8Type),
            false => series_from_values!(field.name(); u8, UInt8Type),
        },
        DataType::UInt16 => match nullable {
            true => series_from_values!(field.name(); Option<u16>, UInt16Type),
            false => series_from_values!(field.name(); u16, UInt16Type),
        },
        DataType::UInt32 => match nullable {
            true => series_from_values!(field.name(); Option<u32>, UInt32Type),
            false => series_from_values!(field.name(); u32, UInt32Type),
        },
        DataType::UInt64 => match nullable {
            true => series_from_values!(field.name(); Option<u64>, UInt64Type),
            false => series_from_values!(field.name(); u64, UInt64Type),
        },
        DataType::Int8 => match nullable {
            true => series_from_values!(field.name(); Option<i8>, Int8Type),
            false => series_from_values!(field.name(); i8, Int8Type),
        },
        DataType::Int16 => match nullable {
            true => series_from_values!(field.name(); Option<i16>, Int16Type),
            false => series_from_values!(field.name(); i16, Int16Type),
        },
        DataType::Int32 => match nullable {
            true => series_from_values!(field.name(); Option<i32>, Int32Type),
            false => series_from_values!(field.name(); i32, Int32Type),
        },
        DataType::Int64 => match nullable {
            true => series_from_values!(field.name(); Option<i64>, Int64Type),
            false => series_from_values!(field.name(); i64, Int64Type),
        },
        DataType::Float32 => match nullable {
            true => series_from_values!(field.name(); Option<f32>, Float32Type),
            false => series_from_values!(field.name(); f32, Float32Type),
        },
        DataType::Float64 => match nullable {
            true => series_from_values!(field.name(); Option<f64>, Float64Type),
            false => series_from_values!(field.name(); f64, Float64Type),
        },
        DataType::Date32 => todo!(),
        DataType::Date64 => todo!(),
        DataType::Time64(_) => todo!(),
        _ => unimplemented!(),
    }
}

/// fs_iter macro: converting a Series to an iterator of Vec<FValue> and store it into SeriesIntoIterator
macro_rules! fs_iter {
    ($state:expr, $any_val:expr) => {{
        let iter = $state
            .unwrap()
            .into_iter()
            .map(|ov| match ov {
                Some(v) => Value($any_val(v)),
                None => Value(polars::prelude::AnyValue::Null),
            })
            .collect::<Vec<_>>()
            .into_iter();

        SeriesIntoIterator { iter }
    }};
    ($state1:expr, $state2:expr, $any_val:expr) => {{
        let iter = $state1
            .unwrap()
            .into_iter()
            .map(|ov| match ov {
                Some(v) => Value($any_val(v, $state2)),
                None => Value(polars::prelude::AnyValue::Null),
            })
            .collect::<Vec<_>>()
            .into_iter();

        SeriesIntoIterator { iter }
    }};
}

/// Series IntoIterator implementation
impl<'a> IntoIterator for &'a Series {
    type Item = Value<'a>;
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
            // temporary ignore the rest of DataType variants
            _ => unimplemented!(),
        }
    }
}

pub struct SeriesIntoIterator<'a> {
    iter: std::vec::IntoIter<Value<'a>>,
}

impl<'a> Iterator for SeriesIntoIterator<'a> {
    type Item = Value<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next()
    }
}

impl From<PSeries> for Series {
    fn from(s: PSeries) -> Self {
        Series::from_polars_series(s)
    }
}

impl From<Series> for PSeries {
    fn from(s: Series) -> Self {
        s.0
    }
}

#[cfg(test)]
mod test_fabrix_series {

    use super::*;
    use crate::{series, value};

    #[test]
    fn test_series_creation() {
        let s = Series::from_integer(&10u32);

        println!("{:?}", s);
        println!("{:?}", s.dtype());
        println!("{:?}", s.get(9));
        println!("{:?}", s.take(&[0, 3, 9]).unwrap());

        let s = Series::from_range(&[3u8, 9]);

        println!("{:?}", s);
        println!("{:?}", s.dtype());
        println!("{:?}", s.get(100));
        println!("{:?}", s.take(&[0, 4]).unwrap());

        let s = Series::from_values(
            vec![
                value!(Some("Jacob")),
                value!(Some("Jamie")),
                value!(None::<&str>),
            ],
            true,
        )
        .unwrap();

        println!("{:?}", s);
        println!("{:?}", s.dtype());
    }

    #[test]
    fn test_series_props() {
        let s = series!("yes" => &[Some(1), None, Some(2)]);
        println!("{:?}", s.has_null());

        let s = series!("no" => &[Some(1), Some(3), Some(2)]);
        println!("{:?}", s.has_null());

        let s = series!("no" => &[1, 3, 2]);
        println!("{:?}", s.has_null());
    }

    #[test]
    fn test_series_get() {
        let s = series!("dollars" => &["Jacob", "Sam", "James", "April", "Julia", "Jack", "Henry"]);

        println!("{:?}", s.head(None));
        println!("{:?}", s.head(Some(2)));
        println!("{:?}", s.head(Some(10)));

        println!("{:?}", s.tail(None));
        println!("{:?}", s.tail(Some(2)));
        println!("{:?}", s.tail(Some(10)));

        println!("{:?}", s.split(4));
    }

    #[test]
    fn test_series_op() {
        let s = series!("dollars" => &["Jacob", "Sam", "James", "April"]);

        let flt = series!("cmp" => &["Jacob", "Bob"]);
        println!("{:?}", s.find_indices(&flt));

        let flt = value!("April");
        println!("{:?}", s.find_index(&flt));
    }

    #[test]
    fn test_series_concat() {
        let mut s1 = series!("dollars" => &["Jacob", "Sam", "James", "April"]);
        let s2 = series!("other" => &["Julia", "Jack", "John"]);

        s1.concat(s2).unwrap();

        println!("{:?}", s1);
    }

    #[test]
    fn test_series_op1() {
        let mut s1 = series!("dollars" => &["Jacob", "Sam", "James", "April"]);

        let v1 = value!("Julia");
        println!("{:?}", s1.push(v1).unwrap());

        let s2 = series!(["Jackson", "Jan"]);
        println!("{:?}", s1.concat(s2).unwrap());

        let v2 = value!("Merry");
        println!("{:?}", s1.insert(2, v2).unwrap());

        let s3 = series!(["Jasmine", "Justin"]);
        println!("{:?}", s1.insert_many(3, s3).unwrap());

        println!("{:?}", s1.pop().unwrap());
        println!("{:?}", s1.remove(3).unwrap());
    }

    #[test]
    fn test_series_op2() {
        let mut s1 = series!("dollars" => &["Jacob", "Sam", "James", "April", "Julia", "Jack", "Merry", "Justin"]);

        println!("{:?}", s1.slice(3, 4));
        println!("{:?}", s1.remove_slice(3, 4));

        println!("{:?}", s1.slice(-3, 4));
        println!("{:?}", s1.remove_slice(-3, 4));
    }
}
