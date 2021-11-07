//! Fabrix Series

use itertools::Itertools;
use polars::prelude::{
    BooleanChunked, BooleanType, Float32Chunked, Float32Type, Float64Chunked, Float64Type,
    Int16Chunked, Int16Type, Int32Chunked, Int32Type, Int64Chunked, Int64Type, Int8Chunked,
    Int8Type, ObjectChunked, TakeRandom, TakeRandomUtf8, UInt16Chunked, UInt16Type, UInt32Chunked,
    UInt32Type, UInt64Chunked, UInt64Type, UInt8Chunked, UInt8Type, Utf8Chunked, Utf8Type,
};
use polars::prelude::{DataType, Field, IntoSeries, NewChunkedArray, Series as PSeries};

use super::{oob_err, FieldInfo, Stepper, IDX};
use crate::core::{
    ObjectTypeDate, ObjectTypeDateTime, ObjectTypeDecimal, ObjectTypeTime, ObjectTypeUuid,
};
use crate::{
    series, value, Date, DateTime, Decimal, FabrixError, FabrixResult, Time, Uuid, Value, ValueType,
};

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
    pub fn from_integer<I>(value: &I) -> FabrixResult<Self>
    where
        I: Into<Value> + Copy,
    {
        Ok(from_integer(value.clone().into())?)
    }

    /// new Series from a range
    pub fn from_range<'a, I>(range: &[I; 2]) -> FabrixResult<Self>
    where
        I: Into<Value> + Copy,
    {
        Ok(from_range([range[0].into(), range[1].into()])?)
    }

    /// new Series from Vec<Value>
    pub fn from_values_default_name(values: Vec<Value>, nullable: bool) -> FabrixResult<Self> {
        Ok(from_values(values, IDX, nullable)?)
    }

    /// new Series from Vec<Value> and name
    pub fn from_values(values: Vec<Value>, name: &str, nullable: bool) -> FabrixResult<Self> {
        Ok(from_values(values, name, nullable)?)
    }

    /// new empty Series from field
    pub fn empty_series_from_field(field: Field, nullable: bool) -> FabrixResult<Self> {
        Ok(empty_series_from_field(field, nullable)?)
    }

    /// rechunk: aggregate all chunks to a contiguous array of memory
    pub fn rechunk(&mut self) {
        self.0 = self.0.rechunk();
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

    /// show Series type
    pub fn dtype(&self) -> ValueType {
        self.0.dtype().into()
    }

    /// get series field
    pub fn field(&self) -> FieldInfo {
        FieldInfo::new(self.0.field().as_ref().to_owned(), self.has_null())
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
            let v = self.0.get(idx);
            Ok(value!(v))
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
    pub fn contains(&self, val: &Value) -> bool {
        self.into_iter().contains(val)
    }

    /// find idx by a Value (`self.into_iter` is not zero copy)
    pub fn find_index(&self, val: &Value) -> Option<usize> {
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
    pub fn push(&mut self, value: Value) -> FabrixResult<&mut Self> {
        let s = from_values(vec![value], IDX, true)?;
        self.concat(s)?;
        Ok(self)
    }

    /// insert a value into the series by idx, self mutation
    pub fn insert(&mut self, idx: usize, value: Value) -> FabrixResult<&mut Self> {
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
fn from_integer(val: Value) -> FabrixResult<Series> {
    match val {
        Value::U8(v) => Ok(series!(IDX => (0..v).collect::<Vec<_>>())),
        Value::U16(v) => Ok(series!(IDX => (0..v).collect::<Vec<_>>())),
        Value::U32(v) => Ok(series!(IDX => (0..v).collect::<Vec<_>>())),
        Value::U64(v) => Ok(series!(IDX => (0..v).collect::<Vec<_>>())),
        Value::I8(v) => Ok(series!(IDX => (0..v).collect::<Vec<_>>())),
        Value::I16(v) => Ok(series!(IDX => (0..v).collect::<Vec<_>>())),
        Value::I32(v) => Ok(series!(IDX => (0..v).collect::<Vec<_>>())),
        Value::I64(v) => Ok(series!(IDX => (0..v).collect::<Vec<_>>())),
        _ => Err(FabrixError::new_common_error("val is not integer")),
    }
}

/// new Series from a range of AnyValue (integer specific)
fn from_range<'a>(rng: [Value; 2]) -> FabrixResult<Series> {
    let [r0, r1] = rng;
    match [r0, r1] {
        [Value::U8(s), Value::U8(e)] => Ok(series!(IDX => (s..e).collect::<Vec<_>>())),
        [Value::U16(s), Value::U16(e)] => Ok(series!(IDX => (s..e).collect::<Vec<_>>())),
        [Value::U32(s), Value::U32(e)] => Ok(series!(IDX => (s..e).collect::<Vec<_>>())),
        [Value::U64(s), Value::U64(e)] => Ok(series!(IDX => (s..e).collect::<Vec<_>>())),
        [Value::I8(s), Value::I8(e)] => Ok(series!(IDX => (s..e).collect::<Vec<_>>())),
        [Value::I16(s), Value::I16(e)] => Ok(series!(IDX => (s..e).collect::<Vec<_>>())),
        [Value::I32(s), Value::I32(e)] => Ok(series!(IDX => (s..e).collect::<Vec<_>>())),
        [Value::I64(s), Value::I64(e)] => Ok(series!(IDX => (s..e).collect::<Vec<_>>())),
        _ => Err(FabrixError::new_common_error(
            "rng is not integer or not the same type of pair",
        )),
    }
}

// Simple conversion
impl From<PSeries> for Series {
    fn from(s: PSeries) -> Self {
        Series::from_polars_series(s)
    }
}

// Simple conversion
impl From<Series> for PSeries {
    fn from(s: Series) -> Self {
        s.0
    }
}

/// new Series from Vec<Value>
///
/// ```rust
/// let r = values
///     .into_iter()
///     .map(|v| bool::try_from(v))
///     .collect::<FabrixResult<Vec<_>>>()?;
/// let s = ChunkedArray::<BooleanType>::new_from_slice(IDX, &r[..]);
/// Ok(Series(s.into_series()))
/// ```
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

/// new Series from Vec<Value>, with nullable option
macro_rules! sfv {
    ($nullable:expr; $name:expr, $values:expr; $ftype:ty, $polars_type:ident) => {{
        match $nullable {
            true => series_from_values!($name, $values; Option<$ftype>, $polars_type),
            false => series_from_values!($name, $values; $ftype, $polars_type),
        }
    }};
    ($nullable:expr; $name:expr; $ftype:ty, $polars_type:ident) => {{
        match $nullable {
            true => series_from_values!($name; Option<$ftype>, $polars_type),
            false => series_from_values!($name; $ftype, $polars_type),
        }
}};
}

/// series from values, series type is determined by the first value, if it is null value
/// then use u64 as the default type. If values are not the same type, then return error.
fn from_values(values: Vec<Value>, name: &str, nullable: bool) -> FabrixResult<Series> {
    if values.len() == 0 {
        return Err(FabrixError::new_common_error("values' length is 0!"));
    }

    let dtype = values[0].clone();

    match dtype {
        Value::Bool(_) => sfv!(nullable; name, values; bool, BooleanType),
        Value::String(_) => sfv!(nullable; name, values; String, Utf8Type),
        Value::U8(_) => sfv!(nullable; name, values; u8, UInt8Type),
        Value::U16(_) => sfv!(nullable; name, values; u16, UInt16Type),
        Value::U32(_) => sfv!(nullable; name, values; u32, UInt32Type),
        Value::U64(_) => sfv!(nullable; name, values; u64, UInt64Type),
        Value::I8(_) => sfv!(nullable; name, values; i8, Int8Type),
        Value::I16(_) => sfv!(nullable; name, values; i16, Int16Type),
        Value::I32(_) => sfv!(nullable; name, values; i32, Int32Type),
        Value::I64(_) => sfv!(nullable; name, values; i64, Int64Type),
        Value::F32(_) => sfv!(nullable; name, values; f32, Float32Type),
        Value::F64(_) => sfv!(nullable; name, values; f64, Float64Type),
        Value::Date(_) => sfv!(nullable; name, values; Date, ObjectTypeDate),
        Value::Time(_) => sfv!(nullable; name, values; Time, ObjectTypeTime),
        Value::DateTime(_) => sfv!(nullable; name, values; DateTime, ObjectTypeDateTime),
        Value::Decimal(_) => sfv!(nullable; name, values; Decimal, ObjectTypeDecimal),
        Value::Uuid(_) => sfv!(nullable; name, values; Uuid, ObjectTypeUuid),
        Value::Null => Ok(Series::from_integer(&(values.len() as u64))?),
    }
}

/// empty series from field
fn empty_series_from_field(field: Field, nullable: bool) -> FabrixResult<Series> {
    match field.data_type() {
        DataType::Boolean => sfv!(nullable; field.name(); bool, BooleanType),
        DataType::Utf8 => sfv!(nullable; field.name(); String, Utf8Type),
        DataType::UInt8 => sfv!(nullable; field.name(); u8, UInt8Type),
        DataType::UInt16 => sfv!(nullable; field.name(); u16, UInt16Type),
        DataType::UInt32 => sfv!(nullable; field.name(); u32, UInt32Type),
        DataType::UInt64 => sfv!(nullable; field.name(); u64, UInt64Type),
        DataType::Int8 => sfv!(nullable; field.name(); i8, Int8Type),
        DataType::Int16 => sfv!(nullable; field.name(); i16, Int16Type),
        DataType::Int32 => sfv!(nullable; field.name(); i32, Int32Type),
        DataType::Int64 => sfv!(nullable; field.name(); i64, Int64Type),
        DataType::Float32 => sfv!(nullable; field.name(); f32, Float32Type),
        DataType::Float64 => sfv!(nullable; field.name(); f64, Float64Type),
        DataType::Object("Date") => todo!(),
        DataType::Object("Time") => todo!(),
        DataType::Object("DateTime") => todo!(),
        DataType::Object("Decimal") => todo!(),
        DataType::Object("Uuid") => todo!(),
        DataType::Null => sfv!(nullable; field.name(); u64, UInt64Type),
        _ => unimplemented!(),
    }
}

/// Series IntoIterator process
///
/// for instance:
/// ```rust
/// let arr = self.0.bool().unwrap();
/// SeriesIntoIterator::Bool(
///     arr.clone(),
///     Stepper::new(arr.len()),
/// )
/// ```
///
/// and custom type:
///
/// ```rust
/// let arr = self.0.as_any()
///     .downcast_ref::<ObjectChunked<ObjectTypeDate>>()
///     .unwrap();
/// SeriesIntoIterator::Date(
///     arr.clone(),
///     Stepper::new(arr.len()),
/// )
/// ```
macro_rules! sii {
    ($fn_call:expr, $series_iter_var:ident) => {{
        let arr = $fn_call.unwrap();
        $crate::core::SeriesIntoIterator::$series_iter_var(
            arr.clone(),
            $crate::core::util::Stepper::new(arr.len()),
        )
    }};
    ($fn_call:expr, $downcast_type:ident, $series_iter_var:ident) => {{
        let arr = $fn_call
            .downcast_ref::<polars::prelude::ObjectChunked<$downcast_type>>()
            .unwrap();
        $crate::core::SeriesIntoIterator::$series_iter_var(
            arr.clone(),
            $crate::core::util::Stepper::new(arr.len()),
        )
    }};
}

/// Series IntoIterator implementation
impl IntoIterator for Series {
    type Item = Value;
    type IntoIter = SeriesIntoIterator;

    fn into_iter(self) -> Self::IntoIter {
        match self.dtype() {
            ValueType::Bool => sii!(self.0.bool(), Bool),
            ValueType::U8 => sii!(self.0.u8(), U8),
            ValueType::U16 => sii!(self.0.u16(), U16),
            ValueType::U32 => sii!(self.0.u32(), U32),
            ValueType::U64 => sii!(self.0.u64(), U64),
            ValueType::I8 => sii!(self.0.i8(), I8),
            ValueType::I16 => sii!(self.0.i16(), I16),
            ValueType::I32 => sii!(self.0.i32(), I32),
            ValueType::I64 => sii!(self.0.i64(), I64),
            ValueType::F32 => sii!(self.0.f32(), F32),
            ValueType::F64 => sii!(self.0.f64(), F64),
            ValueType::String => sii!(self.0.utf8(), String),
            ValueType::Date => sii!(self.0.as_any(), Date, Date),
            ValueType::Time => sii!(self.0.as_any(), Time, Time),
            ValueType::DateTime => sii!(self.0.as_any(), DateTime, DateTime),
            ValueType::Decimal => sii!(self.0.as_any(), Decimal, Decimal),
            ValueType::Uuid => sii!(self.0.as_any(), Uuid, Uuid),
            ValueType::Null => panic!("Null value series"),
        }
    }
}

/// IntoIterator
pub enum SeriesIntoIterator {
    Id(UInt64Chunked, Stepper),
    Bool(BooleanChunked, Stepper),
    U8(UInt8Chunked, Stepper),
    U16(UInt16Chunked, Stepper),
    U32(UInt32Chunked, Stepper),
    U64(UInt64Chunked, Stepper),
    I8(Int8Chunked, Stepper),
    I16(Int16Chunked, Stepper),
    I32(Int32Chunked, Stepper),
    I64(Int64Chunked, Stepper),
    F32(Float32Chunked, Stepper),
    F64(Float64Chunked, Stepper),
    String(Utf8Chunked, Stepper),
    Date(ObjectChunked<Date>, Stepper),
    Time(ObjectChunked<Time>, Stepper),
    DateTime(ObjectChunked<DateTime>, Stepper),
    Decimal(ObjectChunked<Decimal>, Stepper),
    Uuid(ObjectChunked<Uuid>, Stepper),
}

/// The `next` function for Series iterator
///
/// for instance:
///
/// ```rust
/// if s.exhausted() {
///     None
/// } else {
///     let res = match arr.get(s.step) {
///         Some(v) => value!(v),
///         None => Value::default(),
///     };
///     s.step += 1;
///     Some(res)
/// }
/// ```
macro_rules! s_fn_next {
    ($arr:expr, $stepper:expr) => {{
        if $stepper.exhausted() {
            None
        } else {
            let res = $crate::value!($arr.get($stepper.step));
            $stepper.forward();
            Some(res)
        }
    }};
}

macro_rules! sc_fn_next {
    ($arr:expr, $stepper:expr) => {{
        if $stepper.exhausted() {
            None
        } else {
            let res = $crate::value!($arr.get($stepper.step).cloned());
            $stepper.forward();
            Some(res)
        }
    }};
}

impl Iterator for SeriesIntoIterator {
    type Item = Value;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            SeriesIntoIterator::Id(arr, s) => s_fn_next!(arr, s),
            SeriesIntoIterator::Bool(arr, s) => s_fn_next!(arr, s),
            SeriesIntoIterator::U8(arr, s) => s_fn_next!(arr, s),
            SeriesIntoIterator::U16(arr, s) => s_fn_next!(arr, s),
            SeriesIntoIterator::U32(arr, s) => s_fn_next!(arr, s),
            SeriesIntoIterator::U64(arr, s) => s_fn_next!(arr, s),
            SeriesIntoIterator::I8(arr, s) => s_fn_next!(arr, s),
            SeriesIntoIterator::I16(arr, s) => s_fn_next!(arr, s),
            SeriesIntoIterator::I32(arr, s) => s_fn_next!(arr, s),
            SeriesIntoIterator::I64(arr, s) => s_fn_next!(arr, s),
            SeriesIntoIterator::F32(arr, s) => s_fn_next!(arr, s),
            SeriesIntoIterator::F64(arr, s) => s_fn_next!(arr, s),
            SeriesIntoIterator::String(arr, s) => s_fn_next!(arr, s),
            SeriesIntoIterator::Date(ref arr, s) => sc_fn_next!(arr, s),
            SeriesIntoIterator::Time(ref arr, s) => sc_fn_next!(arr, s),
            SeriesIntoIterator::DateTime(ref arr, s) => sc_fn_next!(arr, s),
            SeriesIntoIterator::Decimal(ref arr, s) => sc_fn_next!(arr, s),
            SeriesIntoIterator::Uuid(ref arr, s) => sc_fn_next!(arr, s),
        }
    }
}

/// Series Iterator process
///
/// for instance:
/// ```rust
/// let arr = self.0.bool().unwrap();
/// SeriesIterator::Bool(
///     arr,
///     Stepper::new(arr.len()),
/// )
/// ```
macro_rules! si {
    ($fn_call:expr, $series_iter_var:ident) => {{
        let arr = $fn_call.unwrap();
        $crate::core::SeriesIterator::$series_iter_var(
            arr,
            $crate::core::util::Stepper::new(arr.len()),
        )
    }};
    ($fn_call:expr, $downcast_type:ident, $series_iter_var:ident) => {{
        let arr = $fn_call
            .downcast_ref::<polars::prelude::ObjectChunked<$downcast_type>>()
            .unwrap();
        $crate::core::SeriesIterator::$series_iter_var(
            arr,
            $crate::core::util::Stepper::new(arr.len()),
        )
    }};
}

impl<'a> IntoIterator for &'a Series {
    type Item = Value;
    type IntoIter = SeriesIterator<'a>;

    fn into_iter(self) -> Self::IntoIter {
        match self.dtype() {
            ValueType::Bool => si!(self.0.bool(), Bool),
            ValueType::U8 => si!(self.0.u8(), U8),
            ValueType::U16 => si!(self.0.u16(), U16),
            ValueType::U32 => si!(self.0.u32(), U32),
            ValueType::U64 => si!(self.0.u64(), U64),
            ValueType::I8 => si!(self.0.i8(), I8),
            ValueType::I16 => si!(self.0.i16(), I16),
            ValueType::I32 => si!(self.0.i32(), I32),
            ValueType::I64 => si!(self.0.i64(), I64),
            ValueType::F32 => si!(self.0.f32(), F32),
            ValueType::F64 => si!(self.0.f64(), F64),
            ValueType::String => si!(self.0.utf8(), String),
            ValueType::Date => si!(self.0.as_any(), Date, Date),
            ValueType::Time => si!(self.0.as_any(), Time, Time),
            ValueType::DateTime => si!(self.0.as_any(), DateTime, DateTime),
            ValueType::Decimal => si!(self.0.as_any(), Decimal, Decimal),
            ValueType::Uuid => si!(self.0.as_any(), Uuid, Uuid),
            // temporary ignore the rest of DataType variants
            _ => unimplemented!(),
        }
    }
}

pub enum SeriesIterator<'a> {
    Id(&'a UInt64Chunked, Stepper),
    Bool(&'a BooleanChunked, Stepper),
    U8(&'a UInt8Chunked, Stepper),
    U16(&'a UInt16Chunked, Stepper),
    U32(&'a UInt32Chunked, Stepper),
    U64(&'a UInt64Chunked, Stepper),
    I8(&'a Int8Chunked, Stepper),
    I16(&'a Int16Chunked, Stepper),
    I32(&'a Int32Chunked, Stepper),
    I64(&'a Int64Chunked, Stepper),
    F32(&'a Float32Chunked, Stepper),
    F64(&'a Float64Chunked, Stepper),
    String(&'a Utf8Chunked, Stepper),
    Date(&'a ObjectChunked<Date>, Stepper),
    Time(&'a ObjectChunked<Time>, Stepper),
    DateTime(&'a ObjectChunked<DateTime>, Stepper),
    Decimal(&'a ObjectChunked<Decimal>, Stepper),
    Uuid(&'a ObjectChunked<Uuid>, Stepper),
}

impl<'a> Iterator for SeriesIterator<'a> {
    type Item = Value;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            SeriesIterator::Id(arr, s) => s_fn_next!(arr, s),
            SeriesIterator::Bool(arr, s) => s_fn_next!(arr, s),
            SeriesIterator::U8(arr, s) => s_fn_next!(arr, s),
            SeriesIterator::U16(arr, s) => s_fn_next!(arr, s),
            SeriesIterator::U32(arr, s) => s_fn_next!(arr, s),
            SeriesIterator::U64(arr, s) => s_fn_next!(arr, s),
            SeriesIterator::I8(arr, s) => s_fn_next!(arr, s),
            SeriesIterator::I16(arr, s) => s_fn_next!(arr, s),
            SeriesIterator::I32(arr, s) => s_fn_next!(arr, s),
            SeriesIterator::I64(arr, s) => s_fn_next!(arr, s),
            SeriesIterator::F32(arr, s) => s_fn_next!(arr, s),
            SeriesIterator::F64(arr, s) => s_fn_next!(arr, s),
            SeriesIterator::String(arr, s) => s_fn_next!(arr, s),
            SeriesIterator::Date(ref arr, s) => sc_fn_next!(arr, s),
            SeriesIterator::Time(ref arr, s) => sc_fn_next!(arr, s),
            SeriesIterator::DateTime(ref arr, s) => sc_fn_next!(arr, s),
            SeriesIterator::Decimal(ref arr, s) => sc_fn_next!(arr, s),
            SeriesIterator::Uuid(ref arr, s) => sc_fn_next!(arr, s),
        }
    }
}

#[cfg(test)]
mod test_fabrix_series {

    use super::*;
    use crate::{series, value};

    #[test]
    fn test_series_creation() {
        let s = Series::from_integer(&10u32).unwrap();

        println!("{:?}", s);
        println!("{:?}", s.dtype());
        println!("{:?}", s.get(9));
        println!("{:?}", s.take(&[0, 3, 9]).unwrap());

        let s = Series::from_range(&[3u8, 9]).unwrap();

        println!("{:?}", s);
        println!("{:?}", s.dtype());
        println!("{:?}", s.get(100));
        println!("{:?}", s.take(&[0, 4]).unwrap());

        let s = Series::from_values_default_name(
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
