//! A jotting lib used for testing polars crate and etc.

// use itertools::Itertools;
use polars::prelude::*;

#[derive(Debug)]
pub enum MyValue {
    String(String),
    Bool(bool),
    Number(f64),
    Integer(i64),
    Null,
}

impl<'a> From<AnyValue<'a>> for MyValue {
    fn from(av: AnyValue<'a>) -> Self {
        match av {
            AnyValue::Boolean(v) => MyValue::Bool(v),
            AnyValue::Utf8(v) => MyValue::String(v.to_owned()),
            AnyValue::UInt8(v) => MyValue::Integer(v.into()),
            AnyValue::UInt16(_) => todo!(),
            AnyValue::UInt32(_) => todo!(),
            AnyValue::UInt64(_) => todo!(),
            AnyValue::Int8(_) => todo!(),
            AnyValue::Int16(_) => todo!(),
            AnyValue::Int32(_) => todo!(),
            AnyValue::Int64(_) => todo!(),
            AnyValue::Float32(_) => todo!(),
            AnyValue::Float64(_) => todo!(),
            AnyValue::Date32(_) => todo!(),
            AnyValue::Date64(_) => todo!(),
            AnyValue::Time64(_, _) => todo!(),
            _ => unimplemented!(),
        }
    }
}

pub struct MySeries {
    data: Series,
    dtype: DataType,
}

impl IntoIterator for MySeries {
    type Item = MyValue;
    type IntoIter = MySeriesIntoIterator;

    fn into_iter(self) -> Self::IntoIter {
        match self.dtype {
            DataType::Boolean => {
                let arr = self.data.unpack::<BooleanType>().unwrap().clone();
                let len = arr.len();
                MySeriesIntoIterator::Bool(arr, len, 0)
            }
            DataType::UInt8 => todo!(),
            DataType::UInt16 => todo!(),
            DataType::UInt32 => todo!(),
            DataType::UInt64 => todo!(),
            DataType::Int8 => todo!(),
            DataType::Int16 => todo!(),
            DataType::Int32 => todo!(),
            DataType::Int64 => todo!(),
            DataType::Float32 => todo!(),
            DataType::Float64 => todo!(),
            DataType::Utf8 => todo!(),
            DataType::Date32 => todo!(),
            DataType::Date64 => todo!(),
            DataType::Time64(_) => todo!(),
            _ => unimplemented!(),
        }
    }
}

pub enum MySeriesIntoIterator {
    Bool(BooleanChunked, usize, usize),
    I8(Int8Chunked, usize, usize),
    I16(Int16Chunked, usize, usize),
    I32(Int32Chunked, usize, usize),
    I64(Int64Chunked, usize, usize),
    U8(UInt8Chunked, usize, usize),
    U16(UInt16Chunked, usize, usize),
    U32(UInt32Chunked, usize, usize),
    U64(UInt64Chunked, usize, usize),
    F32(Float32Chunked, usize, usize),
    F64(Float64Chunked, usize, usize),
    Str(Utf8Chunked, usize, usize),
    Date32(Date32Chunked, usize, usize),
    Date64(Date64Chunked, usize, usize),
    Time64(Time64NanosecondChunked, usize, usize),
}

impl Iterator for MySeriesIntoIterator {
    type Item = MyValue;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            MySeriesIntoIterator::Bool(arr, size, idx) => {
                if idx == size {
                    return None;
                } else {
                    let res = match arr.get(*idx) {
                        Some(v) => Some(MyValue::Bool(v)),
                        None => Some(MyValue::Null),
                    };
                    *idx += 1;
                    res
                }
            }
            MySeriesIntoIterator::I8(arr, size, idx) => todo!(),
            MySeriesIntoIterator::I16(arr, size, idx) => todo!(),
            MySeriesIntoIterator::I32(arr, size, idx) => todo!(),
            MySeriesIntoIterator::I64(arr, size, idx) => todo!(),
            MySeriesIntoIterator::U8(arr, size, idx) => todo!(),
            MySeriesIntoIterator::U16(arr, size, idx) => todo!(),
            MySeriesIntoIterator::U32(arr, size, idx) => todo!(),
            MySeriesIntoIterator::U64(arr, size, idx) => todo!(),
            MySeriesIntoIterator::F32(arr, size, idx) => todo!(),
            MySeriesIntoIterator::F64(arr, size, idx) => todo!(),
            MySeriesIntoIterator::Str(arr, size, idx) => todo!(),
            MySeriesIntoIterator::Date32(arr, size, idx) => todo!(),
            MySeriesIntoIterator::Date64(arr, size, idx) => todo!(),
            MySeriesIntoIterator::Time64(arr, size, idx) => todo!(),
        }
    }
}

#[cfg(test)]
mod test_core {

    use polars::prelude::*;

    use super::*;

    #[test]
    fn test_into_iter() {
        let s = Series::new("dev", [true, false, false, true, false]);
        let s = MySeries {
            data: s,
            dtype: DataType::Boolean,
        };

        for i in s.into_iter() {
            println!("{:?}", i);
        }
    }
}
