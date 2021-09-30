//! fabrix dataframe

use std::vec::IntoIter;

use polars::prelude::*;
use sea_query::Value;

/// FValue is a wrapper used for holding Polars AnyValue in order to
/// satisfy type conversion between `sea_query::Value`
pub struct FValue<'a>(AnyValue<'a>);

impl<'a> FValue<'a> {
    pub fn new(v: AnyValue<'a>) -> Self {
        FValue(v)
    }
}

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
            AnyValue::Date32(v) => todo!(),
            AnyValue::Date64(v) => todo!(),
            AnyValue::Time64(_, _) => todo!(),
            AnyValue::Duration(_, _) => todo!(),
            AnyValue::List(_) => todo!(),
        }
    }
}

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

impl<'a> FValue<'a> {
    pub fn new_default() -> Self {
        todo!()
    }
}

#[derive(Debug, Clone)]
pub struct FSeries(Series);

impl FSeries {
    const IDX: &'static str = "index";

    pub fn new(s: Series) -> Self {
        FSeries(s)
    }

    pub fn from_integer_rng<'a>(value: AnyValue<'a>) -> Self {
        match value {
            AnyValue::UInt8(v) => {
                let s: Vec<_> = (0..v).collect();
                FSeries(Series::new(Self::IDX, s))
            }
            AnyValue::UInt16(v) => {
                let s: Vec<_> = (0..v).collect();
                FSeries(Series::new(Self::IDX, s))
            }
            AnyValue::UInt32(v) => {
                let s: Vec<_> = (0..v).collect();
                FSeries(Series::new(Self::IDX, s))
            }
            AnyValue::UInt64(v) => {
                let s: Vec<_> = (0..v).collect();
                FSeries(Series::new(Self::IDX, s))
            }
            AnyValue::Int8(v) => {
                let s: Vec<_> = (0..v).collect();
                FSeries(Series::new(Self::IDX, s))
            }
            AnyValue::Int16(v) => {
                let s: Vec<_> = (0..v).collect();
                FSeries(Series::new(Self::IDX, s))
            }
            AnyValue::Int32(v) => {
                let s: Vec<_> = (0..v).collect();
                FSeries(Series::new(Self::IDX, s))
            }
            AnyValue::Int64(v) => {
                let s: Vec<_> = (0..v).collect();
                FSeries(Series::new(Self::IDX, s))
            }
            _ => todo!(),
        }
    }

    pub fn from_integer<'a, I>(value: I) -> Self
    where
        I: Into<AnyValue<'a>>,
    {
        Self::from_integer_rng(value.into())
    }

    pub fn data(&self) -> &Series {
        &self.0
    }

    pub fn dtype(&self) -> &DataType {
        &self.0.dtype()
    }
}

#[derive(Debug, Clone)]
pub struct FDataFrame {
    data: DataFrame,
    index: FSeries,
}

impl FDataFrame {
    pub fn new(data: DataFrame, index: FSeries) -> Self {
        let h = data.height();

        todo!()
    }

    pub fn from_df(df: DataFrame) -> Self {
        todo!()
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

    pub fn row_iter(&self) -> IntoIter<Vec<AnyValue>> {
        todo!()
    }
}

#[cfg(test)]
mod test_fabrix {

    use super::*;

    #[test]
    fn test_series() {
        let s = FSeries::from_integer(10u8);

        println!("{:?}", s);

        println!("{:?}", s.dtype());
    }
}
