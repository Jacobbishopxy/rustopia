//! Sql types

use std::{collections::HashMap, marker::PhantomData};

use polars::prelude::DataType;

use crate::Value;

// pub(crate) enum SqlColumnTypeStr {
//     List(&'static [&'static str]),
//     Str(&'static str),
// }

pub(crate) trait SqlTypeTagMarker {
    fn to_dtype(&self) -> DataType;
}

pub(crate) struct SqlTypeTag<T>(&'static str, PhantomData<T>)
where
    T: Into<Value>;

impl<T> SqlTypeTag<T>
where
    T: Into<Value>,
{
    pub(crate) fn new(st: &'static str) -> Self {
        SqlTypeTag(st, PhantomData)
    }
}

impl SqlTypeTagMarker for SqlTypeTag<bool> {
    fn to_dtype(&self) -> DataType {
        DataType::Boolean
    }
}

// pub(crate) const foo: SqlTypeTag<bool> = SqlTypeTag::<bool>("TINYINT(1)", PhantomData);
