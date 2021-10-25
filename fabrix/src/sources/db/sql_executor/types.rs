//! Sql types

use std::{collections::HashMap, marker::PhantomData};

use polars::prelude::DataType;

use crate::Value;

pub(crate) trait SqlTypeTagMarker: Send + Sync {
    fn to_dtype(&self) -> DataType;
}

#[derive(Debug)]
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

type SqlTypeTagKind = Box<dyn SqlTypeTagMarker + Send + Sync>;

lazy_static::lazy_static! {
    static ref MySqlTypeMap: HashMap<&'static str, Box<dyn SqlTypeTagMarker + Send + Sync>> = {
        let mut m = HashMap::new();

        m.insert("TINYINT(1)", Box::new(SqlTypeTag::<bool>::new("TINYINT(1)")) as SqlTypeTagKind);

        m
    };

}

#[cfg(test)]
mod test_types {
    use super::*;

    #[test]
    fn name() {
        println!("{:?}", MySqlTypeMap.get("TINYINT(1)").unwrap().to_dtype());
    }
}
