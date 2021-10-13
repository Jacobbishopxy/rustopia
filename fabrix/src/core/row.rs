//! Fabrix row

use std::vec::IntoIter;

use polars::frame::row::Row as PRow;

use super::{DataFrame, Value};
use crate::{FabrixError, FabrixResult};

#[derive(Debug, Clone)]
pub struct Row<'a> {
    pub(crate) index: Value<'a>,
    pub(crate) data: Vec<Value<'a>>,
}

impl<'a> Row<'a> {
    /// Row constructor
    pub fn new(index: Value<'a>, data: Vec<Value<'a>>) -> Self {
        Row { index, data }
    }

    /// Row constructor, from `polars` Row
    pub fn from_row(index: Value<'a>, data: PRow<'a>) -> Self {
        Row {
            index,
            data: data.0.into_iter().map(|i| i.into()).collect(),
        }
    }

    /// get data
    pub fn data(&self) -> &[Value<'a>] {
        &self.data[..]
    }

    /// get index
    pub fn index(&self) -> &Value<'a> {
        &self.index
    }
}

impl DataFrame {
    /// get a row by index. This method is slower than get a column.
    pub fn get_row<'a>(&self, index: &Value<'a>) -> FabrixResult<Row> {
        self.index.find_index(index).map_or(
            Err(FabrixError::new_common_error(format!(
                "value {:?} is not in index",
                index
            ))),
            |i| self.get_row_by_idx(i),
        )
    }

    /// get a row by idx. This method is slower than get a column (`self.data.get_row`).
    pub fn get_row_by_idx(&self, idx: usize) -> FabrixResult<Row> {
        let len = self.height();
        if idx >= len {
            return Err(FabrixError::new_common_error(format!(
                "index {:?} out of len {:?} boundary",
                idx, len
            )));
        }
        let data = self.data.get_row(idx);
        let index = self.index.get(idx)?;
        Ok(Row::from_row(index, data))
    }

    /// append a row to the dataframe
    pub fn append<'a>(&mut self, row: Row<'a>) -> FabrixResult<()> {
        todo!()
    }

    /// insert a row into the dataframe
    pub fn insert_row<'a>(&mut self, index: Value<'a>, row: Row<'a>) -> FabrixResult<()> {
        todo!()
    }

    /// insert a row into the dataframe by idx
    pub fn insert_row_by_idx<'a>(&mut self, idx: usize, row: Row<'a>) -> FabrixResult<()> {
        todo!()
    }

    /// remove a row
    pub fn remove_row<'a>(&mut self, index: Value<'a>) -> FabrixResult<()> {
        todo!()
    }

    /// remove a row by idx
    pub fn remove_row_by_idx<'a>(&mut self, idx: usize) -> FabrixResult<()> {
        todo!()
    }

    /// row-wise iteration
    pub fn row_iter<'a>(&self) -> IntoIter<Row<'a>> {
        todo!()
    }
}

#[cfg(test)]
mod test_row {

    use crate::core::Value;
    use crate::df;

    use polars::prelude::AnyValue;

    #[test]
    fn test_get_row() {
        let df = df![
            "ord";
            "names" => ["Jacob", "Sam", "James"],
            "ord" => [1,2,3],
            "val" => [Some(10), None, Some(8)]
        ]
        .unwrap();

        println!("{:?}", df.get_row_by_idx(1));
        println!("{:?}", df.get_row(&Value::new(AnyValue::Int32(2))));
    }
}
