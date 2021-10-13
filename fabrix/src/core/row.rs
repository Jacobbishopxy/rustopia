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
    /// get a row by idx from a dataframe. This method is slower than get column (`self.data.get_row`).
    pub fn get_row(&self, idx: usize) -> FabrixResult<Row> {
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

    pub fn get_row_by_index<'a>(&self, index: Value<'a>) -> Row {
        todo!()
    }

    pub fn append<'a>(&mut self, row: Row<'a>) -> FabrixResult<()> {
        todo!()
    }

    pub fn insert_row<'a>(&mut self, index: Value<'a>, row: Row<'a>) -> FabrixResult<()> {
        todo!()
    }

    pub fn insert_row_by_idx<'a>(&mut self, idx: usize, row: Row<'a>) -> FabrixResult<()> {
        todo!()
    }

    pub fn row_iter<'a>(&self) -> IntoIter<Row<'a>> {
        todo!()
    }
}

#[cfg(test)]
mod test_row {

    use crate::df;

    #[test]
    fn test_get_row() {
        let df = df![
            "names" => ["Jacob", "Sam", "James"],
            "ord" => [1,2,3],
            "val" => [Some(10), None, Some(8)]
        ]
        .unwrap();

        println!("{:?}", df.get_row(5));
    }
}
