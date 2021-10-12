//! Fabrix row

use std::vec::IntoIter;

use super::{DataFrame, Value};
use crate::FabrixResult;

#[derive(Debug, Clone)]
pub struct Row<'a> {
    pub(crate) data: Vec<Value<'a>>,
    pub(crate) index: Value<'a>,
}

impl<'a> Row<'a> {
    /// new
    pub fn new(index: Value<'a>, data: Vec<Value<'a>>) -> Self {
        todo!()
    }
}

impl DataFrame {
    pub fn get_row(&self, idx: usize) -> Row {
        todo!()
    }

    pub fn get_row_by_index<'a>(&self, index: Value<'a>) -> Row {
        todo!()
    }

    pub fn append<'a>(&mut self, row: Row<'a>) -> FabrixResult<()> {
        todo!()
    }

    pub fn row_iter<'a>(&self) -> IntoIter<Row<'a>> {
        todo!()
    }
}
