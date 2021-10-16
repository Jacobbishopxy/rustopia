//! Fabrix row

use std::vec::IntoIter;

use polars::frame::row::Row as PRow;
use polars::prelude::DataFrame as PDataFrame;

use super::{inf_err, oob_err, IDX_V};
use crate::macros::new_df_from_rdf;
use crate::{DataFrame, FabrixError, FabrixResult, Value};

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

/// polars row -> Row
impl<'a> From<PRow<'a>> for Row<'a> {
    fn from(pr: PRow<'a>) -> Self {
        Row::from_row(IDX_V.clone(), pr)
    }
}

/// Row -> polars row
impl<'a> From<Row<'a>> for PRow<'a> {
    fn from(r: Row<'a>) -> Self {
        PRow(r.data.into_iter().map(|i| i.0).collect::<Vec<_>>())
    }
}

impl DataFrame {
    /// create a DataFrame by Rows, slower than column-wise constructors.
    pub fn from_rows<'a>(rows: Vec<Row<'a>>) -> FabrixResult<Self> {
        let rows = rows.into_iter().map(|r| r.into()).collect::<Vec<PRow>>();

        Ok(new_df_from_rdf(PDataFrame::from_rows(&rows))?)
    }

    /// get a row by index. This method is slower than get a column.
    pub fn get_row<'a>(&self, index: &Value<'a>) -> FabrixResult<Row> {
        self.index
            .find_index(index)
            .map_or(Err(inf_err(index)), |i| self.get_row_by_idx(i))
    }

    /// get a row by idx. This method is slower than get a column (`self.data.get_row`).
    pub fn get_row_by_idx(&self, idx: usize) -> FabrixResult<Row> {
        let len = self.height();
        if idx >= len {
            return Err(oob_err(idx, len));
        }
        let data = self.data.get_row(idx);
        let index = self.index.get(idx)?;
        Ok(Row::from_row(index, data))
    }

    /// append a row to the dataframe
    pub fn append<'a>(&mut self, row: Row<'a>) -> FabrixResult<&mut Self> {
        let d = DataFrame::from_rows(vec![row])?;

        self.vstack_mut(&d)
    }

    /// insert a row into the dataframe
    pub fn insert_row<'a>(&mut self, index: Value<'a>, row: Row<'a>) -> FabrixResult<&mut Self> {
        match self.index.find_index(&index) {
            Some(idx) => self.insert_row_by_idx(idx, row),
            None => Err(inf_err(&index)),
        }
    }

    /// insert a row into the dataframe by idx
    pub fn insert_row_by_idx<'a>(&mut self, idx: usize, row: Row<'a>) -> FabrixResult<&mut Self> {
        let len = self.height();
        let (mut d1, d2) = (self.slice(0, idx), self.slice(idx as i64, len));
        d1.append(row)?.vstack_mut(&d2)?;
        *self = d1;
        Ok(self)
    }

    pub fn insert_rows<'a>(
        &mut self,
        index: Value<'a>,
        rows: Vec<Row<'a>>,
    ) -> FabrixResult<&mut Self> {
        todo!()
    }

    pub fn insert_rows_by_idx<'a>(
        &mut self,
        idx: usize,
        rows: Vec<Row<'a>>,
    ) -> FabrixResult<&mut Self> {
        todo!()
    }

    /// pop row
    pub fn pop_row(&mut self) -> FabrixResult<&mut Self> {
        let len = self.height();
        if len == 0 {
            return Err(FabrixError::new_common_error("dataframe is empty"));
        }
        *self = self.slice(0, len - 1);
        Ok(self)
    }

    /// remove a row
    pub fn remove_row<'a>(&mut self, index: Value<'a>) -> FabrixResult<&mut Self> {
        match self.index.find_index(&index) {
            Some(idx) => self.remove_row_by_idx(idx),
            None => Err(inf_err(&index)),
        }
    }

    /// remove a row by idx
    pub fn remove_row_by_idx<'a>(&mut self, idx: usize) -> FabrixResult<&mut Self> {
        let len = self.height();
        if idx >= len {
            return Err(oob_err(idx, len));
        }

        let (mut s1, s2) = (self.slice(0, idx), self.slice(idx as i64 + 1, len));
        s1.vstack_mut(&s2)?;
        *self = s1;
        Ok(self)
    }

    /// remove a slice of rows from the dataframe
    pub fn remove_slice(&mut self, offset: i64, length: usize) -> FabrixResult<&mut Self> {
        let len = self.height();

        let offset = if offset >= 0 {
            offset
        } else {
            len as i64 + offset
        };

        let (mut d1, d2) = (
            self.slice(0, offset as usize),
            self.slice(offset + length as i64, len),
        );
        d1.vstack_mut(&d2)?;
        *self = d1;
        Ok(self)
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
