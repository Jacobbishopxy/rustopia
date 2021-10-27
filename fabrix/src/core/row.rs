//! Fabrix row

use polars::prelude::{DataFrame as PDataFrame, Field, Schema};

use super::{
    cis_err, inf_err, oob_err,
    util::{new_df_from_rdf_and_series, Stepper},
    SeriesIntoIterator,
};
use crate::{DataFrame, FabrixError, FabrixResult, Series, Value};

#[derive(Debug, Clone)]
pub struct Row {
    pub(crate) index: Value,
    pub(crate) data: Vec<Value>,
}

// TODO: polars `from_rows` method is incomplete, needs refactor!
impl Row {
    /// Row constructor
    pub fn new(index: Value, data: Vec<Value>) -> Self {
        Row { index, data }
    }

    /// Row constructor, no index
    pub fn from_values(data: Vec<Value>) -> Self {
        Row {
            index: Value::Null,
            data,
        }
    }

    /// get data
    pub fn data(&self) -> &[Value] {
        &self.data[..]
    }

    /// get index
    pub fn index(&self) -> &Value {
        &self.index
    }

    /// get data field
    pub fn data_fields(&self) -> Vec<Field> {
        self.data.iter().map(|v| v.into()).collect()
    }
}

/// &Row -> polars row

impl DataFrame {
    // TODO:
    /// create a DataFrame by Rows, slower than column-wise constructors.
    pub fn from_rows<'a>(rows: Vec<Row>) -> FabrixResult<Self> {
        // let (mut index, mut p_rows): (Vec<Value>, Vec<PRow>) = (vec![], vec![]);

        // for row in rows.iter() {
        //     index.push(row.index.clone());
        //     p_rows.push(row.into());
        // }

        // let schema = Schema::new(
        //     rows.first()
        //         .map(|f| f.data_fields())
        //         .ok_or(FabrixError::new_common_error("`rows` is empty"))?,
        // );

        // Ok(new_df_from_rdf_and_series(
        //     PDataFrame::from_rows_and_schema(&p_rows, &schema),
        //     Series::from_values(index, false)?,
        // )?)

        todo!()
    }

    /// get a row by index. This method is slower than get a column.
    pub fn get_row<'a>(&self, index: &Value) -> FabrixResult<Row> {
        self.index
            .find_index(index)
            .map_or(Err(inf_err(index)), |i| self.get_row_by_idx(i))
    }

    // TODO:
    /// get a row by idx. This method is slower than get a column (`self.data.get_row`).
    pub fn get_row_by_idx(&self, idx: usize) -> FabrixResult<Row> {
        let len = self.height();
        if idx >= len {
            return Err(oob_err(idx, len));
        }
        // let (data, index) = (self.data.get_row(idx), self.index.get(idx)?);

        // Ok(Row::from_polars_row(index, data))
        todo!()
    }

    /// append a row to the dataframe. dtypes of the row must be equivalent to self dtypes
    pub fn append(&mut self, row: Row) -> FabrixResult<&mut Self> {
        let d = DataFrame::from_rows(vec![row])?;
        self.vconcat_mut(&d)
    }

    /// insert a row into the dataframe
    pub fn insert_row(&mut self, index: Value, row: Row) -> FabrixResult<&mut Self> {
        match self.index.find_index(&index) {
            Some(idx) => self.insert_row_by_idx(idx, row),
            None => Err(inf_err(&index)),
        }
    }

    /// insert a row into the dataframe by idx
    pub fn insert_row_by_idx(&mut self, idx: usize, row: Row) -> FabrixResult<&mut Self> {
        let len = self.height();
        let (mut d1, d2) = (self.slice(0, idx), self.slice(idx as i64, len));

        d1.append(row)?.vconcat_mut(&d2)?;
        *self = d1;

        Ok(self)
    }

    /// insert rows into the dataframe by index
    pub fn insert_rows(&mut self, index: Value, rows: Vec<Row>) -> FabrixResult<&mut Self> {
        match self.index.find_index(&index) {
            Some(idx) => self.insert_rows_by_idx(idx, rows),
            None => Err(inf_err(&index)),
        }
    }

    /// insert rows into the dataframe by idx
    pub fn insert_rows_by_idx(&mut self, idx: usize, rows: Vec<Row>) -> FabrixResult<&mut Self> {
        let len = self.height();
        let (mut d1, d2) = (self.slice(0, idx), self.slice(idx as i64, len));
        let di = DataFrame::from_rows(rows)?;

        d1.vconcat_mut(&di)?.vconcat_mut(&d2)?;
        *self = d1;

        Ok(self)
    }

    /// pop row
    pub fn pop_row(&mut self) -> FabrixResult<&mut Self> {
        let len = self.height();
        if len == 0 {
            return Err(cis_err("dataframe"));
        }

        *self = self.slice(0, len - 1);

        Ok(self)
    }

    /// remove a row
    pub fn remove_row(&mut self, index: Value) -> FabrixResult<&mut Self> {
        match self.index.find_index(&index) {
            Some(idx) => self.remove_row_by_idx(idx),
            None => Err(inf_err(&index)),
        }
    }

    /// remove a row by idx
    pub fn remove_row_by_idx(&mut self, idx: usize) -> FabrixResult<&mut Self> {
        let len = self.height();
        if idx >= len {
            return Err(oob_err(idx, len));
        }
        let (mut s1, s2) = (self.slice(0, idx), self.slice(idx as i64 + 1, len));

        s1.vconcat_mut(&s2)?;
        *self = s1;

        Ok(self)
    }

    /// remove rows. expensive
    pub fn remove_rows<'a>(&mut self, indices: Vec<Value>) -> FabrixResult<&mut Self> {
        let idx = Series::from_values(indices, false)?;
        let idx = self.index.find_indices(&idx);

        self.remove_rows_by_idx(idx)
    }

    /// remove rows by idx. expensive
    pub fn remove_rows_by_idx(&mut self, idx: Vec<usize>) -> FabrixResult<&mut Self> {
        if idx.is_empty() {
            return Err(cis_err("idx"));
        }
        let mut idx = idx;
        idx.sort();
        let mut iter = idx.into_iter();
        let length = iter.next().unwrap();
        let mut df = self.slice(0, length);
        let mut offset = length as i64 + 1;

        for i in iter {
            df.vconcat_mut(&self.slice(offset, i - offset as usize))?;
            offset = i as i64 + 1;
        }
        df.vconcat_mut(&self.slice(offset, self.height()))?;
        *self = df;

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

        d1.vconcat_mut(&d2)?;
        *self = d1;

        Ok(self)
    }
}

impl IntoIterator for DataFrame {
    type Item = Row;
    type IntoIter = DataFrameIntoIterator;

    fn into_iter(self) -> Self::IntoIter {
        let len = self.height();

        let mut data_iters = Vec::with_capacity(self.width() + 1);
        for s in self.data.iter() {
            let iter = Series(s.clone()).into_iter();
            data_iters.push(iter);
        }

        DataFrameIntoIterator {
            index_iter: self.index.into_iter(),
            data_iters,
            stepper: Stepper::new(len),
        }
    }
}

pub struct DataFrameIntoIterator {
    index_iter: SeriesIntoIterator,
    data_iters: Vec<SeriesIntoIterator>,
    stepper: Stepper,
}

impl Iterator for DataFrameIntoIterator {
    type Item = Row;

    fn next(&mut self) -> Option<Self::Item> {
        if self.stepper.exhausted() {
            None
        } else {
            let index = self.index_iter.next().unwrap();
            let data = self
                .data_iters
                .iter_mut()
                .map(|v| v.next().unwrap())
                .collect::<Vec<_>>();

            self.stepper.forward();
            Some(Row::new(index, data))
        }
    }
}

#[cfg(test)]
mod test_row {

    use crate::{df, rows, value, DataFrame, Row};

    #[test]
    fn test_from_rows() {
        let rows = rows!(
            [0, "Jacob", "A", 10],
            [1, "Sam", "A", 9],
            [2, "James", "A", 9],
        );

        let df = DataFrame::from_rows(rows).unwrap();

        println!("{:?}", df);

        let rows = rows!(
            100 => [0, "Jacob", "A", 10],
            101 => [1, "Sam", "A", 9],
            102 => [2, "James", "A", 9],
        );

        println!("{:?}", rows);

        let df = DataFrame::from_rows(rows).unwrap();

        println!("{:?}", df);
    }

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
        println!("{:?}", df.get_row(&value!(2i32)));
    }

    #[test]
    fn test_df_op() {
        let mut df = df![
            "ord";
            "names" => ["Jacob", "Sam", "James"],
            "ord" => [1,2,3],
            "val" => [10, 9, 8]
        ]
        .unwrap();

        let row1 = Row::new(value!(4), vec![value!("Mia"), value!(10)]);

        println!("{:?}", df.append(row1).unwrap());

        let row2 = Row::new(value!(5), vec![value!("Mandy"), value!(9)]);

        println!("{:?}", df.insert_row(value!(2), row2).unwrap());

        let rows = rows!(
            6 => ["Jamie", 9],
            7 => ["Justin", 6],
            8 => ["Julia", 8]
        );

        println!("{:?}", df.insert_rows(value!(5), rows).unwrap());

        println!("{:?}", df.remove_row(value!(7)).unwrap());

        println!("{:?}", df.remove_slice(1, 2).unwrap());

        println!("{:?}", df.remove_rows(vec![value!(2), value!(4)]).unwrap());
    }

    #[test]
    fn test_df_iter() {
        let df = df![
            "name" => ["Jacob", "Sam", "James", "Julia"],
            "star" => [100, 99, 100, 69],
            "loc" => [2u8, 3, 1, 4]
        ]
        .unwrap();

        for (idx, row) in df.into_iter().enumerate() {
            println!("{:?} >>> {:?}", idx, row);
        }
    }
}
