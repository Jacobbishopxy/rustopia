//! Fabrix DataFrame

use std::vec::IntoIter;

use polars::frame::select::Selection;
use polars::prelude::{AnyValue, DataFrame as PDataFrame, Field, NewChunkedArray, UInt32Chunked};

use super::Series;
use super::IDX;
use crate::{FabrixError, FabrixResult};

/// DataFrame is a data structure used in Fabrix crate, it wrapped `polars` Series as DF index and
/// `polars` DataFrame for holding 2 dimensional data
#[derive(Debug, Clone)]
pub struct DataFrame {
    data: PDataFrame,
    index: Series,
}

impl DataFrame {
    /// DataFrame constructor
    pub fn new(data: PDataFrame, index: Series) -> Self {
        DataFrame { data, index }
    }

    /// Create a DataFrame from Vec<Series> and index name
    pub fn from_series_with_index(series: Vec<Series>, index_name: &str) -> FabrixResult<Self> {
        let index;
        let mut series = series;
        match series.iter().position(|s| s.name() == index_name) {
            Some(i) => {
                index = series.swap_remove(i);
            }
            None => {
                return Err(FabrixError::Common(
                    "index name is not found in Vec<Series>",
                ))
            }
        }

        let data = series.into_iter().map(|s| s.0).collect();

        let data = PDataFrame::new(data)?;

        Ok(DataFrame { data, index })
    }

    /// Create a DataFrame from Vec<Series>, index is automatically generated
    pub fn from_series(series: Vec<Series>) -> FabrixResult<Self> {
        let len = series
            .first()
            .ok_or(FabrixError::Common("Vec<Series> is empty"))?
            .len() as u64;

        let data = PDataFrame::new(series.into_iter().map(|s| s.0).collect())?;
        let index = Series::from_integer(&len);

        Ok(DataFrame { data, index })
    }

    /// get a cloned column
    pub fn get_column(&self, name: &str) -> Option<Series> {
        match self.data.column(name) {
            Ok(s) => Some(Series::new(s.clone())),
            Err(_) => None,
        }
    }

    /// get a vector of cloned columns
    pub fn get_columns<'a, S>(&self, names: S) -> Option<Vec<Series>>
    where
        S: Selection<'a, &'a str>,
    {
        match self.data.select_series(names) {
            Ok(r) => Some(r.into_iter().map(|s| Series::new(s)).collect()),
            Err(_) => None,
        }
    }

    /// get a reference of FDataFrame's data
    pub fn data(&self) -> &PDataFrame {
        &self.data
    }

    /// get a reference of FDataFrame's index
    pub fn index(&self) -> &Series {
        &self.index
    }

    /// get FDataFrame column info
    pub fn get_column_schema(&self) -> Vec<Field> {
        self.data.schema().fields().clone()
    }

    /// take cloned rows by an indices array
    pub fn take_rows_by_indices(&self, indices: &[u32]) -> FabrixResult<DataFrame> {
        let idx = UInt32Chunked::new_from_slice(IDX, indices);

        let data = self.data.take(&idx)?;

        Ok(DataFrame {
            data,
            index: self.index.take(indices)?,
        })
    }

    /// take cloned FDataFrame by an index FSeries
    pub fn take_rows(&self, index: &Series) -> FabrixResult<DataFrame> {
        let idx = self.index.find_indices(index);
        let idx = idx.into_iter().map(|i| i as u32).collect::<Vec<_>>();

        Ok(self.take_rows_by_indices(&idx[..])?)
    }

    /// take cloned FDataFrame by column names
    pub fn take_cols<'a, S>(&self, cols: S) -> FabrixResult<DataFrame>
    where
        S: Selection<'a, &'a str>,
    {
        let data = self.data.select(cols)?;
        Ok(DataFrame {
            data,
            index: self.index.clone(),
        })
    }

    pub fn row_iter(&self) -> IntoIter<Vec<AnyValue>> {
        todo!()
    }
}

#[cfg(test)]
mod test_fabrix_dataframe {

    use crate::{df, series};

    #[test]
    fn test_df_new1() {
        let df = df![
            "names" => ["Jacob", "Sam", "Jason"],
            "ord" => [1,2,3],
            "val" => [Some(10), None, Some(8)]
        ]
        .unwrap();

        println!("{:?}", df);

        println!("{:?}", df.get_column_schema());

        println!("{:?}", df.get_column("names").unwrap());
    }

    #[test]
    fn test_df_new2() {
        let df = df![
            "ord";
            "names" => ["Jacob", "Sam", "Jason"],
            "ord" => [1,2,3],
            "val" => [Some(10), None, Some(8)]
        ]
        .unwrap();

        println!("{:?}", df);

        println!("{:?}", df.get_column_schema());

        println!("{:?}", df.get_column("names").unwrap());
    }

    #[test]
    fn test_df_op() {
        let df = df![
            "names" => ["Jacob", "Sam", "James"],
            "ord" => [1,2,3],
            "val" => [Some(10), None, Some(8)]
        ]
        .unwrap();

        println!("{:?}", df.get_columns(&["names", "val"]).unwrap());

        println!("{:?}", df.take_rows_by_indices(&[0, 2]));

        println!("{:?}", df.take_cols(&["names", "val"]).unwrap());

        // watch out that the default index type is u64
        let flt = series!([1u64, 3u64]);

        println!("{:?}", df.take_rows(&flt));
    }
}
