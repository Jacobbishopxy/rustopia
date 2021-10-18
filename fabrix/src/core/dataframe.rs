//! Fabrix DataFrame

use polars::frame::select::Selection;
use polars::prelude::{DataFrame as PDataFrame, DataType, Field, NewChunkedArray, UInt32Chunked};

use super::{cis_err, Series, IDX};
use crate::{FabrixError, FabrixResult};

/// DataFrame is a data structure used in Fabrix crate, it wrapped `polars` Series as DF index and
/// `polars` DataFrame for holding 2 dimensional data
#[derive(Debug, Clone)]
pub struct DataFrame {
    pub(crate) data: PDataFrame,
    pub(crate) index: Series,
}

impl DataFrame {
    /// DataFrame constructor
    pub fn new(data: PDataFrame, index: Series) -> Self {
        DataFrame { data, index }
    }

    /// DataFrame constructor, create an empty dataframe by data fields and index field
    pub fn new_empty(data_fields: Vec<Field>, index_field: Field) -> FabrixResult<Self> {
        let data = data_fields
            .into_iter()
            .map(|d| Series::empty_series_from_field(d))
            .collect::<FabrixResult<Vec<Series>>>()?;
        let index = Series::empty_series_from_field(index_field)?;

        DataFrame::from_series(data, index)
    }

    /// Create a DataFrame from Vec<Series> (data) and Series (index)
    pub fn from_series(series: Vec<Series>, index: Series) -> FabrixResult<Self> {
        let data = PDataFrame::new(series.into_iter().map(|s| s.0).collect())?;
        Ok(DataFrame { data, index })
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
                return Err(FabrixError::new_common_error(format!(
                    "index {:?} does not exist",
                    index_name
                )))
            }
        }

        let data = series.into_iter().map(|s| s.0).collect();
        let data = PDataFrame::new(data)?;

        Ok(DataFrame { data, index })
    }

    /// Create a DataFrame from Vec<Series>, index is automatically generated
    pub fn from_series_default_index(series: Vec<Series>) -> FabrixResult<Self> {
        let len = series.first().ok_or(cis_err("Vec<Series>"))?.len() as u64;
        let data = PDataFrame::new(series.into_iter().map(|s| s.0).collect())?;
        let index = Series::from_integer(&len);

        Ok(DataFrame { data, index })
    }

    /// get a cloned column
    pub fn get_column(&self, name: &str) -> Option<Series> {
        match self.data.column(name) {
            Ok(s) => Some(Series::from_polars_series(s.clone())),
            Err(_) => None,
        }
    }

    /// get a vector of cloned columns
    pub fn get_columns<'a, S>(&self, names: S) -> Option<Vec<Series>>
    where
        S: Selection<'a, &'a str>,
    {
        match self.data.select_series(names) {
            Ok(r) => Some(
                r.into_iter()
                    .map(|s| Series::from_polars_series(s))
                    .collect(),
            ),
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

    /// get column names
    pub fn get_column_names(&self) -> Vec<&str> {
        self.data.get_column_names()
    }

    /// set column names
    pub fn set_column_names<N>(&mut self, names: &[N]) -> FabrixResult<&mut Self>
    where
        N: AsRef<str>,
    {
        self.data.set_column_names(names)?;
        Ok(self)
    }

    /// rename
    pub fn rename(&mut self, origin: &str, new: &str) -> FabrixResult<&mut Self> {
        self.data.rename(origin, new)?;
        Ok(self)
    }

    /// series dtype
    pub fn index_dtype(&self) -> &DataType {
        self.index.dtype()
    }

    /// dataframe dtypes
    pub fn data_dtypes(&self) -> Vec<DataType> {
        self.data.dtypes()
    }

    /// series dtype + dataframe dtypes
    pub fn dtypes(&self) -> (&DataType, Vec<DataType>) {
        (self.index_dtype(), self.data_dtypes())
    }

    /// is dtypes match
    pub fn is_dtypes_match(&self, df: &DataFrame) -> bool {
        self.dtypes() == df.dtypes()
    }

    /// get FDataFrame column info
    pub fn fields(&self) -> Vec<Field> {
        self.data.fields()
    }

    /// get shape
    pub fn shape(&self) -> (usize, usize) {
        self.data.shape()
    }

    /// get width
    pub fn width(&self) -> usize {
        self.data.width()
    }

    /// get height
    pub fn height(&self) -> usize {
        self.data.height()
    }

    /// horizontal stack, return cloned data
    pub fn hconcat(&self, columns: &[Series]) -> FabrixResult<DataFrame> {
        let raw_columns = columns
            .into_iter()
            .cloned()
            .map(|v| v.0)
            .collect::<Vec<_>>();
        let data = self.data.hstack(&raw_columns[..])?;

        Ok(DataFrame::new(data, self.index.clone()))
    }

    /// horizontal stack, self mutation
    pub fn hconcat_mut(&mut self, columns: &[Series]) -> FabrixResult<&mut Self> {
        let raw_columns = columns
            .into_iter()
            .cloned()
            .map(|v| v.0)
            .collect::<Vec<_>>();

        self.data = self.data.hstack(&raw_columns[..])?;

        Ok(self)
    }

    // TODO: dtypes safety check is optional?
    /// vertical stack, return cloned data
    pub fn vconcat(&self, df: &DataFrame) -> FabrixResult<DataFrame> {
        if !self.is_dtypes_match(&df) {
            return Err(FabrixError::new_dtypes_mismatch_error(
                self.dtypes(),
                df.dtypes(),
            ));
        }
        let data = self.data.vstack(df.data())?;
        let mut index = self.index.0.clone();
        index.append(&df.index.0)?;
        let index = Series::from_polars_series(index);

        Ok(DataFrame::new(data, index))
    }

    // TODO: dtypes safety check is optional?
    /// vertical concat, self mutation
    pub fn vconcat_mut(&mut self, df: &DataFrame) -> FabrixResult<&mut Self> {
        if !self.is_dtypes_match(&df) {
            return Err(FabrixError::new_dtypes_mismatch_error(
                self.dtypes(),
                df.dtypes(),
            ));
        }
        self.data.vstack_mut(df.data())?;
        self.index.0.append(&df.index.0)?;

        Ok(self)
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

    /// slice the DataFrame along the rows
    pub fn slice(&self, offset: i64, length: usize) -> DataFrame {
        let data = self.data.slice(offset, length);
        let index = self.index.slice(offset, length);

        DataFrame::new(data, index.into())
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
        println!("{:?}", df.data_dtypes());
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
        println!("{:?}", df.fields());
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

        println!("{:?}", df.slice(2, 3));
    }
}
