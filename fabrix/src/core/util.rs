//! Fabrix util
//!
//! utilities

use crate::{DataFrame, FabrixResult, Series};

type RDF = Result<polars::prelude::DataFrame, polars::error::PolarsError>;

/// From a Result polars' DataFrame and index name, and it will be removed consequently.
pub fn new_df_from_rdf_with_index(df: RDF, index_name: &str) -> FabrixResult<DataFrame> {
    let df = df?;
    let idx = df.column(index_name)?.clone();
    let mut df = df;
    df.drop_in_place(index_name)?;

    Ok(DataFrame::new(df, Series::from_polars_series(idx)))
}

/// From a Result polars' DataFrame, auto generate index
pub fn new_df_from_rdf_default_index(df: RDF) -> FabrixResult<DataFrame> {
    let df = df?;
    let h = df.height() as u64;

    let index = Series::from_integer(&h);

    Ok(DataFrame::new(df, index))
}

/// From a Result polars' DataFrame and Series
pub fn new_df_from_rdf_and_series(df: RDF, series: Series) -> FabrixResult<DataFrame> {
    let df = df?;
    Ok(DataFrame::new(df, series))
}
