//! Fabrix macros

use crate::core::{DataFrame, Series};
use crate::FabrixResult;

/// value creation macro
#[macro_export]
macro_rules! value {
    ($val:expr) => {{
        let res: $crate::Value = $val.into();
        res
    }};
}

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
pub fn new_df_from_rdf(df: RDF) -> FabrixResult<DataFrame> {
    let df = df?;
    let h = df.height() as u64;

    let index = Series::from_integer(&h);

    Ok(DataFrame::new(df, index))
}

/// df creation macro
/// Supporting:
/// 1. dataframe with default index
/// 1. dataframe with given index
#[macro_export]
macro_rules! df {
    ($($col_name:expr => $slice:expr), +) => {{
        use polars::prelude::NamedFrom;

        let mut columns = vec![];
            $(
                columns.push(polars::prelude::Series::new($col_name, $slice));
            )+
        let df = polars::prelude::DataFrame::new(columns);
        $crate::macros::new_df_from_rdf(df)
    }};
    ($index_name:expr; $($col_name:expr => $slice:expr), +) => {{
        use polars::prelude::NamedFrom;

        let mut columns = vec![];
        $(
            columns.push(polars::prelude::Series::new($col_name, $slice));
        )+
        let df = polars::prelude::DataFrame::new(columns);
        $crate::macros::new_df_from_rdf_with_index(df, $index_name)
    }};
}

/// series creation macro
/// Supporting:
/// 1. series with default name
/// 1. series with given name
#[macro_export]
macro_rules! series {
    ($slice:expr) => {{
        use polars::prelude::NamedFrom;

        $crate::Series::from_polars_series(polars::prelude::Series::new($crate::core::IDX, $slice))
    }};
    ($name:expr => $slice:expr) => {{
        use polars::prelude::NamedFrom;

        $crate::Series::from_polars_series(polars::prelude::Series::new($name, $slice))
    }};
}

/// rows creation macro
/// Supporting:
/// 1. rows with default indices
/// 1. rows with given indices
#[macro_export]
macro_rules! rows {
    ($([$($val:expr),* $(,)*]),+ $(,)*) => {{
        let mut idx = 0u32;
        let mut buf: Vec<$crate::Row> = Vec::new();
        $({
            let mut row: Vec<$crate::Value> = Vec::new();
            $(
                row.push($crate::value!($val));
            )*
            idx += 1;
            buf.push($crate::Row::new($crate::value!(idx - 1), row));
        })+
        buf
    }};
    ($($index:expr => [$($val:expr),* $(,)*]),+ $(,)*) => {{
        let mut buf: Vec<$crate::Row> = Vec::new();
        $({
            let mut row: Vec<$crate::Value> = Vec::new();
            $(
                row.push($crate::value!($val));
            )*
            buf.push($crate::Row::new($crate::value!($index), row));
        })+
        buf
    }};
}

#[cfg(test)]
mod test_macros {

    #[test]
    fn test_value() {
        println!("{:?}", value!("Jacob"));
    }

    #[test]
    fn test_series_new() {
        let series = series!(["Jacob", "Sam", "Jason"]);
        println!("{:?}", series);

        let series = series!("name" => ["Jacob", "Sam", "Jason"]);
        println!("{:?}", series);
    }

    #[test]
    fn test_df_new1() {
        let df = df![
            "names" => ["Jacob", "Sam", "Jason"],
            "ord" => [1,2,3],
            "val" => [Some(10), None, Some(8)]
        ]
        .unwrap();

        println!("{:?}", df);
        println!("{:?}", df.dtypes());
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
    fn test_rows_new() {
        let rows = rows!(
            [0, "Jacob", "A", 10],
            [1, "Sam", "A", 9],
            [2, "James", "A", 9],
        );

        println!("{:?}", rows);

        let rows = rows!(
            1 => ["Jacob", "A", 10],
            2 => ["Sam", "A", 9],
            3 => ["James", "A", 9],
        );

        println!("{:?}", rows);
    }
}
