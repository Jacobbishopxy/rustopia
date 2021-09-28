//! Dataframe
//!
//! Core struct of this crate. A `Dataframe` plays as a data structure to hold two dimensional data, heterogeneously.
//! Supporting three kinds of data storage, a `Dataframe` can store data in horizontal, vertical or raw orientation.
//! Gluing external crates such as [arrow-rs](https://github.com/apache/arrow-rs), a `Dataframe` is capable of being
//! converted to other type of sources.
//!
//! Main function:
//! 1. `new`
//! 1. `from_2d_vec`
//! 1. `data`
//! 1. `iloc`
//! 1. `loc`
//! 1. `transpose`
//! 1. `append`
//! 1. `concat`
//! 1. `insert` (multi-dir)
//! 1. `insert_many` (multi-dir)
//! 1. `truncate`
//! 1. `delete` (multi-dir)
//! 1. `delete_many` (multi-dir)
//! 1. `update` (multi-dir)      TODO:
//! 1. `update_many` (multi-dir) TODO:
//! 1. `is_empty`
//! 1. `size`
//! 1. `columns`
//! 1. `columns_name`
//! 1. `indices`
//! 1. `data_orientation`
//! 1. `rename_column`
//! 1. `rename_columns`
//! 1. `replace_index`
//! 1. `replace_indices`
//!

use serde::{Deserialize, Serialize};

mod accessor;
mod constructor;
mod iter;
mod manipulator;
mod misc;

use super::meta::*;
use super::series::Series;

/// Dataframe
/// Core struct of this lib crate
///
/// A dataframe can store three kinds of data, which is determined by its direction:
/// - horizontal presence: each row means one record, certified data size
/// - vertical presence: each column means one record, certified data size
/// - strict presence: similar to vertical presence, but each series should have the same type
/// - raw: raw data, uncertified data size (each row can have different size)
#[derive(Debug, Default, Serialize, Deserialize)]
pub struct Dataframe {
    data: D2,
    columns: Vec<DataframeColumn>,
    indices: Vec<Index>,
    data_orientation: DataOrientation,
    size: (usize, usize),
}

/// Convert dataframe to pure DF structure
impl From<Dataframe> for D2 {
    fn from(dataframe: Dataframe) -> Self {
        match &dataframe.data_orientation {
            DataOrientation::Horizontal => {
                let mut dataframe = dataframe;
                let head = dataframe
                    .columns
                    .into_iter()
                    .map(|d| d.name.into())
                    .collect::<Vec<_>>();
                dataframe.data.insert(0, head);
                dataframe.data
            }
            DataOrientation::Vertical => dataframe
                .data
                .into_iter()
                .zip(dataframe.columns.into_iter())
                .map(|(mut row, cd)| {
                    row.insert(0, cd.name.into());
                    row
                })
                .collect::<Vec<_>>(),
            #[cfg(feature = "strict")]
            DataOrientation::Strict => todo!(),
            DataOrientation::Raw => dataframe.data,
        }
    }
}

impl From<Series> for D1 {
    fn from(series: Series) -> Self {
        let mut series = series;
        series.data.insert(0, series.name.into());
        series.data
    }
}
