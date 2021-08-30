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

/// iterator returns `Series` (takes ownership)
impl IntoIterator for Dataframe {
    type Item = D1;
    type IntoIter = IntoIteratorDf;

    fn into_iter(self) -> Self::IntoIter {
        IntoIteratorDf {
            iter: self.data.into_iter(),
        }
    }
}

pub struct IntoIteratorDf {
    iter: std::vec::IntoIter<D1>,
}

impl Iterator for IntoIteratorDf {
    type Item = D1;

    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next()
    }
}

/// iterator returns `&Series`
impl<'a> IntoIterator for &'a Dataframe {
    type Item = &'a D1;
    type IntoIter = IteratorDf<'a>;

    fn into_iter(self) -> Self::IntoIter {
        IteratorDf {
            iter: self.data.iter(),
        }
    }
}

pub struct IteratorDf<'a> {
    iter: std::slice::Iter<'a, D1>,
}

impl<'a> Iterator for IteratorDf<'a> {
    type Item = &'a D1;

    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next()
    }
}

/// iterator returns `&mut Series`
impl<'a> IntoIterator for &'a mut Dataframe {
    type Item = &'a mut D1;
    type IntoIter = IterMutDf<'a>;

    fn into_iter(self) -> Self::IntoIter {
        IterMutDf {
            iter: self.data.iter_mut(),
        }
    }
}

pub struct IterMutDf<'a> {
    iter: std::slice::IterMut<'a, D1>,
}

impl<'a> Iterator for IterMutDf<'a> {
    type Item = &'a mut D1;

    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next()
    }
}

/// impl `iter` & `iter_mut` methods for `Dataframe`
impl<'a> Dataframe {
    pub fn iter(&'a self) -> IteratorDf<'a> {
        self.into_iter()
    }

    pub fn iter_mut(&'a mut self) -> IterMutDf<'a> {
        self.into_iter()
    }
}

#[cfg(test)]
mod tiny_df_test {
    use crate::d2;
    use crate::prelude::*;

    #[test]
    fn test_df_iter() {
        let data = d2![
            ["idx", "name", "tag"],
            [0, "Jacob", "Cool"],
            [1, "Sam", "Mellow"],
        ];

        let mut df = Dataframe::from_vec(data, "h");

        df.iter().for_each(|i| {
            println!("{:?}", i);
        });

        // mutate `df`, mocking insert index to each row
        df.iter_mut()
            .enumerate()
            .for_each(|(idx, v)| v.insert(0, DataframeData::Id(idx as u64)));

        println!("{:#?}", df);
    }
}
