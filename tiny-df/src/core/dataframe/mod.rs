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

use std::mem;

use serde::{Deserialize, Serialize};

mod accessor;
mod constructor;
mod manipulator;
mod misc;

use super::meta::*;

/// Dataframe
/// Core struct of this lib crate
///
/// A dataframe can store three kinds of data, which is determined by its direction:
/// - horizontal presence: each row means one record, certified data size
/// - vertical presence: each column means one record, certified data size
/// - raw: raw data, uncertified data size (each row can have different size)
#[derive(Debug, Default, Serialize, Deserialize)]
pub struct Dataframe {
    data: DF,
    columns: Vec<DataframeColumn>,
    indices: Vec<DataframeIndex>,
    data_orientation: DataOrientation,
    size: (usize, usize),
}

/// Columns definition
/// 1. D: dynamic column
/// 1. R: reference
pub(crate) enum RefCols<'a> {
    D,
    R(&'a Vec<DataframeColumn>),
}

/// process series (dataframe row) data, e.g. type correction, trim data length
pub(crate) struct DataframeRowProcessor<'a> {
    pub data: Series,
    pub columns: RefCols<'a>,
    _cache_col_name: Option<String>,
    _cache_col: Option<DataframeColumn>,
}

impl<'a> DataframeRowProcessor<'a> {
    /// dataframe row processor constructor
    pub fn new(ref_col: RefCols<'a>) -> Self {
        DataframeRowProcessor {
            data: Vec::new(),
            columns: ref_col,
            _cache_col_name: None,
            _cache_col: None,
        }
    }

    /// check data type, if matching push the data to buf else push None to buf
    pub fn exec(&mut self, type_idx: usize, data: &mut DataframeData) {
        match self.columns {
            RefCols::D => {
                if type_idx == 0 {
                    // get column name
                    self._cache_col_name = Some(data.to_string());
                    return;
                }
                if type_idx == 1 {
                    // until now (the 2nd cell) we can know the type of this row
                    // create `DataframeColDef` and push to `columns`
                    let cd = DataframeColumn::new(
                        self._cache_col_name.clone().unwrap(),
                        data.as_ref().into(),
                    );

                    self._cache_col = Some(cd);
                }

                // check type and wrap
                let mut tmp = DataframeData::None;
                let value_type: DataType = data.as_ref().into();
                if self._cache_col.as_ref().unwrap().col_type == value_type {
                    mem::swap(&mut tmp, data);
                }

                self.data.push(tmp)
            }
            RefCols::R(r) => {
                // check type and wrap
                let mut tmp = DataframeData::None;
                let value_type: DataType = data.as_ref().into();
                if r.get(type_idx).unwrap().col_type == value_type {
                    mem::swap(&mut tmp, data);
                }

                self.data.push(tmp)
            }
        }
    }

    /// push None to buf
    pub fn skip(&mut self) {
        self.data.push(DataframeData::None);
    }

    /// get cached column, used for vertical data direction processing
    pub fn get_cache_col(&self) -> DataframeColumn {
        self._cache_col.clone().unwrap_or_default()
    }
}

/// Convert dataframe to pure DF structure
impl From<Dataframe> for DF {
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

/// iterator returns `Series` (takes ownership)
impl IntoIterator for Dataframe {
    type Item = Series;
    type IntoIter = IntoIteratorDf;

    fn into_iter(self) -> Self::IntoIter {
        IntoIteratorDf {
            iter: self.data.into_iter(),
        }
    }
}

pub struct IntoIteratorDf {
    iter: std::vec::IntoIter<Series>,
}

impl Iterator for IntoIteratorDf {
    type Item = Series;

    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next()
    }
}

/// iterator returns `&Series`
impl<'a> IntoIterator for &'a Dataframe {
    type Item = &'a Series;
    type IntoIter = IteratorDf<'a>;

    fn into_iter(self) -> Self::IntoIter {
        IteratorDf {
            iter: self.data.iter(),
        }
    }
}

pub struct IteratorDf<'a> {
    iter: std::slice::Iter<'a, Series>,
}

impl<'a> Iterator for IteratorDf<'a> {
    type Item = &'a Series;

    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next()
    }
}

/// iterator returns `&mut Series`
impl<'a> IntoIterator for &'a mut Dataframe {
    type Item = &'a mut Series;
    type IntoIter = IterMutDf<'a>;

    fn into_iter(self) -> Self::IntoIter {
        IterMutDf {
            iter: self.data.iter_mut(),
        }
    }
}

pub struct IterMutDf<'a> {
    iter: std::slice::IterMut<'a, Series>,
}

impl<'a> Iterator for IterMutDf<'a> {
    type Item = &'a mut Series;

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
    use crate::df;
    use crate::prelude::*;

    #[test]
    fn test_df_iter() {
        let data = df![
            ["idx", "name", "tag"],
            [0, "Jacob", "Cool"],
            [1, "Sam", "Mellow"],
        ];

        let mut df = Dataframe::new(data, "h");

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
