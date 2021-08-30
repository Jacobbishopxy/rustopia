//! tiny-df util

use std::mem;

use crate::prelude::*;

/// Columns definition
/// 1. D: dynamic column
/// 1. R: reference
pub(crate) enum RefCols<'a> {
    D,
    R(&'a Vec<DataframeColumn>),
}

/// process series (dataframe row) data, e.g. type correction, trim data length
pub(crate) struct SeriesDataProcessor<'a> {
    pub data: D1,
    pub columns: RefCols<'a>,
    _cache_col_name: Option<String>,
    _cache_col: Option<DataframeColumn>,
}

impl<'a> SeriesDataProcessor<'a> {
    /// dataframe row processor constructor
    pub fn new(ref_col: RefCols<'a>) -> Self {
        SeriesDataProcessor {
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
