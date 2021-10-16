//! Fabrix core

pub mod dataframe;
pub mod row;
pub mod series;
pub mod value;

pub use dataframe::*;
pub use row::*;
pub use series::*;
pub use value::*;

use polars::prelude::AnyValue;

use crate::FabrixError;

/// a general naming for a default FDataFrame index
pub const IDX: &'static str = "index";

/// index value
pub const IDX_V: &'static Value = &Value(AnyValue::Utf8(IDX));

/// out of boundary error
pub(crate) fn oob_err(length: usize, len: usize) -> FabrixError {
    FabrixError::new_common_error(format!("length {:?} out of len {:?} boundary", length, len))
}

/// index not found error
pub(crate) fn inf_err<'a>(index: &Value<'a>) -> FabrixError {
    FabrixError::new_common_error(format!("index {:?} not found", index))
}
