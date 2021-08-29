//! Prelude
//!
//! Allowing user to import every at once (except macros).

pub use crate::core::dataframe::Dataframe;
pub(crate) use crate::core::dataframe::{DataframeRowProcessor, RefCols};
pub use crate::core::meta::{
    DataOrientation, DataType, DataframeColumn, DataframeData, DataframeIndex, D1, D2,
};
