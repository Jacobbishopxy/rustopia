//! Prelude
//!
//! Allowing user to import everything at once (except macros).

pub use crate::core::dataframe::Dataframe;
pub use crate::core::meta::{
    DataOrientation, DataType, DataframeColumn, DataframeData, Index, D1, D2,
};
pub use crate::core::series::Series;
pub use crate::db::{Loader, TdDbError};
