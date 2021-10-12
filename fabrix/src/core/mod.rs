//! Fabrix core

pub mod dataframe;
pub mod series;
pub mod value;

pub use dataframe::*;
pub use series::*;
pub use value::*;

/// a general naming for a default FDataFrame index
pub const IDX: &'static str = "index";
