//! A row based Dataframe structure

pub mod core;
pub mod de;
pub mod se;

#[cfg(feature = "arrow")]
pub mod arrow;

pub use crate::core::*;
