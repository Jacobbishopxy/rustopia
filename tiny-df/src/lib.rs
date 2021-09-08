//! A row based Dataframe structure

pub mod core;
#[cfg(feature = "db")]
pub mod db;
pub mod de;
pub mod se;

pub mod prelude;
