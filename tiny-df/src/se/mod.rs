//! Serialize

#[cfg(feature = "json")]
pub mod json;
#[cfg(feature = "sql")]
pub mod sql;

#[cfg(feature = "json")]
pub use json::Json;

#[cfg(feature = "sql")]
pub use sql::{IndexOption, IndexType, SaveOption, SaveStrategy, Sql};
