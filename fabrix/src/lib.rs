//! Fabrix
//!
//! A connector, who links several resources together, reading, transforming, operating
//! and writing in a row.

pub mod core;
pub mod db;
pub mod errors;
pub mod file;
pub mod json;
pub mod macros;
pub mod prelude;

pub use errors::*;
pub use prelude::*;
