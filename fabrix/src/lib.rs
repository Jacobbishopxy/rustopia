//! Fabrix
//!
//! A connector, who links several resources together, reading, transforming, operating
//! and writing in a row.

pub mod db;
pub mod errors;
pub mod fabrix;
pub mod file;
pub mod json;
pub mod macros;

pub use errors::*;
