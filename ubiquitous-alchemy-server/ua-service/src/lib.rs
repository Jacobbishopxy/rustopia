//! # Ua-service
//!
//! Business logic implementation.
//! Directly provides functionality of accessing to customer specified database.

pub mod dao;
pub mod errors;
pub mod interface;
pub mod provider;
pub mod repository;
pub mod util;

pub use dao::{Dao, DaoMY, DaoOptions, DaoPG};
pub use errors::DaoError;
pub use util::{JsonType, QueryResult};
