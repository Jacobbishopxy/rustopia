//! Ua-application
//!
//! A http server provides APIs as following:
//! 1. Maintaining dynamic database connection pool.
//! 2. Sql DDL and DML operations.
//!
//! Moreover, this server provides two modes: persistence and non-persistence.
//! The former one needs a database available for data persistency.

pub mod constant;
pub mod controller;
pub mod error;
pub mod model;
pub mod service;
