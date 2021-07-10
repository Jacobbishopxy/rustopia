//! # Dyn-conn
//!
//! Dyn-conn is a package aiming to maintain databases' dynamic connection pool,
//! both in project runtime memory and persistency.
//!
//! ## Data structure
//! - ConnInfo: handling connection information.
//! - ConnMember: contains a custom connection info and a connection pool with business logic.
//! - ConnStore: contains a hashmap which saving all the connection pools, and
//! an optional persistence field.
//!
//! ## Traits
//! - ConnInfoFunctionality: a trait bound for concrete connection info type
//! - BizPoolFunctionality: a trait bound for concrete business type
//! - ConnGeneratorFunctionality: nested trait bound (implemented BizPoolFunctionality)
//! which abstracts connection establishment and etc.
//! - PersistenceFunctionality: dynamic trait object for persisting runtime data

pub mod model;

pub use model::*;
