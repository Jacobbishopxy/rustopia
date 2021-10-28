//! Fabrix Sql engine

use async_trait::async_trait;

use crate::{adt, DataFrame, FabrixResult};

/// An engin is an interface to describe sql executor's business logic
#[async_trait]
pub trait Engine {
    /// connect to the database
    async fn connect(&mut self) -> FabrixResult<()>;

    /// disconnect from the database
    async fn disconnect(&mut self) -> FabrixResult<()>;

    /// get primary key from a table
    async fn get_primary_key(&self, table_name: &str) -> FabrixResult<String>;

    /// get data from db
    async fn select(&self, select: &adt::Select) -> FabrixResult<DataFrame>;
}
