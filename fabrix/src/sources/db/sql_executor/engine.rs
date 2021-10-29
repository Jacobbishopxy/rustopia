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

    /// insert data into a table
    async fn insert(&self, table_name: &str, data: DataFrame) -> FabrixResult<usize>;

    /// update data in a table
    async fn update(&self, table_name: &str, data: DataFrame) -> FabrixResult<usize>;

    /// save data into a table
    async fn save(
        &self,
        table_name: &str,
        data: DataFrame,
        strategy: &adt::SaveStrategy,
    ) -> FabrixResult<usize>;

    /// get data from db. If the table has primary key, DataFrame's index will be the primary key
    async fn select(&self, select: &adt::Select) -> FabrixResult<DataFrame>;
}
