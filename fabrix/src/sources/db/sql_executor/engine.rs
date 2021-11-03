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
    async fn insert(&self, table_name: &str, data: DataFrame) -> FabrixResult<u64>;

    /// update data in a table
    async fn update(&self, table_name: &str, data: DataFrame) -> FabrixResult<u64>;

    /// save data into a table
    /// saving strategy:
    /// 1. Replace: no matter the table is exist, create a new table
    /// 1. Append: if the table is exist, append data to the table, otherwise failed
    /// 1. Upsert: update and insert
    /// 1. Fail: if the table is exist, do nothing, otherwise create a new table
    async fn save(
        &self,
        table_name: &str,
        data: DataFrame,
        strategy: &adt::SaveStrategy,
    ) -> FabrixResult<usize>;

    /// get data from db. If the table has primary key, DataFrame's index will be the primary key
    async fn select(&self, select: &adt::Select) -> FabrixResult<DataFrame>;
}
