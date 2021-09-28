//! Loader's engine
//! Engine is a trait that describes functionalities interacting with database
//!
//! provided methods:
//! 1. get_table_schema
//! 1. raw_fetch
//! 1. fetch TODO: selection, condition & pagination
//! 1. create_table
//! 1. insert
//! 1. update TODO: id column must be specified
//! 1. upsert TODO: id column must be specified
//! 1. save TODO: transaction for upsert saving strategy
//! 1. ...

use async_trait::async_trait;

use crate::db::TdDbResult;
use crate::se::{IndexOption, SaveOption};

#[async_trait]
pub trait Engine<DF, COL> {
    async fn get_table_schema(&self, table_name: &str) -> TdDbResult<Vec<COL>>;

    /// fetch all data by a query string, and turn result into a `Dataframe` (strict mode)
    async fn raw_fetch(&self, query: &str) -> TdDbResult<Option<DF>>;

    // async fn fetch(&self,) -> TdDbResult<Option<DF>>;

    /// create a table by a dataframe's columns
    async fn create_table(
        &self,
        table_name: &str,
        columns: Vec<COL>,
        index_option: Option<&IndexOption>,
    ) -> TdDbResult<u64>;

    /// insert a `Dataframe` to an existing table
    async fn insert(
        &self,
        table_name: &str,
        dataframe: DF,
        index_option: Option<&IndexOption>,
    ) -> TdDbResult<u64>;

    async fn update(
        &self,
        table_name: &str,
        dataframe: DF,
        index_option: &IndexOption,
    ) -> TdDbResult<u64>;

    // async fn upsert(&self, dataframe: Dataframe) -> TdDbResult<()>;

    /// the most useful and common writing method to a database (transaction is used)
    async fn save(
        &self,
        table_name: &str,
        dataframe: DF,
        save_option: &SaveOption,
    ) -> TdDbResult<u64>;
}
