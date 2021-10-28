//! Fabrix Sql engine

use async_trait::async_trait;
use sqlx::{MySqlPool, PgPool, SqlitePool};

use super::types::SqlRow;
use crate::{adt, DataFrame, FabrixError, FabrixResult, Value};

/// An engin is an interface to describe sql executor's business logic
#[async_trait]
pub trait Engine {
    /// connect to the database
    async fn connect(&mut self) -> FabrixResult<()>;

    /// disconnect from the database
    async fn disconnect(&mut self) -> FabrixResult<()>;

    /// get data from db
    async fn select(&self, select: &adt::Select) -> FabrixResult<DataFrame>;
}

/// database pool interface
#[async_trait]
pub trait FabrixDatabasePool: Send + Sync {
    /// disconnect from the current database
    async fn disconnect(&self);

    /// raw fetch and return a DataFrame
    async fn raw_fetch(&self, query: &str) -> FabrixResult<DataFrame>;

    /// row sql string execution
    async fn raw_exec(&self, query: &str) -> FabrixResult<()>;
}

#[async_trait]
impl FabrixDatabasePool for MySqlPool {
    async fn disconnect(&self) {
        self.close().await;
    }

    async fn raw_fetch(&self, query: &str) -> FabrixResult<DataFrame> {
        let res: Vec<Vec<Value>> = sqlx::query(&query)
            .try_map(|row| {
                SqlRow::Mysql(&row).row_processor().map_err(|e| match e {
                    FabrixError::Sqlx(se) => se,
                    _ => sqlx::Error::WorkerCrashed,
                })
            })
            .fetch_all(self)
            .await?;

        Ok(DataFrame::from_row_wise_values(res)?)
    }

    async fn raw_exec(&self, _query: &str) -> FabrixResult<()> {
        todo!()
    }
}

#[async_trait]
impl FabrixDatabasePool for PgPool {
    async fn disconnect(&self) {
        self.close().await;
    }

    async fn raw_fetch(&self, query: &str) -> FabrixResult<DataFrame> {
        let res: Vec<Vec<Value>> = sqlx::query(&query)
            .try_map(|row| {
                SqlRow::Pg(&row).row_processor().map_err(|e| match e {
                    FabrixError::Sqlx(se) => se,
                    _ => sqlx::Error::WorkerCrashed,
                })
            })
            .fetch_all(self)
            .await?;

        Ok(DataFrame::from_row_wise_values(res)?)
    }

    async fn raw_exec(&self, _query: &str) -> FabrixResult<()> {
        todo!()
    }
}

#[async_trait]
impl FabrixDatabasePool for SqlitePool {
    async fn disconnect(&self) {
        self.close().await;
    }

    async fn raw_fetch(&self, query: &str) -> FabrixResult<DataFrame> {
        let res: Vec<Vec<Value>> = sqlx::query(&query)
            .try_map(|row| {
                SqlRow::Sqlite(&row).row_processor().map_err(|e| match e {
                    FabrixError::Sqlx(se) => se,
                    _ => sqlx::Error::WorkerCrashed,
                })
            })
            .fetch_all(self)
            .await?;

        Ok(DataFrame::from_row_wise_values(res)?)
    }

    async fn raw_exec(&self, _query: &str) -> FabrixResult<()> {
        todo!()
    }
}
