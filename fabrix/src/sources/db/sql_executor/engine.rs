//! Fabrix Sql engine

use async_trait::async_trait;
use sqlx::{MySqlPool, PgPool, SqlitePool};

use super::types::{row_processor_mysql, row_processor_pg, row_processor_sqlite};
use crate::{DataFrame, FabrixError, FabrixResult, Row};

#[async_trait]
pub trait Engine {
    /// connect to the database
    async fn connect(&mut self) -> FabrixResult<()>;

    /// disconnect from the database
    async fn disconnect(&mut self) -> FabrixResult<()>;
}

#[async_trait]
pub trait FabrixDatabasePool {
    async fn disconnect(&self);

    async fn raw_fetch(&self, query: &str) -> FabrixResult<Option<DataFrame>>;

    async fn raw_exec(&self, query: &str) -> FabrixResult<()>;
}

#[async_trait]
impl FabrixDatabasePool for MySqlPool {
    async fn disconnect(&self) {
        self.close().await;
    }

    async fn raw_fetch(&self, query: &str) -> FabrixResult<Option<DataFrame>> {
        let res: Vec<Row> = sqlx::query(&query)
            .try_map(|row| {
                row_processor_mysql(row).map_err(|e| match e {
                    FabrixError::Sqlx(se) => se,
                    _ => sqlx::Error::WorkerCrashed,
                })
            })
            .fetch_all(self)
            .await?;

        Ok(Some(DataFrame::from_rows(res)?))
    }

    async fn raw_exec(&self, query: &str) -> FabrixResult<()> {
        todo!()
    }
}

#[async_trait]
impl FabrixDatabasePool for PgPool {
    async fn disconnect(&self) {
        self.close().await;
    }

    async fn raw_fetch(&self, query: &str) -> FabrixResult<Option<DataFrame>> {
        let res: Vec<Row> = sqlx::query(&query)
            .try_map(|row| {
                row_processor_pg(row).map_err(|e| match e {
                    FabrixError::Sqlx(se) => se,
                    _ => sqlx::Error::WorkerCrashed,
                })
            })
            .fetch_all(self)
            .await?;

        Ok(Some(DataFrame::from_rows(res)?))
    }

    async fn raw_exec(&self, query: &str) -> FabrixResult<()> {
        todo!()
    }
}

#[async_trait]
impl FabrixDatabasePool for SqlitePool {
    async fn disconnect(&self) {
        self.close().await;
    }

    async fn raw_fetch(&self, query: &str) -> FabrixResult<Option<DataFrame>> {
        let res: Vec<Row> = sqlx::query(&query)
            .try_map(|row| {
                row_processor_sqlite(row).map_err(|e| match e {
                    FabrixError::Sqlx(se) => se,
                    _ => sqlx::Error::WorkerCrashed,
                })
            })
            .fetch_all(self)
            .await?;

        Ok(Some(DataFrame::from_rows(res)?))
    }

    async fn raw_exec(&self, query: &str) -> FabrixResult<()> {
        todo!()
    }
}
