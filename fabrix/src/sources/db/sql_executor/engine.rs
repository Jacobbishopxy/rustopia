//! Fabrix Sql engine

use async_trait::async_trait;
use sqlx::{MySqlPool, PgPool, SqlitePool};

use crate::{DataFrame, FabrixResult};

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
        todo!()
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
        todo!()
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
        todo!()
    }

    async fn raw_exec(&self, query: &str) -> FabrixResult<()> {
        todo!()
    }
}
