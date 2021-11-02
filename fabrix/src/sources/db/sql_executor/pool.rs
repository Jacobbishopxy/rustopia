//! Fabrix sql executor pool

use async_trait::async_trait;
use sqlx::{
    mysql::MySqlQueryResult, postgres::PgQueryResult, sqlite::SqliteQueryResult, MySql, MySqlPool,
    PgPool, Postgres, Sqlite, SqlitePool, Transaction,
};

use super::processor::SqlRowProcessor;
use crate::{adt::ExecutionResult, FabrixError, FabrixResult, Row, Value};

impl From<MySqlQueryResult> for ExecutionResult {
    fn from(result: MySqlQueryResult) -> Self {
        ExecutionResult {
            rows_affected: result.rows_affected(),
        }
    }
}

impl From<PgQueryResult> for ExecutionResult {
    fn from(result: PgQueryResult) -> Self {
        ExecutionResult {
            rows_affected: result.rows_affected(),
        }
    }
}

impl From<SqliteQueryResult> for ExecutionResult {
    fn from(result: SqliteQueryResult) -> Self {
        ExecutionResult {
            rows_affected: result.rows_affected(),
        }
    }
}

pub enum PoolTransaction<'a> {
    Mysql(Transaction<'a, MySql>),
    Pg(Transaction<'a, Postgres>),
    Sqlite(Transaction<'a, Sqlite>),
}

impl<'a> PoolTransaction<'_> {
    pub async fn execute(&mut self, sql: &str) -> FabrixResult<ExecutionResult> {
        match self {
            Self::Mysql(tx) => {
                let result = sqlx::query(&sql).execute(tx).await?;
                Ok(ExecutionResult::from(result))
            }
            Self::Pg(tx) => {
                let result = sqlx::query(&sql).execute(tx).await?;
                Ok(ExecutionResult::from(result))
            }
            Self::Sqlite(tx) => {
                let result = sqlx::query(&sql).execute(tx).await?;
                Ok(ExecutionResult::from(result))
            }
        }
    }

    pub async fn rollback(self) -> FabrixResult<()> {
        match self {
            Self::Mysql(tx) => Ok(tx.rollback().await?),
            Self::Pg(tx) => Ok(tx.rollback().await?),
            Self::Sqlite(tx) => Ok(tx.rollback().await?),
        }
    }

    pub async fn commit(self) -> FabrixResult<()> {
        match self {
            PoolTransaction::Mysql(tx) => Ok(tx.commit().await?),
            PoolTransaction::Pg(tx) => Ok(tx.commit().await?),
            PoolTransaction::Sqlite(tx) => Ok(tx.commit().await?),
        }
    }
}

/// database pool interface
#[async_trait]
pub trait FabrixDatabasePool: Send + Sync {
    /// disconnect from the current database
    async fn disconnect(&self);

    /// fetch all and return 2d Value Vec
    async fn fetch_all(&self, query: &str) -> FabrixResult<Vec<Vec<Value>>>;

    /// fetch all with primary key. Make sure the first select column is always the primary key
    async fn fetch_all_with_key(&self, query: &str) -> FabrixResult<Vec<Row>>;

    /// fetch one and return 1d Value Vec
    async fn fetch_one(&self, query: &str) -> FabrixResult<Vec<Value>>;

    /// fetch optional
    async fn fetch_optional(&self, query: &str) -> FabrixResult<Option<Vec<Value>>>;

    /// fetch many
    async fn fetch_many(&self, queries: &[&str]) -> FabrixResult<Vec<Vec<Value>>>;

    /// sql string execution
    async fn execute(&self, query: &str) -> FabrixResult<ExecutionResult>;

    /// multiple sql string execution
    async fn execute_many(&self, _query: &[&str]) -> FabrixResult<ExecutionResult>;

    /// create a transaction instance
    async fn transaction(&self) -> FabrixResult<PoolTransaction<'_>>;
}

fn convert_pool_err(e: FabrixError) -> sqlx::Error {
    match e {
        FabrixError::Sqlx(se) => se,
        _ => sqlx::Error::WorkerCrashed,
    }
}

#[async_trait]
impl FabrixDatabasePool for MySqlPool {
    async fn disconnect(&self) {
        self.close().await;
    }

    async fn fetch_all(&self, query: &str) -> FabrixResult<Vec<Vec<Value>>> {
        let mut srp = SqlRowProcessor::new();
        let res = sqlx::query(&query)
            .try_map(|row| srp.process(&row).map_err(convert_pool_err))
            .fetch_all(self)
            .await?;

        Ok(res)
    }

    async fn fetch_all_with_key(&self, query: &str) -> FabrixResult<Vec<Row>> {
        let mut srp = SqlRowProcessor::new();
        let res = sqlx::query(&query)
            .try_map(|row| srp.process_to_row(&row).map_err(convert_pool_err))
            .fetch_all(self)
            .await?;

        Ok(res)
    }

    async fn fetch_one(&self, query: &str) -> FabrixResult<Vec<Value>> {
        let mut srp = SqlRowProcessor::new();
        let res = sqlx::query(&query)
            .try_map(|row| srp.process(&row).map_err(convert_pool_err))
            .fetch_one(self)
            .await?;

        Ok(res)
    }

    async fn fetch_optional(&self, query: &str) -> FabrixResult<Option<Vec<Value>>> {
        let mut srp = SqlRowProcessor::new();
        let res = sqlx::query(&query)
            .try_map(|row| srp.process(&row).map_err(convert_pool_err))
            .fetch_optional(self)
            .await?;

        Ok(res)
    }

    async fn fetch_many(&self, _queries: &[&str]) -> FabrixResult<Vec<Vec<Value>>> {
        todo!()
    }

    async fn execute(&self, query: &str) -> FabrixResult<ExecutionResult> {
        let eff = sqlx::query(query).execute(self).await?;
        Ok(eff.into())
    }

    async fn execute_many(&self, query: &[&str]) -> FabrixResult<ExecutionResult> {
        // use futures::TryStreamExt;
        // let mut foo = sqlx::query("").execute_many(self).await;

        // while let Some(x) = foo.try_next().await? {
        //     println!("{:?}", x);
        // }

        // Ok(())

        todo!()
    }

    async fn transaction(&self) -> FabrixResult<PoolTransaction<'_>> {
        Ok(PoolTransaction::Mysql(self.begin().await?))
    }
}

#[async_trait]
impl FabrixDatabasePool for PgPool {
    async fn disconnect(&self) {
        self.close().await;
    }

    async fn fetch_all(&self, query: &str) -> FabrixResult<Vec<Vec<Value>>> {
        let mut srp = SqlRowProcessor::new();
        let res = sqlx::query(&query)
            .try_map(|row| srp.process(&row).map_err(convert_pool_err))
            .fetch_all(self)
            .await?;

        Ok(res)
    }

    async fn fetch_all_with_key(&self, query: &str) -> FabrixResult<Vec<Row>> {
        let mut srp = SqlRowProcessor::new();
        let res = sqlx::query(&query)
            .try_map(|row| srp.process_to_row(&row).map_err(convert_pool_err))
            .fetch_all(self)
            .await?;

        Ok(res)
    }

    async fn fetch_one(&self, query: &str) -> FabrixResult<Vec<Value>> {
        let mut srp = SqlRowProcessor::new();
        let res = sqlx::query(&query)
            .try_map(|row| srp.process(&row).map_err(convert_pool_err))
            .fetch_one(self)
            .await?;

        Ok(res)
    }

    async fn fetch_optional(&self, query: &str) -> FabrixResult<Option<Vec<Value>>> {
        let mut srp = SqlRowProcessor::new();
        let res = sqlx::query(&query)
            .try_map(|row| srp.process(&row).map_err(convert_pool_err))
            .fetch_optional(self)
            .await?;

        Ok(res)
    }

    async fn fetch_many(&self, _queries: &[&str]) -> FabrixResult<Vec<Vec<Value>>> {
        todo!()
    }

    async fn execute(&self, query: &str) -> FabrixResult<ExecutionResult> {
        let eff = sqlx::query(query).execute(self).await?;
        Ok(eff.into())
    }

    async fn execute_many(&self, _query: &[&str]) -> FabrixResult<ExecutionResult> {
        todo!()
    }

    async fn transaction(&self) -> FabrixResult<PoolTransaction<'_>> {
        Ok(PoolTransaction::Pg(self.begin().await?))
    }
}

#[async_trait]
impl FabrixDatabasePool for SqlitePool {
    async fn disconnect(&self) {
        self.close().await;
    }

    async fn fetch_all(&self, query: &str) -> FabrixResult<Vec<Vec<Value>>> {
        let mut srp = SqlRowProcessor::new();
        let res = sqlx::query(&query)
            .try_map(|row| srp.process(&row).map_err(convert_pool_err))
            .fetch_all(self)
            .await?;

        Ok(res)
    }

    async fn fetch_all_with_key(&self, query: &str) -> FabrixResult<Vec<Row>> {
        let mut srp = SqlRowProcessor::new();
        let res = sqlx::query(&query)
            .try_map(|row| srp.process_to_row(&row).map_err(convert_pool_err))
            .fetch_all(self)
            .await?;

        Ok(res)
    }

    async fn fetch_one(&self, query: &str) -> FabrixResult<Vec<Value>> {
        let mut srp = SqlRowProcessor::new();
        let res = sqlx::query(&query)
            .try_map(|row| srp.process(&row).map_err(convert_pool_err))
            .fetch_one(self)
            .await?;

        Ok(res)
    }

    async fn fetch_optional(&self, query: &str) -> FabrixResult<Option<Vec<Value>>> {
        let mut srp = SqlRowProcessor::new();
        let res = sqlx::query(&query)
            .try_map(|row| srp.process(&row).map_err(convert_pool_err))
            .fetch_optional(self)
            .await?;

        Ok(res)
    }

    async fn fetch_many(&self, _queries: &[&str]) -> FabrixResult<Vec<Vec<Value>>> {
        todo!()
    }

    async fn execute(&self, query: &str) -> FabrixResult<ExecutionResult> {
        let eff = sqlx::query(query).execute(self).await?;
        Ok(eff.into())
    }

    async fn execute_many(&self, _query: &[&str]) -> FabrixResult<ExecutionResult> {
        todo!()
    }

    async fn transaction(&self) -> FabrixResult<PoolTransaction<'_>> {
        Ok(PoolTransaction::Sqlite(self.begin().await?))
    }
}
