//! Fabrix sql executor pool

use async_trait::async_trait;
use sqlx::{MySqlPool, PgPool, SqlitePool};

use super::types::SqlRow;
use crate::{FabrixError, FabrixResult, Row, Value};

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

    // TODO: needs return type instead of ()
    /// sql string execution
    async fn exec(&self, query: &str) -> FabrixResult<()>;

    // TODO: needs return type instead of ()
    /// multiple sql string execution
    async fn exec_many(&self, _query: &[&str]) -> FabrixResult<()>;
}

#[async_trait]
impl FabrixDatabasePool for MySqlPool {
    async fn disconnect(&self) {
        self.close().await;
    }

    async fn fetch_all(&self, query: &str) -> FabrixResult<Vec<Vec<Value>>> {
        let res = sqlx::query(&query)
            .try_map(|row| {
                SqlRow::Mysql(&row).row_processor().map_err(|e| match e {
                    FabrixError::Sqlx(se) => se,
                    _ => sqlx::Error::WorkerCrashed,
                })
            })
            .fetch_all(self)
            .await?;

        Ok(res)
    }

    async fn fetch_all_with_key(&self, query: &str) -> FabrixResult<Vec<Row>> {
        let res = sqlx::query(&query)
            .try_map(|row| {
                SqlRow::Mysql(&row)
                    .row_processor()
                    .map(|mut r| {
                        let v = r.remove(0);
                        Row::new(v, r)
                    })
                    .map_err(|e| match e {
                        FabrixError::Sqlx(se) => se,
                        _ => sqlx::Error::WorkerCrashed,
                    })
            })
            .fetch_all(self)
            .await?;

        Ok(res)
    }

    async fn fetch_one(&self, query: &str) -> FabrixResult<Vec<Value>> {
        let res = sqlx::query(&query)
            .try_map(|row| {
                SqlRow::Mysql(&row).row_processor().map_err(|e| match e {
                    FabrixError::Sqlx(se) => se,
                    _ => sqlx::Error::WorkerCrashed,
                })
            })
            .fetch_one(self)
            .await?;

        Ok(res)
    }

    async fn fetch_optional(&self, query: &str) -> FabrixResult<Option<Vec<Value>>> {
        let res = sqlx::query(&query)
            .try_map(|row| {
                SqlRow::Mysql(&row).row_processor().map_err(|e| match e {
                    FabrixError::Sqlx(se) => se,
                    _ => sqlx::Error::WorkerCrashed,
                })
            })
            .fetch_optional(self)
            .await?;

        Ok(res)
    }

    async fn fetch_many(&self, _queries: &[&str]) -> FabrixResult<Vec<Vec<Value>>> {
        todo!()
    }

    async fn exec(&self, query: &str) -> FabrixResult<()> {
        sqlx::query(query).execute(self).await?;
        Ok(())
    }

    async fn exec_many(&self, _query: &[&str]) -> FabrixResult<()> {
        // use futures::TryStreamExt;
        // let mut foo = sqlx::query("").execute_many(self).await;

        // while let Some(x) = foo.try_next().await? {
        //     println!("{:?}", x);
        // }

        // Ok(())

        todo!()
    }
}

#[async_trait]
impl FabrixDatabasePool for PgPool {
    async fn disconnect(&self) {
        self.close().await;
    }

    async fn fetch_all(&self, query: &str) -> FabrixResult<Vec<Vec<Value>>> {
        let res = sqlx::query(&query)
            .try_map(|row| {
                SqlRow::Pg(&row).row_processor().map_err(|e| match e {
                    FabrixError::Sqlx(se) => se,
                    _ => sqlx::Error::WorkerCrashed,
                })
            })
            .fetch_all(self)
            .await?;

        Ok(res)
    }

    async fn fetch_all_with_key(&self, query: &str) -> FabrixResult<Vec<Row>> {
        let res = sqlx::query(&query)
            .try_map(|row| {
                SqlRow::Pg(&row)
                    .row_processor()
                    .map(|mut r| {
                        let v = r.remove(0);
                        Row::new(v, r)
                    })
                    .map_err(|e| match e {
                        FabrixError::Sqlx(se) => se,
                        _ => sqlx::Error::WorkerCrashed,
                    })
            })
            .fetch_all(self)
            .await?;

        Ok(res)
    }

    async fn fetch_one(&self, query: &str) -> FabrixResult<Vec<Value>> {
        let res = sqlx::query(&query)
            .try_map(|row| {
                SqlRow::Pg(&row).row_processor().map_err(|e| match e {
                    FabrixError::Sqlx(se) => se,
                    _ => sqlx::Error::WorkerCrashed,
                })
            })
            .fetch_one(self)
            .await?;

        Ok(res)
    }

    async fn fetch_optional(&self, query: &str) -> FabrixResult<Option<Vec<Value>>> {
        let res = sqlx::query(&query)
            .try_map(|row| {
                SqlRow::Pg(&row).row_processor().map_err(|e| match e {
                    FabrixError::Sqlx(se) => se,
                    _ => sqlx::Error::WorkerCrashed,
                })
            })
            .fetch_optional(self)
            .await?;

        Ok(res)
    }

    async fn fetch_many(&self, _queries: &[&str]) -> FabrixResult<Vec<Vec<Value>>> {
        todo!()
    }

    async fn exec(&self, query: &str) -> FabrixResult<()> {
        sqlx::query(query).execute(self).await?;
        Ok(())
    }

    async fn exec_many(&self, _query: &[&str]) -> FabrixResult<()> {
        todo!()
    }
}

#[async_trait]
impl FabrixDatabasePool for SqlitePool {
    async fn disconnect(&self) {
        self.close().await;
    }

    async fn fetch_all(&self, query: &str) -> FabrixResult<Vec<Vec<Value>>> {
        let res = sqlx::query(&query)
            .try_map(|row| {
                SqlRow::Sqlite(&row).row_processor().map_err(|e| match e {
                    FabrixError::Sqlx(se) => se,
                    _ => sqlx::Error::WorkerCrashed,
                })
            })
            .fetch_all(self)
            .await?;

        Ok(res)
    }

    async fn fetch_all_with_key(&self, query: &str) -> FabrixResult<Vec<Row>> {
        let res = sqlx::query(&query)
            .try_map(|row| {
                SqlRow::Sqlite(&row)
                    .row_processor()
                    .map(|mut r| {
                        let v = r.remove(0);
                        Row::new(v, r)
                    })
                    .map_err(|e| match e {
                        FabrixError::Sqlx(se) => se,
                        _ => sqlx::Error::WorkerCrashed,
                    })
            })
            .fetch_all(self)
            .await?;

        Ok(res)
    }

    async fn fetch_one(&self, query: &str) -> FabrixResult<Vec<Value>> {
        let res = sqlx::query(&query)
            .try_map(|row| {
                SqlRow::Sqlite(&row).row_processor().map_err(|e| match e {
                    FabrixError::Sqlx(se) => se,
                    _ => sqlx::Error::WorkerCrashed,
                })
            })
            .fetch_one(self)
            .await?;

        Ok(res)
    }

    async fn fetch_optional(&self, query: &str) -> FabrixResult<Option<Vec<Value>>> {
        let res = sqlx::query(&query)
            .try_map(|row| {
                SqlRow::Sqlite(&row).row_processor().map_err(|e| match e {
                    FabrixError::Sqlx(se) => se,
                    _ => sqlx::Error::WorkerCrashed,
                })
            })
            .fetch_optional(self)
            .await?;

        Ok(res)
    }

    async fn fetch_many(&self, _queries: &[&str]) -> FabrixResult<Vec<Vec<Value>>> {
        todo!()
    }

    async fn exec(&self, query: &str) -> FabrixResult<()> {
        sqlx::query(query).execute(self).await?;
        Ok(())
    }

    async fn exec_many(&self, _query: &[&str]) -> FabrixResult<()> {
        todo!()
    }
}
