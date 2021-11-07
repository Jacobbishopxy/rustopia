//! Fabrix sql executor pool

use async_trait::async_trait;
use futures::TryStreamExt;
use sqlx::{
    mysql::MySqlQueryResult, postgres::PgQueryResult, sqlite::SqliteQueryResult, Executor, MySql,
    MySqlPool, PgPool, Postgres, Sqlite, SqlitePool, Transaction,
};

use super::processor::SqlRowProcessor;
use crate::{adt::ExecutionResult, FabrixError, FabrixResult, Row, D1, D2};

/// turn MySqlQueryResult into ExecutionResult
impl From<MySqlQueryResult> for ExecutionResult {
    fn from(result: MySqlQueryResult) -> Self {
        ExecutionResult {
            rows_affected: result.rows_affected(),
        }
    }
}

/// turn PgQueryResult into ExecutionResult
impl From<PgQueryResult> for ExecutionResult {
    fn from(result: PgQueryResult) -> Self {
        ExecutionResult {
            rows_affected: result.rows_affected(),
        }
    }
}

/// turn SqliteQueryResult into ExecutionResult
impl From<SqliteQueryResult> for ExecutionResult {
    fn from(result: SqliteQueryResult) -> Self {
        ExecutionResult {
            rows_affected: result.rows_affected(),
        }
    }
}

/// Loader transaction aims to provide a common interface for all database transaction objects
pub(crate) enum LoaderTransaction<'a> {
    Mysql(Transaction<'a, MySql>),
    Pg(Transaction<'a, Postgres>),
    Sqlite(Transaction<'a, Sqlite>),
}

impl<'a> LoaderTransaction<'a> {
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
            LoaderTransaction::Mysql(tx) => Ok(tx.commit().await?),
            LoaderTransaction::Pg(tx) => Ok(tx.commit().await?),
            LoaderTransaction::Sqlite(tx) => Ok(tx.commit().await?),
        }
    }
}

/// database loader interface
#[async_trait]
pub(crate) trait FabrixDatabaseLoader: Send + Sync {
    /// disconnect from the current database
    async fn disconnect(&self);

    /// fetch all and return 2d Value Vec
    async fn fetch_all(&self, query: &str) -> FabrixResult<D2>;

    /// fetch all with primary key. Make sure the first select column is always the primary key
    async fn fetch_all_with_key(&self, query: &str) -> FabrixResult<Vec<Row>>;

    /// fetch one and return 1d Value Vec
    async fn fetch_one(&self, query: &str) -> FabrixResult<D1>;

    /// fetch optional
    async fn fetch_optional(&self, query: &str) -> FabrixResult<Option<D1>>;

    /// fetch many
    async fn fetch_many(&self, queries: &[String]) -> FabrixResult<Vec<D2>>;

    /// sql string execution
    async fn execute(&self, query: &str) -> FabrixResult<ExecutionResult>;

    /// multiple sql string execution. Beware, this is not atomic, if needs to be atomic, use transaction
    async fn execute_many(&self, queries: &[String]) -> FabrixResult<ExecutionResult>;

    /// create a transaction instance
    async fn transaction(&self) -> FabrixResult<LoaderTransaction<'_>>;
}

/// LoaderPool
pub(crate) enum LoaderPool {
    Mysql(MySqlPool),
    Pg(PgPool),
    Sqlite(SqlitePool),
}

impl From<MySqlPool> for LoaderPool {
    fn from(pool: MySqlPool) -> Self {
        LoaderPool::Mysql(pool)
    }
}

impl From<PgPool> for LoaderPool {
    fn from(pool: PgPool) -> Self {
        LoaderPool::Pg(pool)
    }
}

impl From<SqlitePool> for LoaderPool {
    fn from(pool: SqlitePool) -> Self {
        LoaderPool::Sqlite(pool)
    }
}

#[async_trait]
impl FabrixDatabaseLoader for LoaderPool {
    async fn disconnect(&self) {
        match self {
            Self::Mysql(pool) => pool.close().await,
            Self::Pg(pool) => pool.close().await,
            Self::Sqlite(pool) => pool.close().await,
        }
    }

    async fn fetch_all(&self, query: &str) -> FabrixResult<D2> {
        let mut srp = SqlRowProcessor::new();
        let res = match self {
            Self::Mysql(pool) => {
                sqlx::query(&query)
                    .try_map(|row| srp.process(&row).map_err(|e| e.into()))
                    .fetch_all(pool)
                    .await?
            }
            Self::Pg(pool) => {
                sqlx::query(&query)
                    .try_map(|row| srp.process(&row).map_err(|e| e.into()))
                    .fetch_all(pool)
                    .await?
            }
            Self::Sqlite(pool) => {
                sqlx::query(&query)
                    .try_map(|row| srp.process(&row).map_err(|e| e.into()))
                    .fetch_all(pool)
                    .await?
            }
        };

        Ok(res)
    }

    async fn fetch_all_with_key(&self, query: &str) -> FabrixResult<Vec<Row>> {
        let mut srp = SqlRowProcessor::new();
        let res = match self {
            Self::Mysql(pool) => {
                sqlx::query(&query)
                    .try_map(|row| srp.process_to_row(&row).map_err(|e| e.into()))
                    .fetch_all(pool)
                    .await?
            }
            Self::Pg(pool) => {
                sqlx::query(&query)
                    .try_map(|row| srp.process_to_row(&row).map_err(|e| e.into()))
                    .fetch_all(pool)
                    .await?
            }
            Self::Sqlite(pool) => {
                sqlx::query(&query)
                    .try_map(|row| srp.process_to_row(&row).map_err(|e| e.into()))
                    .fetch_all(pool)
                    .await?
            }
        };

        Ok(res)
    }

    async fn fetch_one(&self, query: &str) -> FabrixResult<D1> {
        let mut srp = SqlRowProcessor::new();
        let res = match self {
            Self::Mysql(pool) => {
                sqlx::query(&query)
                    .try_map(|row| srp.process(&row).map_err(|e| e.into()))
                    .fetch_one(pool)
                    .await?
            }
            Self::Pg(pool) => {
                sqlx::query(&query)
                    .try_map(|row| srp.process(&row).map_err(|e| e.into()))
                    .fetch_one(pool)
                    .await?
            }
            Self::Sqlite(pool) => {
                sqlx::query(&query)
                    .try_map(|row| srp.process(&row).map_err(|e| e.into()))
                    .fetch_one(pool)
                    .await?
            }
        };

        Ok(res)
    }

    async fn fetch_optional(&self, query: &str) -> FabrixResult<Option<D1>> {
        let mut srp = SqlRowProcessor::new();
        let res = match self {
            Self::Mysql(pool) => {
                sqlx::query(&query)
                    .try_map(|row| srp.process(&row).map_err(|e| e.into()))
                    .fetch_optional(pool)
                    .await?
            }
            Self::Pg(pool) => {
                sqlx::query(&query)
                    .try_map(|row| srp.process(&row).map_err(|e| e.into()))
                    .fetch_optional(pool)
                    .await?
            }
            Self::Sqlite(pool) => {
                sqlx::query(&query)
                    .try_map(|row| srp.process(&row).map_err(|e| e.into()))
                    .fetch_optional(pool)
                    .await?
            }
        };

        Ok(res)
    }

    async fn fetch_many(&self, queries: &[String]) -> FabrixResult<Vec<D2>> {
        // let queries = queries.join(";");

        todo!()
    }

    async fn execute(&self, query: &str) -> FabrixResult<ExecutionResult> {
        let eff = match self {
            Self::Mysql(pool) => sqlx::query(query).execute(pool).await?.into(),
            Self::Pg(pool) => sqlx::query(query).execute(pool).await?.into(),
            Self::Sqlite(pool) => sqlx::query(query).execute(pool).await?.into(),
        };
        Ok(eff)
    }

    async fn execute_many(&self, queries: &[String]) -> FabrixResult<ExecutionResult> {
        let queries = queries.join(";");
        let mut rows_affected = 0;

        match self {
            Self::Mysql(pool) => {
                let mut stream = pool.execute_many(&queries[..]);
                while let Ok(Some(r)) = stream.try_next().await {
                    rows_affected += r.rows_affected();
                }
            }
            Self::Pg(pool) => {
                let mut stream = pool.execute_many(&queries[..]);
                while let Ok(Some(r)) = stream.try_next().await {
                    rows_affected += r.rows_affected();
                }
            }
            Self::Sqlite(pool) => {
                let mut stream = pool.execute_many(&queries[..]);
                while let Ok(Some(r)) = stream.try_next().await {
                    rows_affected += r.rows_affected();
                }
            }
        };

        Ok(rows_affected.into())
    }

    async fn transaction(&self) -> FabrixResult<LoaderTransaction<'_>> {
        let txn = match self {
            Self::Mysql(pool) => LoaderTransaction::Mysql(pool.begin().await?),
            Self::Pg(pool) => LoaderTransaction::Pg(pool.begin().await?),
            Self::Sqlite(pool) => LoaderTransaction::Sqlite(pool.begin().await?),
        };

        Ok(txn)
    }
}

#[cfg(test)]
mod test_pool {
    use super::*;
    use crate::{value, DdlQuery, DmlMutation, DmlQuery, SqlBuilder};
    use futures::TryStreamExt;
    use sqlx::{Executor, Row};

    const CONN1: &'static str = "mysql://root:secret@localhost:3306/dev";
    const CONN2: &'static str = "postgres://root:secret@localhost:5432/dev";
    const CONN3: &'static str = "sqlite:/home/jacob/dev.sqlite";

    #[tokio::test]
    async fn test_sqlx_execute_many() {
        let pool = sqlx::MySqlPool::connect(CONN1).await.unwrap();

        let sql = r#"
        CREATE TABLE IF NOT EXISTS recipes (
            recipe_id INT NOT NULL,
            recipe_name VARCHAR(30) NOT NULL,
            PRIMARY KEY (recipe_id),
            UNIQUE (recipe_name)
          );

        INSERT INTO recipes
            (recipe_id, recipe_name)
        VALUES
            (1,"Tacos"),
            (2,"Tomato Soup"),
            (3,"Grilled Cheese");

        INSERT INTO recipes
            (recipe_id, recipe_name)
        VALUES
            (3, 'Cake'),
            (4, 'Pizza'),
            (5, 'Salad');
        "#;

        let mut stream = pool.execute_many(sql);

        println!("{:?}", "Execution starts...");

        loop {
            match stream.try_next().await {
                Ok(Some(r)) => println!("{:?}", r),
                Ok(None) => break,
                Err(e) => {
                    println!("{:?}", e);
                    break;
                }
            }
        }
    }

    #[tokio::test]
    async fn test_sqlx_fetch_many() {
        // TODO: test query.fetch_many
        unimplemented!()
    }

    // Test get a table's schema
    #[tokio::test]
    async fn test_fetch_one() {
        // let pool1 = LoaderPool::from(sqlx::MySqlPool::connect(CONN1).await.unwrap());

        // let que = SqlBuilder::Mysql.check_table_schema("test_table");

        // let df = pool1.fetch_all(&que).await.unwrap();

        // println!("{:?}", df);

        // let pool2 = LoaderPool::from(sqlx::PgPool::connect(CONN2).await.unwrap());

        // let que = SqlBuilder::Postgres.check_table_schema("author");

        // let df = pool2.fetch_all(&que).await.unwrap();

        // println!("{:?}", df);

        let sqlx_pool = sqlx::SqlitePool::connect(CONN3).await.unwrap();

        let que = SqlBuilder::Sqlite.check_table_schema("tag");

        let res = sqlx::query(&que)
            .try_map(|row: sqlx::sqlite::SqliteRow| {
                let name: String = row.get_unchecked(0);
                let col_type: String = row.get_unchecked(1);
                let is_nullable: bool = row.get_unchecked(2);
                Ok(vec![value!(name), value!(col_type), value!(is_nullable)])
            })
            .fetch_all(&sqlx_pool)
            .await
            .unwrap();

        println!("{:?}", res);
    }

    // Test table if exists
    #[tokio::test]
    async fn test_fetch_optional() {
        let pool1 = LoaderPool::from(sqlx::MySqlPool::connect(CONN1).await.unwrap());

        let que = SqlBuilder::Mysql.check_table_exists("test_table");

        let df = pool1.fetch_optional(&que).await.unwrap();

        println!("{:?}", df);

        let pool2 = LoaderPool::from(sqlx::PgPool::connect(CONN2).await.unwrap());

        let que = SqlBuilder::Postgres.check_table_exists("author");

        let df = pool2.fetch_optional(&que).await.unwrap();

        println!("{:?}", df);

        let pool3 = LoaderPool::from(sqlx::SqlitePool::connect(CONN3).await.unwrap());

        let que = SqlBuilder::Sqlite.check_table_exists("tag");

        let df = pool3.fetch_optional(&que).await.unwrap();

        println!("{:?}", df);
    }
}
