//! tiny-df sql engine
//!
//! Similar to Python's pandas dataframe: `pd.Dataframe.to_sql`

use async_trait::async_trait;
use sqlx::mysql::{MySqlColumn, MySqlRow};
use sqlx::postgres::{PgColumn, PgRow};
use sqlx::sqlite::{SqliteColumn, SqliteRow};
use sqlx::{MySqlPool, PgPool, Row, SqlitePool};

use crate::db::{ConnInfo, TdDbError, TdDbResult};
use crate::prelude::*;
use crate::se::Sql;

/// Loader's engine
/// fetching data from database
#[async_trait]
pub trait Engine {
    async fn fetch_all(&self, query: &str) -> TdDbResult<Option<Dataframe>>;

    // async fn insert(&self, dataframe: Dataframe) -> TdDbResult<()>;

    // async fn update(&self, dataframe: Dataframe) -> TdDbResult<()>;

    // async fn upsert(&self, dataframe: Dataframe) -> TdDbResult<()>;
}

// TODO:
impl From<MySqlColumn> for DataframeData {
    fn from(v: MySqlColumn) -> Self {
        v.into()
    }
}

impl From<PgColumn> for DataframeData {
    fn from(v: PgColumn) -> Self {
        v.into()
    }
}

impl From<SqliteColumn> for DataframeData {
    fn from(v: SqliteColumn) -> Self {
        v.into()
    }
}

#[async_trait]
impl Engine for MySqlPool {
    async fn fetch_all(&self, query: &str) -> TdDbResult<Option<Dataframe>> {
        let mut d2 = sqlx::query(query)
            .map(|row: MySqlRow| {
                // map
                let cols: &[sqlx::mysql::MySqlColumn] = row.columns();
                todo!()
            })
            .fetch_all(self)
            .await?;
        todo!()
    }
}

#[async_trait]
impl Engine for PgPool {
    async fn fetch_all(&self, query: &str) -> TdDbResult<Option<Dataframe>> {
        let mut d2 = sqlx::query(query)
            .map(|row: PgRow| {
                // map
                let cols = row.columns();
                todo!()
            })
            .fetch_all(self)
            .await?;
        todo!()
    }
}

#[async_trait]
impl Engine for SqlitePool {
    async fn fetch_all(&self, query: &str) -> TdDbResult<Option<Dataframe>> {
        let mut d2 = sqlx::query(query)
            .map(|row: SqliteRow| {
                // map
                let cols = row.columns();
                todo!()
            })
            .fetch_all(self)
            .await?;
        todo!()
    }
}

pub struct Loader {
    driver: Sql,
    conn: String,
    pool: Option<Box<dyn Engine>>,
}

// TODO: transaction is required
impl Loader {
    /// from `ConnInfo`
    pub fn new(conn_info: ConnInfo) -> Self {
        Loader {
            driver: conn_info.driver.clone(),
            conn: conn_info.to_string(),
            pool: None,
        }
    }

    /// from `&str`
    pub fn from_str(conn_str: &str) -> Self {
        let mut s = conn_str.split(":");
        let driver = match s.next() {
            Some(v) => v.into(),
            None => Sql::Sqlite,
        };
        Loader {
            driver,
            conn: conn_str.to_string(),
            pool: None,
        }
    }

    // TODO:
    pub async fn fetch_all(&self, query: &str) -> TdDbResult<Option<Dataframe>> {
        match &self.pool {
            Some(p) => Ok(p.fetch_all(query).await?),
            None => Err(TdDbError::Common("Loader pool not set".to_owned())),
        }
    }
}

#[cfg(test)]
mod test_loader {

    use super::*;

    const CONN: &'static str = "mysql://root:secret@localhost:3306/dev";
    const Que: &'static str = r#"
    SELECT EXISTS(
        SELECT 1
        FROM information_schema.TABLES
        WHERE TABLE_NAME = 'table_name'
    )"#;

    #[test]
    fn test_new() {
        let loader1 = Loader::from_str(CONN);
        println!("{:?}", loader1.conn);

        let conn_info = ConnInfo::new(Sql::Mysql, "root", "secret", "localhost", 3306, "dev");
        let loader2 = Loader::new(conn_info);
        println!("{:?}", loader2.conn);

        assert_eq!(loader1.conn, loader2.conn);
    }

    #[tokio::test]
    async fn test_connection() {
        let loader = Loader::from_str(CONN);

        let df = loader.tmp_query(Que).await;

        println!("{:#?}", df);
    }
}
