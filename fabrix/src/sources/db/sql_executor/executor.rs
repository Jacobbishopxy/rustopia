//! Database executor

use async_trait::async_trait;
use sqlx::mysql::MySqlRow;
use sqlx::postgres::PgRow;
use sqlx::sqlite::SqliteRow;
use sqlx::{MySqlPool, PgPool, SqlitePool};

use super::engine::{Engine, FabrixDatabasePool};
use crate::{DataFrame, FabrixError, FabrixResult, Row, SqlBuilder};

/// Connection information
pub struct ConnInfo {
    pub driver: SqlBuilder,
    pub username: String,
    pub password: String,
    pub host: String,
    pub port: i32,
    pub database: String,
}

impl ConnInfo {
    pub fn new(
        driver: SqlBuilder,
        username: &str,
        password: &str,
        host: &str,
        port: i32,
        database: &str,
    ) -> ConnInfo {
        ConnInfo {
            driver,
            username: username.to_owned(),
            password: password.to_owned(),
            host: host.to_owned(),
            port,
            database: database.to_owned(),
        }
    }
}

impl std::fmt::Display for ConnInfo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}://{}:{}@{}:{}/{}",
            self.driver, self.username, self.password, self.host, self.port, self.database,
        )
    }
}
pub struct Executor {
    driver: SqlBuilder,
    conn_str: String,
    pool: Option<Box<dyn FabrixDatabasePool + Send + Sync>>,
}

impl Executor {
    pub fn new(conn_info: ConnInfo) -> Self {
        Executor {
            driver: conn_info.driver.clone(),
            conn_str: conn_info.to_string(),
            pool: None,
        }
    }

    pub fn from_str(conn_str: &str) -> Self {
        let mut s = conn_str.split(":");
        let driver = match s.next() {
            Some(v) => v.into(),
            None => SqlBuilder::Sqlite,
        };
        Executor {
            driver,
            conn_str: conn_str.to_string(),
            pool: None,
        }
    }
}

unsafe impl Send for Executor {}
unsafe impl Sync for Executor {}

#[async_trait]
impl Engine for Executor {
    async fn connect(&mut self) -> FabrixResult<()> {
        if self.pool.is_some() {
            match self.driver {
                SqlBuilder::Mysql => MySqlPool::connect(&self.conn_str).await.map(|pool| {
                    self.pool = Some(Box::new(pool));
                    Ok(())
                })?,
                SqlBuilder::Postgres => PgPool::connect(&self.conn_str).await.map(|pool| {
                    self.pool = Some(Box::new(pool));
                    Ok(())
                })?,
                SqlBuilder::Sqlite => SqlitePool::connect(&self.conn_str).await.map(|pool| {
                    self.pool = Some(Box::new(pool));
                    Ok(())
                })?,
            }
        } else {
            Err(FabrixError::new_common_error(
                "connection has already been established",
            ))
        }
    }
}
