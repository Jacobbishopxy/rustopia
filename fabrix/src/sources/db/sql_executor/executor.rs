//! Database executor

use async_trait::async_trait;
// use sqlx::mysql::MySqlRow;
// use sqlx::postgres::PgRow;
// use sqlx::sqlite::SqliteRow;
use sqlx::{MySqlPool, PgPool, SqlitePool};

use super::engine::{Engine, FabrixDatabasePool};
use crate::{adt, DataFrame, DmlQuery, FabrixResult, SqlBuilder};

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
    pool: Option<Box<dyn FabrixDatabasePool>>,
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

macro_rules! conn_e_err {
    ($pool:expr) => {
        if $pool.is_some() {
            return Err($crate::FabrixError::new_common_error(
                "connection has already been established",
            ));
        }
    };
}

macro_rules! conn_n_err {
    ($pool:expr) => {
        if $pool.is_none() {
            return Err($crate::FabrixError::new_common_error(
                "connection has not been established yet",
            ));
        }
    };
}

#[async_trait]
impl Engine for Executor {
    async fn connect(&mut self) -> FabrixResult<()> {
        conn_e_err!(self.pool);
        match self.driver {
            SqlBuilder::Mysql => MySqlPool::connect(&self.conn_str).await.map(|pool| {
                self.pool = Some(Box::new(pool));
            })?,
            SqlBuilder::Postgres => PgPool::connect(&self.conn_str).await.map(|pool| {
                self.pool = Some(Box::new(pool));
            })?,
            SqlBuilder::Sqlite => SqlitePool::connect(&self.conn_str).await.map(|pool| {
                self.pool = Some(Box::new(pool));
            })?,
        }
        Ok(())
    }

    async fn disconnect(&mut self) -> FabrixResult<()> {
        conn_n_err!(self.pool);
        self.pool.as_ref().unwrap().disconnect().await;
        Ok(())
    }

    async fn select(&self, select: &adt::Select) -> FabrixResult<DataFrame> {
        conn_n_err!(self.pool);
        let que = self.driver.select(select);
        self.pool.as_ref().unwrap().raw_fetch(&que).await
    }
}

#[cfg(test)]
mod test_executor {

    use super::*;

    const CONN1: &'static str = "mysql://root:secret@localhost:3306/dev";
    // const CONN2: &'static str = "postgres://root:secret@localhost:5432/dev";
    // const CONN3: &'static str = "sqlite:cache/dev.sqlite";

    #[tokio::test]
    async fn test_connection() {
        let mut exc = Executor::from_str(CONN1);

        exc.connect().await.expect("connection is ok");
    }

    #[tokio::test]
    async fn test_select() {
        let mut exc = Executor::from_str(CONN1);

        exc.connect().await.expect("connection is ok");

        let select = adt::Select {
            table: "products".to_owned(),
            columns: vec![
                adt::ColumnAlias::Simple("product_id".to_owned()),
                adt::ColumnAlias::Simple("url".to_owned()),
                adt::ColumnAlias::Simple("name".to_owned()),
                adt::ColumnAlias::Simple("description".to_owned()),
                adt::ColumnAlias::Simple("price".to_owned()),
                adt::ColumnAlias::Simple("visible".to_owned()),
            ],
            filter: None,
            order: None,
            limit: None,
            offset: None,
        };

        let df = exc.select(&select).await.unwrap();

        println!("{:?}", df);
    }
}
