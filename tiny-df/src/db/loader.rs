//! tiny-df sql engine
//!
//! Similar to Python's pandas dataframe: `pd.Dataframe.to_sql`

use std::fmt::Display;

use async_trait::async_trait;
use sqlx::mysql::MySqlRow;
use sqlx::{
    Connection, MySqlConnection, MySqlPool, PgConnection, PgPool, SqliteConnection, SqlitePool,
};

use crate::db::TdDbResult;
use crate::prelude::*;
use crate::se::Sql;

use super::TdDbError;

pub struct ConnInfo {
    pub driver: Sql,
    pub username: String,
    pub password: String,
    pub host: String,
    pub port: i32,
    pub database: String,
}

impl ConnInfo {
    pub fn new(
        driver: Sql,
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

impl Display for ConnInfo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}://{}:{}@{}:{}/{}",
            self.driver, self.username, self.password, self.host, self.port, self.database,
        )
    }
}

pub enum TdConnection {
    Mysql(MySqlConnection),
    Postgres(PgConnection),
    Sqlite(SqliteConnection),
}

pub enum TdPool {
    Mysql(MySqlPool),
    Postgres(PgPool),
    Sqlite(SqlitePool),
}

pub struct Loader {
    driver: Sql,
    conn: String,
    pool: Option<TdPool>,
}

#[async_trait]
pub trait TdConnTrait {
    async fn connect(&self) -> TdDbResult<TdConnection>;
}

#[async_trait]
pub trait TdPoolTrait {
    async fn pool(&self) -> TdDbResult<TdPool>;
}

#[async_trait]
impl TdConnTrait for Loader {
    async fn connect(&self) -> TdDbResult<TdConnection> {
        match self.driver {
            Sql::Mysql => Ok(TdConnection::Mysql(
                MySqlConnection::connect(&self.conn).await?,
            )),
            Sql::Postgres => Ok(TdConnection::Postgres(
                PgConnection::connect(&self.conn).await?,
            )),
            Sql::Sqlite => Ok(TdConnection::Sqlite(
                SqliteConnection::connect(&self.conn).await?,
            )),
        }
    }
}

#[async_trait]
impl TdPoolTrait for Loader {
    async fn pool(&self) -> TdDbResult<TdPool> {
        match self.driver {
            Sql::Mysql => Ok(TdPool::Mysql(MySqlPool::connect(&self.conn).await?)),
            Sql::Postgres => Ok(TdPool::Postgres(PgPool::connect(&self.conn).await?)),
            Sql::Sqlite => Ok(TdPool::Sqlite(SqlitePool::connect(&self.conn).await?)),
        }
    }
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
    pub async fn tmp_query(&self, query: &str) -> TdDbResult<Option<Dataframe>> {
        match self.connect().await {
            Ok(c) => match c {
                TdConnection::Mysql(c) => todo!(),
                TdConnection::Postgres(c) => todo!(),
                TdConnection::Sqlite(c) => todo!(),
            },
            Err(e) => Err(e),
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
