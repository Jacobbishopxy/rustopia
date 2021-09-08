//! tiny-df sql engine
//!
//! Similar to Python's pandas dataframe: `pd.Dataframe.to_sql`

use std::{any::Any, fmt::Display};

use sqlx::{Connection, MySqlConnection, PgConnection, SqliteConnection};

use crate::se::sql::Sql;

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

pub struct Loader {
    driver: Sql,
    conn: String,
}

impl Loader {
    pub fn new(conn_info: ConnInfo) -> Self {
        Loader {
            driver: conn_info.driver.clone(),
            conn: conn_info.to_string(),
        }
    }

    pub fn from_str(conn_str: &str) -> Self {
        let mut s = conn_str.split(":");
        let driver = match s.next() {
            Some(v) => v.into(),
            None => Sql::Sqlite,
        };
        Loader {
            driver,
            conn: conn_str.to_string(),
        }
    }

    // TODO:
    #[allow(dead_code)]
    async fn gen_single_connection(&self) -> Box<dyn Any> {
        match self.driver {
            Sql::Mysql => Box::new(MySqlConnection::connect(&self.conn).await),
            Sql::Postgres => Box::new(PgConnection::connect(&self.conn).await),
            Sql::Sqlite => Box::new(SqliteConnection::connect(&self.conn).await),
        }
    }

    // TODO:
    #[allow(dead_code)]
    async fn gen_pool_connection(&self) {
        unimplemented!()
    }
}

#[test]
fn test_loader_new() {
    let loader1 = Loader::from_str("mysql://root:secret@localhost:3306/dev");
    println!("{:?}", loader1.conn);

    let conn_info = ConnInfo::new(Sql::Mysql, "root", "secret", "localhost", 3306, "dev");
    let loader2 = Loader::new(conn_info);
    println!("{:?}", loader2.conn);

    assert_eq!(loader1.conn, loader2.conn);
}
