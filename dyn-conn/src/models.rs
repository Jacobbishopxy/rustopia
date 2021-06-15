//!

use serde::{Deserialize, Serialize};
use sqlx::{Connection, Pool};
use std::{collections::HashMap, fmt::Display};

use sqlx::mysql::MySql;
use sqlx::postgres::Postgres;
use sqlx::{MySqlConnection, PgConnection};

pub enum DynPoolOptions {
    Mysql(Pool<MySql>),
    Postgres(Pool<Postgres>),
}

impl DynPoolOptions {
    pub async fn disconnect(&self) {
        match &self {
            DynPoolOptions::Mysql(c) => {
                c.close().await;
            }
            DynPoolOptions::Postgres(c) => {
                c.close().await;
            }
        }
    }
}

#[derive(Deserialize, Serialize)]
pub enum Driver {
    Postgres,
    Mysql,
}

impl Display for Driver {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self {
            Driver::Postgres => write!(f, "postgres"),
            Driver::Mysql => write!(f, "mysql"),
        }
    }
}

/// JSON body example:
/// {
///     "driver": "Postgres",
///     "username": "pg",
///     "password": "pw",
///     "host": "localhost",
///     "port": 5432,
///     "database": "dev"
/// }
#[derive(Deserialize, Serialize)]
pub struct Conn {
    pub driver: Driver,
    pub username: String,
    pub password: String,
    pub host: String,
    pub port: i32,
    pub database: String,
}

impl Conn {
    pub fn new(
        driver: Driver,
        username: String,
        password: String,
        host: String,
        port: i32,
        database: String,
    ) -> Conn {
        Conn {
            driver,
            username,
            password,
            host,
            port,
            database,
        }
    }

    pub fn to_string(&self) -> String {
        format!(
            "{}://{}:{}@{}:{}/{}",
            self.driver, self.username, self.password, self.host, self.port, self.database,
        )
    }
}

pub struct DynConn {
    pub store: HashMap<String, Conn>,
    pools: HashMap<String, DynPoolOptions>,
}

impl DynConn {
    pub fn new() -> DynConn {
        DynConn {
            store: HashMap::new(),
            pools: HashMap::new(),
        }
    }

    /// Check whether database connection string is available
    pub async fn check_connection(conn: &Conn) -> bool {
        match conn.driver {
            Driver::Postgres => match PgConnection::connect(&conn.to_string()).await {
                Ok(_) => true,
                Err(_) => false,
            },
            Driver::Mysql => match MySqlConnection::connect(&conn.to_string()).await {
                Ok(_) => true,
                Err(_) => false,
            },
        }
    }

    /// show dynamic connection's keys
    pub fn show_keys(&self) -> Vec<String> {
        self.store.keys().map(|f| f.to_owned()).collect()
    }

    /// show store, value as converted string
    pub fn show_store(&self) -> HashMap<String, String> {
        self.store
            .iter()
            .map(|(k, v)| (k.clone(), v.to_string()))
            .collect()
    }

    /// drop an existing connection pool
    pub async fn drop_conn(&mut self, key: &str) -> bool {
        match &self.pools.contains_key(key) {
            true => {
                self.pools.get(key).unwrap().disconnect().await;
                true
            }
            false => false,
        }
    }

    /// create new connection pool and store it in memory
    pub fn new_conn(&mut self, key: &str, conn: Conn) -> String {
        match &self.store.contains_key(key) {
            true => format!("Key \"{:?}\" already existed", key),
            false => {
                self.store.insert(key.to_owned(), conn);
                format!("New conn {:?} success", &key)
            }
        }
    }
}
