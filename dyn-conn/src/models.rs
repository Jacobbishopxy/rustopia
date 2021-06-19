//!

use serde::{Deserialize, Serialize};
use sqlx::{Connection, Pool};
use std::{collections::HashMap, fmt::Display};

use sqlx::mysql::{MySql, MySqlPoolOptions};
use sqlx::postgres::{PgPoolOptions, Postgres};
use sqlx::{MySqlConnection, PgConnection};

/// dynamic pool options
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

#[derive(Deserialize, Serialize, Clone)]
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
#[derive(Deserialize, Serialize, Clone)]
pub struct ConnInfo {
    pub driver: Driver,
    pub username: String,
    pub password: String,
    pub host: String,
    pub port: i32,
    pub database: String,
}

impl ConnInfo {
    pub fn new(
        driver: Driver,
        username: String,
        password: String,
        host: String,
        port: i32,
        database: String,
    ) -> ConnInfo {
        ConnInfo {
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

    /// convert connection info to Conn struct
    pub async fn to_conn(&self) -> Result<Conn, sqlx::Error> {
        let uri = &self.to_string();

        match self.driver {
            Driver::Postgres => {
                let pool = PgPoolOptions::new()
                    .max_connections(10)
                    .connect(uri)
                    .await?;
                let pool = DynPoolOptions::Postgres(pool);
                Ok(Conn {
                    info: self.clone(),
                    pool,
                })
            }
            Driver::Mysql => {
                let pool = MySqlPoolOptions::new()
                    .max_connections(10)
                    .connect(uri)
                    .await?;
                let pool = DynPoolOptions::Mysql(pool);
                Ok(Conn {
                    info: self.clone(),
                    pool,
                })
            }
        }
    }
}

/// Conn struct contains a database connection info, and a connection pool's instance
pub struct Conn {
    pub info: ConnInfo,
    pub pool: DynPoolOptions,
}

/// using hash map to maintain multiple Conn structs
pub struct DynConn {
    pub store: HashMap<String, Conn>,
}

impl DynConn {
    pub fn new() -> DynConn {
        DynConn {
            store: HashMap::new(),
        }
    }

    /// check whether database connection string is available
    pub async fn check_connection(conn_info: &ConnInfo) -> bool {
        match conn_info.driver {
            Driver::Postgres => match PgConnection::connect(&conn_info.to_string()).await {
                Ok(_) => true,
                Err(_) => false,
            },
            Driver::Mysql => match MySqlConnection::connect(&conn_info.to_string()).await {
                Ok(_) => true,
                Err(_) => false,
            },
        }
    }

    // giving a key, check if it's in store
    pub fn check_key(&self, key: &str) -> bool {
        self.store.contains_key(key)
    }

    pub fn get_conn(&self, key: &str) -> Option<&Conn> {
        self.store.get(key)
    }

    /// show dynamic connection's keys
    pub fn show_keys(&self) -> Vec<String> {
        self.store.keys().map(|f| f.to_owned()).collect()
    }

    /// show store, value as converted string
    pub fn show_info(&self) -> HashMap<String, String> {
        self.store
            .iter()
            .map(|(k, v)| (k.clone(), v.info.to_string()))
            .collect()
    }

    /// drop an existing connection pool
    pub async fn delete_conn(&mut self, key: &str) -> String {
        match &self.store.contains_key(key) {
            true => {
                self.store.get(key).unwrap().pool.disconnect().await;
                format!("Disconnected from {:?}", key)
            }
            false => format!("Key \"{:?}\" does not exist", key),
        }
    }

    /// create new connection pool and store it in memory
    pub async fn create_conn(&mut self, key: &str, conn_info: ConnInfo) -> String {
        match self.store.contains_key(key) {
            true => format!("Key \"{:?}\" already existed", key),
            false => {
                if let Ok(r) = conn_info.to_conn().await {
                    self.store.insert(key.to_owned(), r);
                    return format!("New conn {:?} succeeded", &key);
                }
                format!("Failed to create connection")
            }
        }
    }

    /// update an existing connection pool
    pub async fn update_conn(&mut self, key: &str, conn_info: ConnInfo) -> String {
        match self.store.contains_key(key) {
            true => {
                if let Ok(r) = conn_info.to_conn().await {
                    self.store.insert(key.to_owned(), r);
                    return format!("Update conn {:?} succeeded", &key);
                }
                format!("Failed to update connection")
            }
            false => format!("Key \"{:?}\" does not exist", key),
        }
    }
}
