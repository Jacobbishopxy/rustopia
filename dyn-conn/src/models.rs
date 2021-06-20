//!

use std::{collections::HashMap, fmt::Display};

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use sqlx::mysql::{MySql, MySqlPoolOptions};
use sqlx::postgres::{PgPoolOptions, Postgres};
use sqlx::{Connection, MySqlConnection, PgConnection, Pool};

#[async_trait]
pub trait DynPoolFunctionality {
    async fn disconnect(&self);
}

/// dynamic pool options
pub enum DynPoolOptions {
    Mysql(Pool<MySql>),
    Postgres(Pool<Postgres>),
}

#[async_trait]
impl DynPoolFunctionality for DynPoolOptions {
    async fn disconnect(&self) {
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

#[derive(Deserialize, Serialize, Clone, Copy)]
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
pub struct ConnInfo {
    pub driver: Driver,
    pub username: String,
    pub password: String,
    pub host: String,
    pub port: i32,
    pub database: String,
}

impl Clone for ConnInfo {
    fn clone(&self) -> Self {
        ConnInfo {
            driver: self.driver,
            username: self.username.clone(),
            password: self.password.clone(),
            host: self.host.clone(),
            port: self.port,
            database: self.database.clone(),
        }
    }
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
}

/// convert connection info to Conn struct
async fn conn_establish(conn_info: ConnInfo) -> Result<Conn<DynPoolOptions>, sqlx::Error> {
    let uri = &conn_info.to_string();

    match conn_info.driver {
        Driver::Postgres => {
            let pool = PgPoolOptions::new()
                .max_connections(10)
                .connect(uri)
                .await?;
            let pool = DynPoolOptions::Postgres(pool);
            Ok(Conn {
                info: conn_info.clone(),
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
                info: conn_info.clone(),
                pool,
            })
        }
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

/// Conn struct contains a database connection info, and a connection pool's instance
pub struct Conn<T: DynPoolFunctionality> {
    pub info: ConnInfo,
    pub pool: T,
}

/// using hash map to maintain multiple Conn structs
pub struct DynConn<T: DynPoolFunctionality> {
    pub store: HashMap<String, Conn<T>>,
}

impl<T: DynPoolFunctionality> DynConn<T> {
    pub fn new() -> Self {
        DynConn {
            store: HashMap::<String, Conn<T>>::new(),
        }
    }

    // giving a key, check if it's in store
    pub fn check_key(&self, key: &str) -> bool {
        self.store.contains_key(key)
    }

    pub fn get_conn(&self, key: &str) -> Option<&Conn<T>> {
        self.store.get(key)
    }

    /// show dynamic connection's keys
    pub fn show_keys(&self) -> Vec<String> {
        self.store.keys().map(|f| f.to_owned()).collect()
    }
}

#[async_trait]
pub trait DynConnFunctionality<T: DynPoolFunctionality> {
    type Out;

    fn show_info(&self) -> Self::Out;

    async fn delete_conn(&mut self, key: &str) -> Self::Out;

    async fn create_conn(&mut self, key: &str, conn_info: ConnInfo) -> Self::Out;

    async fn update_conn(&mut self, key: &str, conn_info: ConnInfo) -> Self::Out;
}

#[derive(Serialize)]
#[serde(untagged)]
pub enum DynPoolOptionsOut {
    Simple(String),
    SimpleMap(HashMap<String, String>),
}

#[async_trait]
impl DynConnFunctionality<DynPoolOptions> for DynConn<DynPoolOptions> {
    type Out = DynPoolOptionsOut;

    /// show store, value as converted string
    fn show_info(&self) -> Self::Out {
        let res = self
            .store
            .iter()
            .map(|(k, v)| (k.clone(), v.info.to_string()))
            .collect();

        DynPoolOptionsOut::SimpleMap(res)
    }

    /// drop an existing connection pool
    async fn delete_conn(&mut self, key: &str) -> Self::Out {
        match self.store.contains_key(key) {
            true => {
                self.store.get(key).unwrap().pool.disconnect().await;
                DynPoolOptionsOut::Simple(format!("Disconnected from {:?}", key))
            }
            false => DynPoolOptionsOut::Simple(format!("Key \"{:?}\" does not exist", key)),
        }
    }

    /// create new connection pool and store it in memory
    async fn create_conn(&mut self, key: &str, conn_info: ConnInfo) -> Self::Out {
        match self.store.contains_key(key) {
            true => DynPoolOptionsOut::Simple(format!("Key \"{:?}\" already existed", key)),
            false => {
                if let Ok(r) = conn_establish(conn_info).await {
                    self.store.insert(key.to_owned(), r);
                    return DynPoolOptionsOut::Simple(format!("New conn {:?} succeeded", &key));
                }
                DynPoolOptionsOut::Simple(format!("Failed to create connection"))
            }
        }
    }

    /// update an existing connection pool
    async fn update_conn(&mut self, key: &str, conn_info: ConnInfo) -> Self::Out {
        match self.store.contains_key(key) {
            true => {
                if let Ok(r) = conn_establish(conn_info).await {
                    self.store.insert(key.to_owned(), r);
                    return DynPoolOptionsOut::Simple(format!("Update conn {:?} succeeded", &key));
                }
                DynPoolOptionsOut::Simple(format!("Failed to update connection"))
            }
            false => DynPoolOptionsOut::Simple(format!("Key \"{:?}\" does not exist", key)),
        }
    }
}
