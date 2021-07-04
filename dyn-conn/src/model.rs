//!

use std::{collections::HashMap, fmt::Display};

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use sqlx::{Connection, MySqlConnection, PgConnection};

// database identifier
#[derive(Deserialize, Serialize, Clone, Copy, Debug)]
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

/// storing database connection string
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

    // convert to database connection string uri
    pub fn to_string(&self) -> String {
        format!(
            "{}://{}:{}@{}:{}/{}",
            self.driver, self.username, self.password, self.host, self.port, self.database,
        )
    }
}

/// trait for user implement
/// database connection pool with business logic
#[async_trait]
pub trait BizPoolFunctionality {
    async fn disconnect(&self);
}

/// trait for user implement
/// ConnInfo establish real database connection pool
#[async_trait]
pub trait ConnInfoFunctionality<T: BizPoolFunctionality> {
    type ErrorType;
    async fn conn_establish(conn_info: ConnInfo) -> Result<ConnMember<T>, Self::ErrorType>;
}

/// Conn struct contains a database connection info, and a connection pool's instance
pub struct ConnMember<T: BizPoolFunctionality> {
    pub info: ConnInfo,
    pub biz_pool: T,
}

/// DynConn's responses
#[derive(Serialize)]
#[serde(untagged)]
pub enum ConnStoreResponses {
    String(String),
    Map(HashMap<String, String>),
}

impl ConnStoreResponses {
    pub fn json(&self) -> serde_json::Value {
        serde_json::json!(self)
    }

    pub fn json_string(&self) -> String {
        self.json().to_string()
    }
}

/// DynConn's error
#[derive(Serialize)]
#[serde(untagged)]
pub enum ConnStoreError {
    ConnNotFound(String),
    ConnAlreadyExists(String),
    ConnFailed(String),
}

impl ConnStoreError {
    pub fn json(&self) -> serde_json::Value {
        serde_json::json!(self)
    }

    pub fn json_string(&self) -> String {
        self.json().to_string()
    }
}

pub type ConnStoreResult = Result<ConnStoreResponses, ConnStoreError>;

// TODO: Migration, considering make persistence migration runtime

/// persists ConnStore
#[async_trait]
pub trait PersistenceFunctionality {
    /// load a ConnInfo from DB
    async fn load(&self, key: &str) -> Result<ConnInfo, ConnStoreError>;
    /// load all ConnInfo from DB
    async fn load_all(&self) -> Result<HashMap<String, ConnInfo>, ConnStoreError>;
    /// save a ConnInfo to DB
    async fn save(&self, key: &str, conn: &ConnInfo) -> Result<ConnInfo, ConnStoreError>;
    /// update a ConnInfo to DB
    async fn update(&self, key: &str, conn: &ConnInfo) -> Result<ConnInfo, ConnStoreError>;
    /// delete a ConnInfo from DB
    async fn delete(&self, key: &str) -> Result<ConnInfo, ConnStoreError>;
}

/// using hash map to maintain multiple Conn structs
pub struct ConnStore<T>
where
    T: BizPoolFunctionality,
    T: ConnInfoFunctionality<T>,
{
    pub store: HashMap<String, ConnMember<T>>,
    persistence: Option<Box<dyn PersistenceFunctionality + Send>>,
}

/// main struct of dyn-conn crate
/// handling CRUD memory's database connection pools with custom business logics.
impl<T> ConnStore<T>
where
    T: BizPoolFunctionality,
    T: ConnInfoFunctionality<T>,
{
    pub fn new() -> Self {
        ConnStore {
            store: HashMap::<String, ConnMember<T>>::new(),
            persistence: None,
        }
    }

    /// only works if persistence is None and only works once
    pub async fn attach_persistence(
        &mut self,
        p: Box<dyn PersistenceFunctionality + Send>,
    ) -> ConnStoreResult {
        if let None = &self.persistence {
            let persisted_data = &p.load_all().await?;
            self.persistence = Some(p);

            let mut tmp = HashMap::<String, ConnMember<T>>::new();

            for (key, conn_info) in persisted_data.iter() {
                match T::conn_establish(conn_info.clone()).await {
                    Ok(ci) => {
                        tmp.insert(key.to_owned(), ci);
                    }
                    Err(_) => return Err(ConnStoreError::ConnFailed(conn_info.to_string())),
                }
            }

            self.store = tmp;

            return Ok(ConnStoreResponses::String(
                "attach persistence succeeded!".to_owned(),
            ));
        }
        Err(ConnStoreError::ConnFailed(
            "attach persistence failed".to_owned(),
        ))
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

    /// show dynamic connection's keys
    pub fn show_keys(&self) -> Vec<String> {
        self.store.keys().map(|f| f.to_owned()).collect()
    }

    /// show all database connection string
    pub fn show_info(&self) -> ConnStoreResult {
        let res = self
            .store
            .iter()
            .map(|(k, v)| (k.clone(), v.info.to_string()))
            .collect();
        Ok(ConnStoreResponses::Map(res))
    }

    /// get an existing connection pool
    pub fn get_conn(&self, key: &str) -> Option<&ConnMember<T>> {
        self.store.get(key)
    }

    /// delete an existing connection pool
    pub async fn delete_conn(&mut self, key: &str) -> ConnStoreResult {
        match self.store.contains_key(key) {
            true => {
                let s = self.store.get(key).unwrap();
                s.biz_pool.disconnect().await;
                if let Some(p) = &self.persistence {
                    p.delete(key).await?;
                }
                Ok(ConnStoreResponses::String(format!(
                    "Disconnected from {:?}",
                    key
                )))
            }
            false => Err(ConnStoreError::ConnNotFound(key.to_owned())),
        }
    }

    /// create a new connection pool and save in memory
    pub async fn create_conn(&mut self, key: &str, conn_info: ConnInfo) -> ConnStoreResult {
        match self.store.contains_key(key) {
            true => Err(ConnStoreError::ConnAlreadyExists(key.to_owned())),
            false => {
                if let Ok(r) = T::conn_establish(conn_info.clone()).await {
                    self.store.insert(key.to_owned(), r);
                    if let Some(p) = &self.persistence {
                        p.save(key, &conn_info).await?;
                    }
                    return Ok(ConnStoreResponses::String(format!(
                        "New conn {:?} succeeded",
                        &key
                    )));
                }
                Err(ConnStoreError::ConnFailed(conn_info.to_string()))
            }
        }
    }

    /// update an existing connection pool
    pub async fn update_conn(&mut self, key: &str, conn_info: ConnInfo) -> ConnStoreResult {
        match self.store.contains_key(key) {
            true => {
                if let Ok(r) = T::conn_establish(conn_info.clone()).await {
                    self.store.get(key).unwrap().biz_pool.disconnect().await;
                    self.store.insert(key.to_owned(), r);
                    if let Some(p) = &self.persistence {
                        p.update(key, &conn_info).await?;
                    }
                    return Ok(ConnStoreResponses::String(format!(
                        "New conn {:?} succeeded",
                        &key
                    )));
                }
                Err(ConnStoreError::ConnFailed(conn_info.to_string()))
            }
            false => Err(ConnStoreError::ConnNotFound(key.to_owned())),
        }
    }
}
