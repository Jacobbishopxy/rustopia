use std::{collections::HashMap, fmt::Display};

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// database identifier
#[derive(Deserialize, Serialize, Clone, Copy, Debug, PartialEq, Eq)]
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

/// convert to database connection string uri
impl Display for ConnInfo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}://{}:{}@{}:{}/{}",
            self.driver, self.username, self.password, self.host, self.port, self.database,
        )
    }
}

/// trait for user implement
/// custom connection information
pub trait ConnInfoFunctionality {
    fn to_conn_info(&self) -> ConnInfo;
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
pub trait ConnGeneratorFunctionality<R: ConnInfoFunctionality + Clone, B: BizPoolFunctionality> {
    type ErrorType;
    async fn check_connection(conn_info: &ConnInfo) -> Result<bool, Self::ErrorType>;
    async fn conn_establish(conn_info: &ConnInfo) -> Result<ConnMember<R, B>, Self::ErrorType>;
}

/// Conn struct contains a database connection info, and a connection pool's instance
pub struct ConnMember<R: ConnInfoFunctionality + Clone, B: BizPoolFunctionality> {
    pub info: R,
    pub biz_pool: B,
}

/// DynConn's responses
#[derive(Serialize)]
#[serde(untagged)]
pub enum ConnStoreResponses<R: ConnInfoFunctionality + Serialize> {
    Bool(bool),
    String(String),
    Map(HashMap<String, String>),
    Conn(R),
    ConnVec(Vec<R>),
}

impl<R: ConnInfoFunctionality + Serialize> ConnStoreResponses<R> {
    pub fn json(&self) -> serde_json::Value {
        serde_json::json!(self)
    }

    pub fn json_string(&self) -> String {
        self.json().to_string()
    }
}

/// DynConn's error
#[derive(Serialize, Debug)]
#[serde(untagged)]
pub enum ConnStoreError {
    Exception(String),
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

pub type ConnStoreResult<R> = Result<ConnStoreResponses<R>, ConnStoreError>;

/// persists ConnStore
#[async_trait]
pub trait PersistenceFunctionality<R: ConnInfoFunctionality> {
    /// load all ConnInfo from DB
    async fn load_all(&self) -> Result<HashMap<Uuid, R>, ConnStoreError>;
    /// save a ConnInfo to DB
    async fn save(&self, key: &Uuid, conn: &R) -> Result<(), ConnStoreError>; // TODO: return R
    /// update a ConnInfo to DB
    async fn update(&self, key: &Uuid, conn: &R) -> Result<(), ConnStoreError>;
    /// delete a ConnInfo from DB
    async fn delete(&self, key: &Uuid) -> Result<(), ConnStoreError>;
}

/// using hash map to maintain multiple Conn structs
pub struct ConnStore<R, B>
where
    R: ConnInfoFunctionality + Clone + Send,
    B: BizPoolFunctionality + Send,
    B: ConnGeneratorFunctionality<R, B>,
{
    pub store: HashMap<Uuid, ConnMember<R, B>>,
    persistence: Option<Box<dyn PersistenceFunctionality<R> + Send>>,
}

/// main struct of dyn-conn crate
/// handling CRUD memory's database connection pools with custom business logics.
impl<R, B> ConnStore<R, B>
where
    R: ConnInfoFunctionality + Clone + Serialize + Send,
    B: BizPoolFunctionality + Send,
    B: ConnGeneratorFunctionality<R, B>,
{
    pub fn new() -> Self {
        ConnStore {
            store: HashMap::<Uuid, ConnMember<R, B>>::new(),
            persistence: None,
        }
    }

    /// only works if persistence is None and only works once
    pub async fn attach_persistence(
        &mut self,
        p: Box<dyn PersistenceFunctionality<R> + Send>,
    ) -> ConnStoreResult<R> {
        if let None = &self.persistence {
            let persisted_data = &p.load_all().await?;
            self.persistence = Some(p);

            self.store = HashMap::<Uuid, ConnMember<R, B>>::new();

            let mut errors = Vec::new();

            for (key, conn_info) in persisted_data.iter() {
                match B::conn_establish(&conn_info.to_conn_info()).await {
                    Ok(ci) => {
                        self.store.insert(key.to_owned(), ci);
                    }
                    Err(_) => {
                        // let the rest of connections establish, accumulates errors
                        errors.push(conn_info.to_conn_info().to_string());
                    }
                }
            }

            if errors.len() > 0 {
                return Err(ConnStoreError::ConnFailed(errors.join(", ")));
            }

            return Ok(ConnStoreResponses::String(
                "attach persistence succeeded!".to_owned(),
            ));
        }
        Err(ConnStoreError::ConnFailed(
            "attach persistence failed".to_owned(),
        ))
    }

    /// check whether database connection string is available
    pub async fn check_connection(&self, conn_info: &R) -> ConnStoreResult<R> {
        match B::check_connection(&conn_info.to_conn_info()).await {
            Ok(res) => Ok(ConnStoreResponses::Bool(res)),
            Err(_) => Err(ConnStoreError::ConnFailed(
                conn_info.to_conn_info().to_string(),
            )),
        }
    }

    /// giving a key, check if it's in store
    pub fn check_key(&self, key: &Uuid) -> bool {
        self.store.contains_key(key)
    }

    /// show dynamic connection's keys
    pub fn show_keys(&self) -> Vec<String> {
        self.store.keys().map(|f| f.to_string()).collect()
    }

    /// show all database connection string
    pub fn show_info(&self) -> ConnStoreResult<R> {
        let res = self
            .store
            .iter()
            .map(|(k, v)| (k.to_string(), v.info.to_conn_info().to_string()))
            .collect();
        Ok(ConnStoreResponses::Map(res))
    }

    /// get an existing connection pool
    pub fn get_conn(&self, key: &Uuid) -> Result<&ConnMember<R, B>, ConnStoreError> {
        match self.store.get(key) {
            Some(c) => Ok(c),
            None => Err(ConnStoreError::ConnNotFound(key.to_string())),
        }
    }

    /// get all database connection
    pub async fn list_conn(&mut self) -> ConnStoreResult<R> {
        let map = match &self.persistence {
            Some(p) => p.load_all().await?,
            None => self
                .store
                .iter()
                .map(|(k, v)| (k.clone(), v.info.clone()))
                .collect::<HashMap<Uuid, R>>(),
        };

        let res = map.values().map(|c| c.clone()).collect::<Vec<R>>();

        Ok(ConnStoreResponses::ConnVec(res))
    }

    /// create a new connection pool and save in memory
    pub async fn create_conn(&mut self, conn_info: &R) -> ConnStoreResult<R> {
        let key = Uuid::new_v4();
        if let Ok(r) = B::conn_establish(&conn_info.to_conn_info()).await {
            self.store.insert(key.to_owned(), r);
            if let Some(p) = &self.persistence {
                p.save(&key, &conn_info).await?;
            }
            return Ok(ConnStoreResponses::String(format!(
                "New conn {:?} succeeded",
                &key
            )));
        }
        Err(ConnStoreError::ConnFailed(
            conn_info.to_conn_info().to_string(),
        ))
    }

    /// update an existing connection pool
    pub async fn update_conn(&mut self, key: &Uuid, conn_info: &R) -> ConnStoreResult<R> {
        match self.store.contains_key(key) {
            true => {
                if let Ok(r) = B::conn_establish(&conn_info.to_conn_info()).await {
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
                Err(ConnStoreError::ConnFailed(
                    conn_info.to_conn_info().to_string(),
                ))
            }
            false => Err(ConnStoreError::ConnNotFound(key.to_string())),
        }
    }

    /// delete an existing connection pool
    pub async fn delete_conn(&mut self, key: &Uuid) -> ConnStoreResult<R> {
        match self.store.contains_key(key) {
            true => {
                let s = self.store.get(key).unwrap();
                s.biz_pool.disconnect().await;
                if let Some(p) = &self.persistence {
                    p.delete(&key).await?;
                }
                self.store.remove(key);
                Ok(ConnStoreResponses::String(format!(
                    "Disconnected from {:?}",
                    key
                )))
            }
            false => Err(ConnStoreError::ConnNotFound(key.to_string())),
        }
    }
}

/// utility functions
pub struct ConnUtil;

impl ConnUtil {
    /// turn string to a Uuid
    pub fn str_to_uuid(s: &str) -> Result<Uuid, ConnStoreError> {
        match Uuid::parse_str(s) {
            Ok(res) => Ok(res),
            Err(_) => Err(ConnStoreError::Exception("Uuid parsing error".to_owned())),
        }
    }

    /// turn Uuid to a string
    pub fn uuid_to_str(u: &Uuid) -> Result<String, ConnStoreError> {
        Ok(u.to_string())
    }
}
