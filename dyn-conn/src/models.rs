//!

use serde::{Deserialize, Serialize};
use std::{collections::HashMap, fmt::Display};

// use sqlx::any::AnyPoolOptions;

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

#[derive(Deserialize, Serialize)]
pub struct DynConn {
    pub store: HashMap<String, Conn>,
}

impl DynConn {
    pub fn new() -> DynConn {
        DynConn {
            store: HashMap::new(),
        }
    }

    pub fn show_keys(&self) -> Vec<String> {
        self.store.keys().map(|f| f.to_owned()).collect()
    }

    pub fn show_store(&self) -> HashMap<String, String> {
        self.store
            .iter()
            .map(|(k, v)| (k.clone(), v.to_string()))
            .collect()
    }

    pub fn new_conn(&mut self, key: String, conn: Conn) -> String {
        match self.store.contains_key(&key) {
            true => format!("Key \"{:?}\" already existed", key),
            false => {
                self.store.insert(key.clone(), conn);
                format!("New conn {:?} success", &key)
            }
        }
    }
}
