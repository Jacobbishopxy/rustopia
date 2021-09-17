//！Db: tiny-df database interaction module
//！
//！data conversion

pub mod engine;
pub mod errors;
pub mod loader;
pub mod types;

pub use errors::{TdDbError, TdDbResult};
pub use loader::Loader;

use crate::se::Sql;

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

impl std::fmt::Display for ConnInfo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}://{}:{}@{}:{}/{}",
            self.driver, self.username, self.password, self.host, self.port, self.database,
        )
    }
}
