pub mod my;
pub mod pg;

use sqlx::{mysql::MySqlPoolOptions, postgres::PgPoolOptions, Database, MySql, Pool, Postgres};

pub use crate::interface::UaQuery;
pub use crate::interface::UaSchema;

pub type DaoPG = Dao<Postgres>;
pub type DaoMY = Dao<MySql>;

pub struct Dao<T: Database> {
    pub pool: Pool<T>,
}

impl<T: Database> Clone for Dao<T> {
    fn clone(&self) -> Self {
        Dao {
            pool: self.pool.clone(),
        }
    }
}

impl Dao<Postgres> {
    pub async fn new(uri: &str, max_connections: u32) -> Self {
        let pool = PgPoolOptions::new()
            .max_connections(max_connections)
            .connect(uri)
            .await
            .unwrap();

        Dao { pool }
    }
}

impl Dao<MySql> {
    pub async fn new(uri: &str, max_connections: u32) -> Self {
        let pool = MySqlPoolOptions::new()
            .max_connections(max_connections)
            .connect(uri)
            .await
            .unwrap();

        Dao { pool }
    }
}
