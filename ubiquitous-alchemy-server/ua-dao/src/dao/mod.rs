pub mod my;
pub mod pg;

use sqlx::{
    mysql::MySqlPoolOptions, postgres::PgPoolOptions, Database, Error, MySql, MySqlPool, PgPool,
    Pool, Postgres,
};

pub use crate::interface::UaSchema;

pub type DaoPG = Dao<Postgres>;

pub type DaoMY = Dao<MySql>;

#[derive(Clone)]
pub struct Dao<T: Database> {
    pub pool: Pool<T>,
}

impl Dao<Postgres> {
    pub async fn new(uri: &String, max_connections: u32) -> Result<PgPool, Error> {
        PgPoolOptions::new()
            .max_connections(max_connections)
            .connect(uri)
            .await
    }
}

impl Dao<MySql> {
    pub async fn new(uri: &String, max_connections: u32) -> Result<MySqlPool, Error> {
        MySqlPoolOptions::new()
            .max_connections(max_connections)
            .connect(uri)
            .await
    }
}
