use async_trait::async_trait;
use sqlx::{mysql::MySqlPoolOptions, postgres::PgPoolOptions, Database, MySql, Pool, Postgres};

use ua_model::*;

/// Empty trait for tagging custom type
pub trait DbType {}

impl DbType for Postgres {}

impl DbType for MySql {}

pub struct Dao<T>
where
    T: Database + DbType,
{
    pool: Pool<T>,
}

#[async_trait]
pub trait UaDao<T: Database + DbType> {
    async fn new(uri: &String, max: u32) -> Dao<T>;

    // async fn create_table(
    //     &self,
    //     table: TableCreate,
    //     create_if_not_exists: bool,
    // ) -> Result<MyResult, MyError>;
}

#[async_trait]
impl UaDao<Postgres> for Dao<Postgres> {
    async fn new(uri: &String, max: u32) -> Dao<Postgres> {
        let pool = PgPoolOptions::new()
            .max_connections(max)
            .connect(uri)
            .await
            .expect("Connection success!");

        Dao { pool }
    }
}

#[async_trait]
impl UaDao<MySql> for Dao<MySql> {
    async fn new(uri: &String, max: u32) -> Dao<MySql> {
        let pool = MySqlPoolOptions::new()
            .max_connections(max)
            .connect(uri)
            .await
            .expect("Connection success!");

        Dao { pool }
    }
}
