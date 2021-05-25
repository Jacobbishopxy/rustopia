use sqlx::{
    postgres::{PgPoolOptions, PgQueryResult},
    Error, Pool, Postgres,
};

use crate::adaptors::sea;

use ua_model::*;

#[derive(Clone)]
pub struct Dao {
    pool: Pool<Postgres>,
}

// todo: 1. generic trait; 2. custom error
impl Dao {
    pub async fn new(uri: &String, max_connections: u32) -> Self {
        let pool = PgPoolOptions::new()
            .max_connections(max_connections)
            .connect(uri)
            .await
            .expect("Connection success!");

        Dao { pool }
    }

    pub async fn create_table(
        &self,
        table: TableCreate,
        create_if_not_exists: bool,
    ) -> Result<PgQueryResult, Error> {
        let query = sea::create_table(&table, create_if_not_exists);
        sqlx::query(&query).execute(&self.pool).await
    }

    pub async fn alter_table(&self, table: TableAlter) -> Result<PgQueryResult, Error> {
        let vec_query = sea::alter_table(&table);

        let mut tx = self.pool.begin().await.expect("Transaction start");

        for query in &vec_query {
            if let e @ Err(_) = sqlx::query(query).execute(&mut tx).await {
                return e;
            }
        }

        match tx.commit().await {
            Ok(_) => Ok(PgQueryResult::default()),
            Err(_) => Err(Error::WorkerCrashed),
        }
    }

    pub async fn drop_table(&self, table: TableDrop) -> Result<PgQueryResult, Error> {
        let query = sea::drop_table(&table);
        sqlx::query(&query).execute(&self.pool).await
    }

    pub async fn rename_table(&self, table: TableRename) -> Result<PgQueryResult, Error> {
        let query = sea::rename_table(&table);
        sqlx::query(&query).execute(&self.pool).await
    }

    pub async fn truncate_table(&self, table: TableTruncate) -> Result<PgQueryResult, Error> {
        let query = sea::truncate_table(&table);
        sqlx::query(&query).execute(&self.pool).await
    }
}
