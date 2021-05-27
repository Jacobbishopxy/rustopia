use async_trait::async_trait;
use sqlx::{postgres::PgQueryResult, Postgres};

use crate::dao::Dao;
use crate::interface::UaSchema;
use crate::provider::sea;

use crate::error::DaoError as Error;
use ua_model::*;

#[async_trait]
impl UaSchema for Dao<Postgres> {
    type Out = PgQueryResult;

    async fn create_table(
        &self,
        table: TableCreate,
        create_if_not_exists: bool,
    ) -> Result<PgQueryResult, Error> {
        let query = sea::create_table(&table, create_if_not_exists);
        sqlx::query(&query)
            .execute(&self.pool)
            .await
            .map_err(|e| Error::from(e))
    }

    async fn alter_table(&self, table: TableAlter) -> Result<PgQueryResult, Error> {
        let vec_query = sea::alter_table(&table);

        let mut tx = self.pool.begin().await.expect("Transaction start");

        for query in &vec_query {
            if let Err(e) = sqlx::query(query).execute(&mut tx).await {
                return Err(Error::from(e));
            }
        }

        match tx.commit().await {
            Ok(_) => Ok(PgQueryResult::default()),
            Err(e) => Err(Error::from(e)),
        }
    }

    async fn drop_table(&self, table: TableDrop) -> Result<PgQueryResult, Error> {
        let query = sea::drop_table(&table);
        sqlx::query(&query)
            .execute(&self.pool)
            .await
            .map_err(|e| Error::from(e))
    }

    async fn rename_table(&self, table: TableRename) -> Result<PgQueryResult, Error> {
        let query = sea::rename_table(&table);
        sqlx::query(&query)
            .execute(&self.pool)
            .await
            .map_err(|e| Error::from(e))
    }

    async fn truncate_table(&self, table: TableTruncate) -> Result<PgQueryResult, Error> {
        let query = sea::truncate_table(&table);
        sqlx::query(&query)
            .execute(&self.pool)
            .await
            .map_err(|e| Error::from(e))
    }
}
