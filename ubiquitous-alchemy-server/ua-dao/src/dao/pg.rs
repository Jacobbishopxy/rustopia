use async_trait::async_trait;
use sqlx::postgres::PgRow;
use sqlx::Row;
use sqlx::{postgres::PgQueryResult, Postgres};

use crate::dao::Dao;
use crate::interface::UaSchema;
use crate::provider::sea;

use crate::error::DaoError as Error;
use ua_model::*;

#[async_trait]
impl UaSchema for Dao<Postgres> {
    type Out = PgQueryResult;
    type Res = Box<dyn QueryResult>;

    async fn execute(&self, str: &String) -> Result<Self::Out, Error> {
        sqlx::query(str)
            .execute(&self.pool)
            .await
            .map_err(|e| Error::from(e))
    }

    async fn list_table(&self) -> Result<Self::Res, Error> {
        let query = sea::list_table();
        let foo = sqlx::query(&query)
            .map(|row: PgRow| TableSimpleList {
                name: row.get_unchecked("name"),
            })
            .fetch_all(&self.pool)
            .await;

        match foo {
            Ok(r) => Ok(Box::new(r)),
            Err(e) => Err(Error::from(e)),
        }
    }

    async fn create_table(
        &self,
        table: TableCreate,
        create_if_not_exists: bool,
    ) -> Result<PgQueryResult, Error> {
        let query = sea::create_table(&table, create_if_not_exists);
        self.execute(&query).await
    }

    async fn alter_table(&self, table: TableAlter) -> Result<PgQueryResult, Error> {
        let vec_query = sea::alter_table(&table);

        let mut tx = match self.pool.begin().await {
            Ok(t) => t,
            Err(e) => {
                return Err(Error::from(e));
            }
        };

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
        self.execute(&query).await
    }

    async fn rename_table(&self, table: TableRename) -> Result<PgQueryResult, Error> {
        let query = sea::rename_table(&table);
        self.execute(&query).await
    }

    async fn truncate_table(&self, table: TableTruncate) -> Result<PgQueryResult, Error> {
        let query = sea::truncate_table(&table);
        self.execute(&query).await
    }

    async fn create_index(&self, index: IndexCreate) -> Result<Self::Out, Error> {
        let query = sea::create_index(&index);
        self.execute(&query).await
    }

    async fn drop_index(&self, index: IndexDrop) -> Result<Self::Out, Error> {
        let query = sea::drop_index(&index);
        self.execute(&query).await
    }
}
