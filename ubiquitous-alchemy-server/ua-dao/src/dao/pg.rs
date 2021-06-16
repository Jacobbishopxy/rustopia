//!

use async_trait::async_trait;
use sqlx::{postgres::PgRow, Postgres, Row};

use crate::dao::Dao;
use crate::interface::{UaQuery, UaSchema};
use crate::provider::sea::{Builder, BuilderType};
use crate::util::type_conversion_pg::row_to_map;
use crate::util::{DataEnum, DbQueryResult, QueryResult};
use crate::DaoError as Error;
use ua_model::*;

const PG_BUILDER: Builder = Builder(BuilderType::PG);

#[async_trait]
impl UaSchema for Dao<Postgres> {
    type Out = Box<dyn QueryResult>;

    async fn execute(&self, str: &str) -> Result<Self::Out, Error> {
        let res = sqlx::query(str).execute(&self.pool).await;

        match res {
            Ok(r) => Ok(Box::new(DbQueryResult {
                rows_affected: r.rows_affected(),
                last_insert_id: None,
            })),
            Err(_) => todo!(),
        }
    }

    async fn list_table(&self) -> Result<Self::Out, Error> {
        let query = PG_BUILDER.list_table();
        let res = sqlx::query(&query)
            .map(|row: PgRow| -> String { row.get(0) })
            .fetch_all(&self.pool)
            .await;

        match res {
            Ok(r) => Ok(Box::new(r)),
            Err(e) => Err(Error::from(e)),
        }
    }

    async fn create_table(
        &self,
        table: TableCreate,
        create_if_not_exists: bool,
    ) -> Result<Self::Out, Error> {
        let query = PG_BUILDER.create_table(&table, create_if_not_exists);
        self.execute(&query).await
    }

    async fn alter_table(&self, table: &TableAlter) -> Result<Self::Out, Error> {
        let vec_query = PG_BUILDER.alter_table(table);

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
            Ok(_) => Ok(Box::new(DataEnum::Null)),
            Err(e) => Err(Error::from(e)),
        }
    }

    async fn drop_table(&self, table: &TableDrop) -> Result<Self::Out, Error> {
        let query = PG_BUILDER.drop_table(table);
        self.execute(&query).await
    }

    async fn rename_table(&self, table: &TableRename) -> Result<Self::Out, Error> {
        let query = PG_BUILDER.rename_table(table);
        self.execute(&query).await
    }

    async fn truncate_table(&self, table: &TableTruncate) -> Result<Self::Out, Error> {
        let query = PG_BUILDER.truncate_table(table);
        self.execute(&query).await
    }

    async fn create_index(&self, index: &IndexCreate) -> Result<Self::Out, Error> {
        let query = PG_BUILDER.create_index(index);
        self.execute(&query).await
    }

    async fn drop_index(&self, index: &IndexDrop) -> Result<Self::Out, Error> {
        let query = PG_BUILDER.drop_index(index);
        self.execute(&query).await
    }

    async fn create_foreign_key(&self, key: &ForeignKeyCreate) -> Result<Self::Out, Error> {
        let query = PG_BUILDER.create_foreign_key(key);
        self.execute(&query).await
    }

    async fn drop_foreign_key(&self, key: &ForeignKeyDrop) -> Result<Self::Out, Error> {
        let query = PG_BUILDER.drop_foreign_key(&key);
        self.execute(&query).await
    }
}

#[async_trait]
impl UaQuery for Dao<Postgres> {
    type Out = Box<dyn QueryResult>;

    async fn select(&self, select: &Select) -> Result<Self::Out, Error> {
        let query = PG_BUILDER.select_table(select);

        let res = sqlx::query(&query)
            .try_map(|row: PgRow| row_to_map(row, &select.columns))
            .fetch_all(&self.pool)
            .await;

        match res {
            Ok(r) => Ok(Box::new(r)),
            Err(e) => Err(Error::from(e)),
        }
    }
}
