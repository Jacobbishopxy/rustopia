//!

use std::collections::HashMap;

use async_trait::async_trait;
use sqlx::postgres::PgRow;
use sqlx::Column;
use sqlx::Row;
use sqlx::{postgres::PgQueryResult, Postgres};

use crate::dao::Dao;
use crate::interface::{UaQuery, UaSchema};
use crate::provider::sea::{Builder, BuilderType};
use crate::type_info::QueryResult;

use crate::error::DaoError as Error;
use crate::type_info::general::DataEnum;
use ua_model::*;

const PG_BUILDER: Builder = Builder(BuilderType::PG);

#[async_trait]
impl UaSchema for Dao<Postgres> {
    type Out = PgQueryResult;
    type Res = Box<dyn QueryResult>;

    async fn execute(&self, str: &str) -> Result<Self::Out, Error> {
        sqlx::query(str)
            .execute(&self.pool)
            .await
            .map_err(|e| Error::from(e))
    }

    async fn list_table(&self) -> Result<Self::Res, Error> {
        let query = PG_BUILDER.list_table();
        let res = sqlx::query(&query)
            .map(|row: PgRow| {
                let foo: String = row.get("table_name");
                foo
            })
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
    ) -> Result<PgQueryResult, Error> {
        let query = PG_BUILDER.create_table(&table, create_if_not_exists);
        self.execute(&query).await
    }

    async fn alter_table(&self, table: TableAlter) -> Result<PgQueryResult, Error> {
        let vec_query = PG_BUILDER.alter_table(&table);

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
        let query = PG_BUILDER.drop_table(&table);
        self.execute(&query).await
    }

    async fn rename_table(&self, table: TableRename) -> Result<PgQueryResult, Error> {
        let query = PG_BUILDER.rename_table(&table);
        self.execute(&query).await
    }

    async fn truncate_table(&self, table: TableTruncate) -> Result<PgQueryResult, Error> {
        let query = PG_BUILDER.truncate_table(&table);
        self.execute(&query).await
    }

    async fn create_index(&self, index: IndexCreate) -> Result<Self::Out, Error> {
        let query = PG_BUILDER.create_index(&index);
        self.execute(&query).await
    }

    async fn drop_index(&self, index: IndexDrop) -> Result<Self::Out, Error> {
        let query = PG_BUILDER.drop_index(&index);
        self.execute(&query).await
    }

    async fn create_foreign_key(&self, key: ForeignKeyCreate) -> Result<Self::Out, Error> {
        let query = PG_BUILDER.create_foreign_key(&key);
        self.execute(&query).await
    }

    async fn drop_foreign_key(&self, key: ForeignKeyDrop) -> Result<Self::Out, Error> {
        let query = PG_BUILDER.drop_foreign_key(&key);
        self.execute(&query).await
    }
}

#[async_trait]
impl UaQuery for Dao<Postgres> {
    type Res = Box<dyn QueryResult>;

    async fn select(&self, select: Select) -> Result<Self::Res, Error> {
        let query = PG_BUILDER.select_table(&select);

        let res = sqlx::query(&query)
            .map(|row: PgRow| {
                let mut r = HashMap::new();
                for (i, k) in select.columns.iter().enumerate() {
                    let foo = row.column(i).type_info();

                    // todo
                    match foo.to_string() {
                        f if f == "VARCHAR" => {
                            r.insert(k.to_owned(), DataEnum::String(row.get(i)));
                        }
                        f if f == "FLOAT8" => {
                            r.insert(k.to_owned(), DataEnum::Float(row.get(i)));
                        }
                        _ => {}
                    }
                }
                r
            })
            .fetch_all(&self.pool)
            .await;

        match res {
            Ok(r) => Ok(Box::new(r)),
            Err(e) => Err(Error::from(e)),
        }
    }
}
