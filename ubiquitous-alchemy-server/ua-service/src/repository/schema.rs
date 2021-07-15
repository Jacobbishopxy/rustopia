//!

use async_trait::async_trait;
use sqlx::mysql::MySqlRow;
use sqlx::postgres::PgRow;
use sqlx::Row;

use crate::dao::DaoOptions;
use crate::interface::UaSchema;
use crate::provider::sea::{MY_BUILDER, PG_BUILDER};
use crate::{DaoError, QueryResult};

#[async_trait]
impl UaSchema for DaoOptions {
    type Out = Box<dyn QueryResult>;

    async fn list_column(&self, table: &str) -> Result<Self::Out, DaoError> {
        let res = match self {
            DaoOptions::PG(p) => {
                sqlx::query(&PG_BUILDER.list_column(table))
                    .map(|row: PgRow| -> (String, String) { (row.get(0), row.get(1)) })
                    .fetch_all(&p.pool)
                    .await?
            }
            DaoOptions::MY(p) => {
                sqlx::query(&MY_BUILDER.list_column(table))
                    .map(|row: MySqlRow| -> (String, String) { (row.get(0), row.get(1)) })
                    .fetch_all(&p.pool)
                    .await?
            }
        };

        Ok(Box::new(res))
    }

    async fn list_table(&self) -> Result<Self::Out, DaoError> {
        let res = match self {
            DaoOptions::PG(p) => {
                sqlx::query(&PG_BUILDER.list_table())
                    .map(|row: PgRow| -> String { row.get(0) })
                    .fetch_all(&p.pool)
                    .await
            }
            DaoOptions::MY(p) => {
                sqlx::query(&MY_BUILDER.list_table())
                    .map(|row: MySqlRow| -> String { row.get(0) })
                    .fetch_all(&p.pool)
                    .await
            }
        };

        match res {
            Ok(r) => Ok(Box::new(r)),
            Err(e) => Err(DaoError::from(e)),
        }
    }

    async fn create_table(
        &self,
        table: &sqlz::model::TableCreate,
        create_if_not_exists: bool,
    ) -> Result<Self::Out, DaoError> {
        let query = match self {
            DaoOptions::PG(_) => PG_BUILDER.create_table(&table, create_if_not_exists),
            DaoOptions::MY(_) => MY_BUILDER.create_table(&table, create_if_not_exists),
        };

        self.exec(&query).await
    }

    async fn alter_table(&self, table: &sqlz::model::TableAlter) -> Result<Self::Out, DaoError> {
        let seq_query = match self {
            DaoOptions::PG(_) => PG_BUILDER.alter_table(table),
            DaoOptions::MY(_) => MY_BUILDER.alter_table(table),
        };

        self.seq_exec(&seq_query).await
    }

    async fn drop_table(&self, table: &sqlz::model::TableDrop) -> Result<Self::Out, DaoError> {
        let query = match self {
            DaoOptions::PG(_) => PG_BUILDER.drop_table(table),
            DaoOptions::MY(_) => MY_BUILDER.drop_table(table),
        };

        self.exec(&query).await
    }

    async fn rename_table(&self, table: &sqlz::model::TableRename) -> Result<Self::Out, DaoError> {
        let query = match self {
            DaoOptions::PG(_) => PG_BUILDER.rename_table(table),
            DaoOptions::MY(_) => MY_BUILDER.rename_table(table),
        };

        self.exec(&query).await
    }

    async fn truncate_table(
        &self,
        table: &sqlz::model::TableTruncate,
    ) -> Result<Self::Out, DaoError> {
        let query = match self {
            DaoOptions::PG(_) => PG_BUILDER.truncate_table(table),
            DaoOptions::MY(_) => MY_BUILDER.truncate_table(table),
        };

        self.exec(&query).await
    }

    async fn create_index(&self, index: &sqlz::model::IndexCreate) -> Result<Self::Out, DaoError> {
        let query = match self {
            DaoOptions::PG(_) => PG_BUILDER.create_index(index),
            DaoOptions::MY(_) => MY_BUILDER.create_index(index),
        };

        self.exec(&query).await
    }

    async fn drop_index(&self, index: &sqlz::model::IndexDrop) -> Result<Self::Out, DaoError> {
        let query = match self {
            DaoOptions::PG(_) => PG_BUILDER.drop_index(index),
            DaoOptions::MY(_) => MY_BUILDER.drop_index(index),
        };

        self.exec(&query).await
    }

    async fn create_foreign_key(
        &self,
        key: &sqlz::model::ForeignKeyCreate,
    ) -> Result<Self::Out, DaoError> {
        let query = match self {
            DaoOptions::PG(_) => PG_BUILDER.create_foreign_key(key),
            DaoOptions::MY(_) => MY_BUILDER.create_foreign_key(key),
        };

        self.exec(&query).await
    }

    async fn drop_foreign_key(
        &self,
        key: &sqlz::model::ForeignKeyDrop,
    ) -> Result<Self::Out, DaoError> {
        let query = match self {
            DaoOptions::PG(_) => PG_BUILDER.drop_foreign_key(key),
            DaoOptions::MY(_) => MY_BUILDER.drop_foreign_key(key),
        };

        self.exec(&query).await
    }
}
