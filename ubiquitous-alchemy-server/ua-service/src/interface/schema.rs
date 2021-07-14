use async_trait::async_trait;

use crate::DaoError as Error;
use sqlz::model::*;

#[async_trait]
pub trait UaSchema {
    type Out;

    async fn list_column(&self, table: &str) -> Result<Self::Out, Error>;

    async fn list_table(&self) -> Result<Self::Out, Error>;

    async fn create_table(
        &self,
        table: &TableCreate,
        create_if_not_exists: bool,
    ) -> Result<Self::Out, Error>;

    async fn alter_table(&self, table: &TableAlter) -> Result<Self::Out, Error>;

    async fn drop_table(&self, table: &TableDrop) -> Result<Self::Out, Error>;

    async fn rename_table(&self, table: &TableRename) -> Result<Self::Out, Error>;

    async fn truncate_table(&self, table: &TableTruncate) -> Result<Self::Out, Error>;

    async fn create_index(&self, index: &IndexCreate) -> Result<Self::Out, Error>;

    async fn drop_index(&self, index: &IndexDrop) -> Result<Self::Out, Error>;

    async fn create_foreign_key(&self, key: &ForeignKeyCreate) -> Result<Self::Out, Error>;

    async fn drop_foreign_key(&self, key: &ForeignKeyDrop) -> Result<Self::Out, Error>;
}
