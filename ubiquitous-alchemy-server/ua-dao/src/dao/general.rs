//!

use async_trait::async_trait;

use crate::dao::DaoOptions;
use crate::interface::UaSchema;
use crate::provider::sea::{Builder, BuilderType};
use crate::QueryResult;

const PG_BUILDER: Builder = Builder(BuilderType::PG);
const MY_BUILDER: Builder = Builder(BuilderType::MY);

#[async_trait]
impl UaSchema for DaoOptions {
    type Out = Box<dyn QueryResult>;

    async fn execute(&self, str: &str) -> Result<Self::Out, crate::DaoError> {
        todo!()
    }

    async fn list_table(&self) -> Result<Self::Out, crate::DaoError> {
        todo!()
    }

    async fn create_table(
        &self,
        table: ua_model::TableCreate,
        create_if_not_exists: bool,
    ) -> Result<Self::Out, crate::DaoError> {
        todo!()
    }

    async fn alter_table(
        &self,
        table: &ua_model::TableAlter,
    ) -> Result<Self::Out, crate::DaoError> {
        todo!()
    }

    async fn drop_table(&self, table: &ua_model::TableDrop) -> Result<Self::Out, crate::DaoError> {
        todo!()
    }

    async fn rename_table(
        &self,
        table: &ua_model::TableRename,
    ) -> Result<Self::Out, crate::DaoError> {
        todo!()
    }

    async fn truncate_table(
        &self,
        table: &ua_model::TableTruncate,
    ) -> Result<Self::Out, crate::DaoError> {
        todo!()
    }

    async fn create_index(
        &self,
        index: &ua_model::IndexCreate,
    ) -> Result<Self::Out, crate::DaoError> {
        todo!()
    }

    async fn drop_index(&self, index: &ua_model::IndexDrop) -> Result<Self::Out, crate::DaoError> {
        todo!()
    }

    async fn create_foreign_key(
        &self,
        key: &ua_model::ForeignKeyCreate,
    ) -> Result<Self::Out, crate::DaoError> {
        todo!()
    }

    async fn drop_foreign_key(
        &self,
        key: &ua_model::ForeignKeyDrop,
    ) -> Result<Self::Out, crate::DaoError> {
        todo!()
    }
}
