//! Database DDL

use sqlz::model::*;
use ua_service::interface::UaSchema;
use ua_service::{DaoOptions, JsonType};

use crate::error::ServiceError;

pub async fn column_list(dao: &DaoOptions, table: &str) -> Result<JsonType, ServiceError> {
    Ok(dao.list_column(table).await?.json())
}

pub async fn table_list(dao: &DaoOptions) -> Result<JsonType, ServiceError> {
    Ok(dao.list_table().await?.json())
}

pub async fn table_create(
    dao: &DaoOptions,
    table: &TableCreate,
    create_if_not_exists: bool,
) -> Result<JsonType, ServiceError> {
    Ok(dao.create_table(table, create_if_not_exists).await?.json())
}

pub async fn table_alter(dao: &DaoOptions, table: &TableAlter) -> Result<JsonType, ServiceError> {
    Ok(dao.alter_table(table).await?.json())
}

pub async fn table_drop(dao: &DaoOptions, table: &TableDrop) -> Result<JsonType, ServiceError> {
    Ok(dao.drop_table(table).await?.json())
}

pub async fn table_rename(dao: &DaoOptions, table: &TableRename) -> Result<JsonType, ServiceError> {
    Ok(dao.rename_table(table).await?.json())
}

pub async fn table_truncate(
    dao: &DaoOptions,
    table: &TableTruncate,
) -> Result<JsonType, ServiceError> {
    Ok(dao.truncate_table(table).await?.json())
}

pub async fn index_create(dao: &DaoOptions, index: &IndexCreate) -> Result<JsonType, ServiceError> {
    Ok(dao.create_index(index).await?.json())
}

pub async fn index_drop(dao: &DaoOptions, index: &IndexDrop) -> Result<JsonType, ServiceError> {
    Ok(dao.drop_index(index).await?.json())
}

pub async fn foreign_key_create(
    dao: &DaoOptions,
    key: &ForeignKeyCreate,
) -> Result<JsonType, ServiceError> {
    Ok(dao.create_foreign_key(key).await?.json())
}

pub async fn foreign_key_drop(
    dao: &DaoOptions,
    key: &ForeignKeyDrop,
) -> Result<JsonType, ServiceError> {
    Ok(dao.drop_foreign_key(key).await?.json())
}
