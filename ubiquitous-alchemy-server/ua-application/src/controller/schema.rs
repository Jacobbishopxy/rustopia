//!

use actix_web::{get, post, web, HttpResponse, Responder, Scope};
use serde::Deserialize;

use sqlz::model::*;

use super::{DatabaseIdRequest, DatabaseIdTableNameRequest};
use crate::error::ServiceError;
use crate::model::{MutexUaStore, UaUtil};
use crate::service::schema;

#[derive(Deserialize)]
pub struct CreateTableReq {
    db_id: String,
    create_if_not_exists: Option<bool>,
}

#[get("/")]
async fn index() -> impl Responder {
    format!("API: schema")
}

#[get("/column_list")]
pub async fn column_list(
    dyn_conn: web::Data<MutexUaStore>,
    req: web::Query<DatabaseIdTableNameRequest>,
) -> Result<HttpResponse, ServiceError> {
    let conn = dyn_conn.lock().unwrap();
    let key = UaUtil::str_to_uuid(&req.db_id)?;
    let dao = conn.get_conn(&key)?.biz_pool.dao();
    let table = &req.table;

    schema::column_list(dao, table)
        .await
        .map(|r| HttpResponse::Ok().body(r.to_string()))
}

#[get("/table_list")]
pub async fn table_list(
    dyn_conn: web::Data<MutexUaStore>,
    req: web::Query<DatabaseIdRequest>,
) -> Result<HttpResponse, ServiceError> {
    let conn = dyn_conn.lock().unwrap();
    let key = UaUtil::str_to_uuid(&req.db_id)?;
    let dao = conn.get_conn(&key)?.biz_pool.dao();

    schema::table_list(dao)
        .await
        .map(|r| HttpResponse::Ok().body(r.to_string()))
}

#[post("/table_create")]
pub async fn table_create(
    dyn_conn: web::Data<MutexUaStore>,
    req: web::Query<CreateTableReq>,
    table: web::Json<TableCreate>,
) -> Result<HttpResponse, ServiceError> {
    let create_if_not_exists = req.create_if_not_exists.unwrap_or(false);
    let key = UaUtil::str_to_uuid(&req.db_id)?;

    let conn = dyn_conn.lock().unwrap();
    let dao = conn.get_conn(&key)?.biz_pool.dao();

    schema::table_create(dao, &table.0, create_if_not_exists)
        .await
        .map(|r| HttpResponse::Ok().body(r.to_string()))
}

#[post("/table_alter")]
pub async fn table_alter(
    dyn_conn: web::Data<MutexUaStore>,
    req: web::Query<CreateTableReq>,
    table: web::Json<TableAlter>,
) -> Result<HttpResponse, ServiceError> {
    let conn = dyn_conn.lock().unwrap();
    let key = UaUtil::str_to_uuid(&req.db_id)?;
    let dao = conn.get_conn(&key)?.biz_pool.dao();

    schema::table_alter(&dao, &table.0)
        .await
        .map(|r| HttpResponse::Ok().body(r.to_string()))
}

#[post("/table_drop")]
pub async fn table_drop(
    dyn_conn: web::Data<MutexUaStore>,
    req: web::Query<CreateTableReq>,
    table: web::Json<TableDrop>,
) -> Result<HttpResponse, ServiceError> {
    let conn = dyn_conn.lock().unwrap();
    let key = UaUtil::str_to_uuid(&req.db_id)?;
    let dao = conn.get_conn(&key)?.biz_pool.dao();

    schema::table_drop(&dao, &table.0)
        .await
        .map(|r| HttpResponse::Ok().body(r.to_string()))
}

#[post("/table_rename")]
pub async fn table_rename(
    dyn_conn: web::Data<MutexUaStore>,
    req: web::Query<CreateTableReq>,
    table: web::Json<TableRename>,
) -> Result<HttpResponse, ServiceError> {
    let conn = dyn_conn.lock().unwrap();
    let key = UaUtil::str_to_uuid(&req.db_id)?;
    let dao = conn.get_conn(&key)?.biz_pool.dao();

    schema::table_rename(&dao, &table.0)
        .await
        .map(|r| HttpResponse::Ok().body(r.to_string()))
}

#[post("/table_truncate")]
pub async fn table_truncate(
    dyn_conn: web::Data<MutexUaStore>,
    req: web::Query<CreateTableReq>,
    table: web::Json<TableTruncate>,
) -> Result<HttpResponse, ServiceError> {
    let conn = dyn_conn.lock().unwrap();
    let key = UaUtil::str_to_uuid(&req.db_id)?;
    let dao = conn.get_conn(&key)?.biz_pool.dao();

    schema::table_truncate(&dao, &table.0)
        .await
        .map(|r| HttpResponse::Ok().body(r.to_string()))
}

#[post("/index_create")]
pub async fn index_create(
    dyn_conn: web::Data<MutexUaStore>,
    req: web::Query<CreateTableReq>,
    idx: web::Json<IndexCreate>,
) -> Result<HttpResponse, ServiceError> {
    let conn = dyn_conn.lock().unwrap();
    let key = UaUtil::str_to_uuid(&req.db_id)?;
    let dao = conn.get_conn(&key)?.biz_pool.dao();

    schema::index_create(&dao, &idx.0)
        .await
        .map(|r| HttpResponse::Ok().body(r.to_string()))
}

#[post("/index_drop")]
pub async fn index_drop(
    dyn_conn: web::Data<MutexUaStore>,
    req: web::Query<CreateTableReq>,
    idx: web::Json<IndexDrop>,
) -> Result<HttpResponse, ServiceError> {
    let conn = dyn_conn.lock().unwrap();
    let key = UaUtil::str_to_uuid(&req.db_id)?;
    let dao = conn.get_conn(&key)?.biz_pool.dao();

    schema::index_drop(&dao, &idx.0)
        .await
        .map(|r| HttpResponse::Ok().body(r.to_string()))
}

#[post("/foreign_key_create")]
pub async fn foreign_key_create(
    dyn_conn: web::Data<MutexUaStore>,
    req: web::Query<CreateTableReq>,
    key: web::Json<ForeignKeyCreate>,
) -> Result<HttpResponse, ServiceError> {
    let conn = dyn_conn.lock().unwrap();
    let id = UaUtil::str_to_uuid(&req.db_id)?;
    let dao = conn.get_conn(&id)?.biz_pool.dao();

    schema::foreign_key_create(&dao, &key.0)
        .await
        .map(|r| HttpResponse::Ok().body(r.to_string()))
}

#[post("/foreign_key_drop")]
pub async fn foreign_key_drop(
    dyn_conn: web::Data<MutexUaStore>,
    req: web::Query<CreateTableReq>,
    key: web::Json<ForeignKeyDrop>,
) -> Result<HttpResponse, ServiceError> {
    let conn = dyn_conn.lock().unwrap();
    let id = UaUtil::str_to_uuid(&req.db_id)?;
    let dao = conn.get_conn(&id)?.biz_pool.dao();

    schema::foreign_key_drop(&dao, &key.0)
        .await
        .map(|r| HttpResponse::Ok().body(r.to_string()))
}

pub fn scope(name: &str) -> Scope {
    web::scope(name)
        .service(index)
        .service(column_list)
        .service(table_list)
        .service(table_create)
        .service(table_alter)
        .service(table_drop)
        .service(table_rename)
        .service(table_truncate)
        .service(index_create)
        .service(index_drop)
        .service(foreign_key_create)
        .service(foreign_key_drop)
}
