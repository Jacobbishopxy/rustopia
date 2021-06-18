//!

use actix_web::{get, post, web, HttpResponse, Responder};
use serde::Deserialize;

use ua_model::*;

use super::DatabaseIdRequest;
use crate::error::ServiceError;
use crate::service::{schema, MutexServiceDynConn};

#[derive(Deserialize)]
pub struct CreateTableReq {
    db_id: String,
    create_if_not_exists: Option<bool>,
}

#[get("/")]
async fn index() -> impl Responder {
    format!("Welcome to Sea Server!")
}

#[get("/table_list")]
pub async fn table_list(
    dyn_conn: web::Data<MutexServiceDynConn>,
    req: web::Query<DatabaseIdRequest>,
) -> Result<HttpResponse, ServiceError> {
    let dao = dyn_conn.lock().unwrap().get_dao(&req.db_id)?.clone();

    schema::table_list(&dao)
        .await
        .map(|r| HttpResponse::Ok().body(r.to_string()))
}

#[post("/table_create")]
pub async fn table_create(
    dyn_conn: web::Data<MutexServiceDynConn>,
    req: web::Query<CreateTableReq>,
    table: web::Json<TableCreate>,
) -> Result<HttpResponse, ServiceError> {
    let create_if_not_exists = req.create_if_not_exists.unwrap_or(false);

    let dao = dyn_conn.lock().unwrap().get_dao(&req.db_id)?.clone();

    schema::table_create(&dao, &table.0, create_if_not_exists)
        .await
        .map(|r| HttpResponse::Ok().body(r.to_string()))
}

#[post("/table_alter")]
pub async fn table_alter(
    dyn_conn: web::Data<MutexServiceDynConn>,
    req: web::Query<CreateTableReq>,
    table: web::Json<TableAlter>,
) -> Result<HttpResponse, ServiceError> {
    let dao = dyn_conn.lock().unwrap().get_dao(&req.db_id)?.clone();

    schema::table_alter(&dao, &table.0)
        .await
        .map(|r| HttpResponse::Ok().body(r.to_string()))
}

#[post("/table_drop")]
pub async fn table_drop(
    dyn_conn: web::Data<MutexServiceDynConn>,
    req: web::Query<CreateTableReq>,
    table: web::Json<TableDrop>,
) -> Result<HttpResponse, ServiceError> {
    let dao = dyn_conn.lock().unwrap().get_dao(&req.db_id)?.clone();

    schema::table_drop(&dao, &table.0)
        .await
        .map(|r| HttpResponse::Ok().body(r.to_string()))
}

#[post("/table_rename")]
pub async fn table_rename(
    dyn_conn: web::Data<MutexServiceDynConn>,
    req: web::Query<CreateTableReq>,
    table: web::Json<TableRename>,
) -> Result<HttpResponse, ServiceError> {
    let dao = dyn_conn.lock().unwrap().get_dao(&req.db_id)?.clone();

    schema::table_rename(&dao, &table.0)
        .await
        .map(|r| HttpResponse::Ok().body(r.to_string()))
}

#[post("/table_truncate")]
pub async fn table_truncate(
    dyn_conn: web::Data<MutexServiceDynConn>,
    req: web::Query<CreateTableReq>,
    table: web::Json<TableTruncate>,
) -> Result<HttpResponse, ServiceError> {
    let dao = dyn_conn.lock().unwrap().get_dao(&req.db_id)?.clone();

    schema::table_truncate(&dao, &table.0)
        .await
        .map(|r| HttpResponse::Ok().body(r.to_string()))
}

#[post("/index_create")]
pub async fn index_create(
    dyn_conn: web::Data<MutexServiceDynConn>,
    req: web::Query<CreateTableReq>,
    idx: web::Json<IndexCreate>,
) -> Result<HttpResponse, ServiceError> {
    let dao = dyn_conn.lock().unwrap().get_dao(&req.db_id)?.clone();

    schema::index_create(&dao, &idx.0)
        .await
        .map(|r| HttpResponse::Ok().body(r.to_string()))
}

#[post("/index_drop")]
pub async fn index_drop(
    dyn_conn: web::Data<MutexServiceDynConn>,
    req: web::Query<CreateTableReq>,
    idx: web::Json<IndexDrop>,
) -> Result<HttpResponse, ServiceError> {
    let dao = dyn_conn.lock().unwrap().get_dao(&req.db_id)?.clone();

    schema::index_drop(&dao, &idx.0)
        .await
        .map(|r| HttpResponse::Ok().body(r.to_string()))
}

#[post("/foreign_key_create")]
pub async fn foreign_key_create(
    dyn_conn: web::Data<MutexServiceDynConn>,
    req: web::Query<CreateTableReq>,
    key: web::Json<ForeignKeyCreate>,
) -> Result<HttpResponse, ServiceError> {
    let dao = dyn_conn.lock().unwrap().get_dao(&req.db_id)?.clone();

    schema::foreign_key_create(&dao, &key.0)
        .await
        .map(|r| HttpResponse::Ok().body(r.to_string()))
}

#[post("/foreign_key_drop")]
pub async fn foreign_key_drop(
    dyn_conn: web::Data<MutexServiceDynConn>,
    req: web::Query<CreateTableReq>,
    key: web::Json<ForeignKeyDrop>,
) -> Result<HttpResponse, ServiceError> {
    let dao = dyn_conn.lock().unwrap().get_dao(&req.db_id)?.clone();

    schema::foreign_key_drop(&dao, &key.0)
        .await
        .map(|r| HttpResponse::Ok().body(r.to_string()))
}
