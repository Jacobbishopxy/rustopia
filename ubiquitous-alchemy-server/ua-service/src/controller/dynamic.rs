use actix_web::{delete, get, post, put, web, HttpResponse};

use dyn_conn::{ConnInfo, DynConn};

use super::DatabaseIdRequest;
use crate::{error::ServiceError, service::MutexServiceDynConn};

#[post("/check_connection")]
pub async fn check_connection(conn_info: web::Json<ConnInfo>) -> HttpResponse {
    let res = DynConn::check_connection(&conn_info.0).await;

    HttpResponse::Ok().body(serde_json::json!(res).to_string())
}

#[get("/conn")]
pub async fn conn_list(
    dyn_conn: web::Data<MutexServiceDynConn>,
    db_id: web::Query<DatabaseIdRequest>,
) -> Result<HttpResponse, ServiceError> {
    todo!()
}

#[post("/conn")]
pub async fn conn_create(
    dyn_conn: web::Data<MutexServiceDynConn>,
    db_id: web::Query<DatabaseIdRequest>,
    conn_info: web::Json<ConnInfo>,
) -> Result<HttpResponse, ServiceError> {
    todo!()
}

#[put("/conn")]
pub async fn conn_update(
    dyn_conn: web::Data<MutexServiceDynConn>,
    db_id: web::Query<DatabaseIdRequest>,
    conn_info: web::Json<ConnInfo>,
) -> Result<HttpResponse, ServiceError> {
    todo!()
}

#[delete("/conn")]
pub async fn conn_delete(
    dyn_conn: web::Data<MutexServiceDynConn>,
    db_id: web::Query<DatabaseIdRequest>,
) -> Result<HttpResponse, ServiceError> {
    todo!()
}
