use actix_web::{delete, get, post, put, web, HttpResponse, Scope};

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
) -> Result<HttpResponse, ServiceError> {
    let res = dyn_conn.lock().unwrap().list_dao()?;

    Ok(HttpResponse::Ok().body(serde_json::json!(res).to_string()))
}

#[post("/conn")]
pub async fn conn_create(
    dyn_conn: web::Data<MutexServiceDynConn>,
    req: web::Query<DatabaseIdRequest>,
    conn_info: web::Json<ConnInfo>,
) -> Result<HttpResponse, ServiceError> {
    let res = dyn_conn
        .lock()
        .unwrap()
        .create_dao(&req.db_id, conn_info.0)
        .await?;

    Ok(HttpResponse::Ok().body(res.to_string()))
}

#[put("/conn")]
pub async fn conn_update(
    dyn_conn: web::Data<MutexServiceDynConn>,
    req: web::Query<DatabaseIdRequest>,
    conn_info: web::Json<ConnInfo>,
) -> Result<HttpResponse, ServiceError> {
    let res = dyn_conn
        .lock()
        .unwrap()
        .update_dao(&req.db_id, conn_info.0)
        .await?;

    Ok(HttpResponse::Ok().body(res.to_string()))
}

#[delete("/conn")]
pub async fn conn_delete(
    dyn_conn: web::Data<MutexServiceDynConn>,
    req: web::Query<DatabaseIdRequest>,
) -> Result<HttpResponse, ServiceError> {
    let res = dyn_conn.lock().unwrap().delete_dao(&req.db_id).await?;
    Ok(HttpResponse::Ok().body(res.to_string()))
}

pub fn scope(name: &str) -> Scope {
    web::scope(name)
        .service(check_connection)
        .service(conn_list)
        .service(conn_create)
        .service(conn_update)
        .service(conn_delete)
}
