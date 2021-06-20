use actix_web::{delete, get, post, put, web, HttpResponse, Scope};

use dyn_conn::{self, DynConnFunctionality};

use super::DatabaseIdRequest;
use crate::error::ServiceError;
use crate::service::{MutexUaDynConn, UaConnInfo};

#[post("/check_connection")]
pub async fn check_connection(conn_info: web::Json<UaConnInfo>) -> HttpResponse {
    let res = dyn_conn::check_connection(&conn_info.0).await;

    HttpResponse::Ok().body(serde_json::json!(res).to_string())
}

#[get("/conn")]
pub async fn conn_list(dyn_conn: web::Data<MutexUaDynConn>) -> Result<HttpResponse, ServiceError> {
    let res = dyn_conn.lock().unwrap().show_info().unwrap();

    Ok(HttpResponse::Ok().body(res.json_string()))
}

#[post("/conn")]
pub async fn conn_create(
    dyn_conn: web::Data<MutexUaDynConn>,
    req: web::Query<DatabaseIdRequest>,
    conn_info: web::Json<UaConnInfo>,
) -> Result<HttpResponse, ServiceError> {
    let res = dyn_conn
        .lock()
        .unwrap()
        .create_conn(&req.db_id, conn_info.0)
        .await?;

    Ok(HttpResponse::Ok().body(res.json_string()))
}

#[put("/conn")]
pub async fn conn_update(
    dyn_conn: web::Data<MutexUaDynConn>,
    req: web::Query<DatabaseIdRequest>,
    conn_info: web::Json<UaConnInfo>,
) -> Result<HttpResponse, ServiceError> {
    let res = dyn_conn
        .lock()
        .unwrap()
        .update_conn(&req.db_id, conn_info.0)
        .await?;

    Ok(HttpResponse::Ok().body(res.json_string()))
}

#[delete("/conn")]
pub async fn conn_delete(
    dyn_conn: web::Data<MutexUaDynConn>,
    req: web::Query<DatabaseIdRequest>,
) -> Result<HttpResponse, ServiceError> {
    let res = dyn_conn.lock().unwrap().delete_conn(&req.db_id).await?;
    Ok(HttpResponse::Ok().body(res.json_string()))
}

pub fn scope(name: &str) -> Scope {
    web::scope(name)
        .service(check_connection)
        .service(conn_list)
        .service(conn_create)
        .service(conn_update)
        .service(conn_delete)
}
