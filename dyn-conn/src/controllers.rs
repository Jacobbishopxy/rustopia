use std::sync::Mutex;

use actix_web::{delete, get, post, put, web, HttpResponse, Responder, Scope};
use serde::Deserialize;

use crate::models::DynPoolOptions;
use crate::{ConnInfo, ConnStore};

pub type DC = ConnStore<DynPoolOptions>;

#[get("/")]
pub async fn index() -> impl Responder {
    format!("Welcome to DynConn!")
}

/// check database connection
#[post("/check_connection")]
pub async fn check_connection(conn_info: web::Json<ConnInfo>) -> HttpResponse {
    let res = DC::check_connection(&conn_info.0).await;

    HttpResponse::Ok().body(serde_json::json!(res).to_string())
}

/// get current connection pools' information
#[get("/conn")]
pub async fn conn_list(dyn_conn: web::Data<Mutex<DC>>) -> HttpResponse {
    let res = dyn_conn.lock().unwrap().show_info();
    let body = serde_json::json!(res).to_string();

    HttpResponse::Ok().body(body)
}

#[derive(Deserialize)]
pub struct ConnRequest {
    key: String,
}

/// create a new connection pool and save in memory
#[post("/conn")]
pub async fn conn_create(
    dyn_conn: web::Data<Mutex<DC>>,
    req: web::Query<ConnRequest>,
    body: web::Json<ConnInfo>,
) -> HttpResponse {
    let (key, new_info) = (&req.0.key, body.0);
    let res = dyn_conn.lock().unwrap().create_conn(key, new_info).await;

    HttpResponse::Ok().body(serde_json::json!(res).to_string())
}

/// update an existing connection pool
#[put("/conn")]
pub async fn conn_update(
    dyn_conn: web::Data<Mutex<DC>>,
    req: web::Query<ConnRequest>,
    body: web::Json<ConnInfo>,
) -> HttpResponse {
    let (key, new_info) = (&req.0.key, body.0);
    let res = dyn_conn.lock().unwrap().update_conn(key, new_info).await;

    HttpResponse::Ok().body(serde_json::json!(res).to_string())
}

/// delete an existing connection pool
#[delete("/conn")]
pub async fn conn_delete(
    dyn_conn: web::Data<Mutex<DC>>,
    req: web::Query<ConnRequest>,
) -> HttpResponse {
    let key = &req.0.key;
    let res = dyn_conn.lock().unwrap().delete_conn(key).await;

    HttpResponse::Ok().body(serde_json::json!(res).to_string())
}

/// scope for util functionality
pub fn scope_util(name: &str) -> Scope {
    web::scope(name).service(check_connection)
}

/// scope for conn functionality
pub fn scope_api(name: &str) -> Scope {
    web::scope(name)
        .service(index)
        .service(conn_list)
        .service(conn_create)
        .service(conn_update)
        .service(conn_delete)
}
