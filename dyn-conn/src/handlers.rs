use std::sync::Mutex;

use actix_web::{get, post, web, HttpResponse, Responder, Scope};
use serde::Deserialize;

use crate::models::{ConnInfo, DynConn};

#[get("/")]
pub async fn index() -> impl Responder {
    format!("Welcome to DynConn!")
}

#[post("/check_connection")]
pub async fn check_connection(conn: web::Json<ConnInfo>) -> HttpResponse {
    let res = DynConn::check_connection(&conn.0).await;

    HttpResponse::Ok().body(serde_json::json!(res).to_string())
}

#[get("/info")]
pub async fn info(dyn_conn: web::Data<Mutex<DynConn>>) -> HttpResponse {
    let dc = dyn_conn.lock().unwrap().show_info();
    let body = serde_json::json!(dc).to_string();

    HttpResponse::Ok().body(body)
}

#[derive(Deserialize)]
pub struct InfoNewRequest {
    key: String,
}

#[post("/info")]
pub async fn info_new(
    dye_conn: web::Data<Mutex<DynConn>>,
    req: web::Query<InfoNewRequest>,
    body: web::Json<ConnInfo>,
) -> HttpResponse {
    let key = &req.0.key;
    let new_info = body.0;
    let res = dye_conn.lock().unwrap().new_conn(key, new_info).await;

    HttpResponse::Ok().body(res)
}

pub fn scope_util(name: &str) -> Scope {
    web::scope(name).service(check_connection)
}

pub fn scope_api(name: &str) -> Scope {
    web::scope(name)
        .service(index)
        .service(info)
        .service(info_new)
}
