use std::sync::Mutex;

use actix_web::{get, post, web, HttpResponse, Responder};
use serde::Deserialize;

use crate::models::{Conn, DynConn};

#[get("/")]
pub async fn index() -> impl Responder {
    format!("Welcome to DynConn!")
}

#[post("/check_connection")]
pub async fn check_connection(conn: web::Json<Conn>) -> HttpResponse {
    let res = DynConn::check_connection(&conn.0).await;

    HttpResponse::Ok().body(serde_json::json!(res).to_string())
}

#[get("/info")]
pub async fn info(dyn_conn: web::Data<Mutex<DynConn>>) -> HttpResponse {
    let dc = dyn_conn.lock().unwrap().show_store();
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
    body: web::Json<Conn>,
) -> HttpResponse {
    let key = &req.0.key;
    let new_info = body.0;
    let res = dye_conn.lock().unwrap().new_conn(key, new_info);

    HttpResponse::Ok().body(res)
}
