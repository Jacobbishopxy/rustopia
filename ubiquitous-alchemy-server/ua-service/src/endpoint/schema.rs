//!

use actix_web::{get, post, web, HttpResponse, Responder};
use serde::Deserialize;

use ua_dao::dao::{DaoPG, UaSchema};
use ua_model::{
    IndexCreate, IndexDrop, TableAlter, TableCreate, TableDrop, TableRename, TableTruncate,
};

// TODO: 1. abstraction on `dao`; 2. better http response

#[derive(Deserialize)]
pub struct CreateTableReq {
    create_if_not_exists: Option<bool>,
}

#[get("/")]
async fn index() -> impl Responder {
    format!("Welcome to Sea Server!")
}

#[post("/table_create")]
pub async fn table_create(
    table: web::Json<TableCreate>,
    req: web::Query<CreateTableReq>,
    dao: web::Data<DaoPG>,
) -> HttpResponse {
    let create_if_not_exists = req.create_if_not_exists.unwrap_or(false);

    let res = dao.create_table(table.0, create_if_not_exists).await;

    match res {
        Ok(_) => HttpResponse::Ok().body("succeeded"),
        Err(e) => {
            let s = serde_json::to_string_pretty(&e).unwrap();
            HttpResponse::BadRequest().body(s)
        }
    }
}

#[post("/table_alter")]
pub async fn table_alter(table: web::Json<TableAlter>, dao: web::Data<DaoPG>) -> HttpResponse {
    let res = dao.alter_table(table.0).await;

    match res {
        Ok(_) => HttpResponse::Ok().body("succeeded"),
        Err(e) => {
            let s = serde_json::to_string_pretty(&e).unwrap();
            HttpResponse::BadRequest().body(s)
        }
    }
}

#[post("/table_drop")]
pub async fn table_drop(table: web::Json<TableDrop>, dao: web::Data<DaoPG>) -> HttpResponse {
    let res = dao.drop_table(table.0).await;

    match res {
        Ok(_) => HttpResponse::Ok().body("succeeded"),
        Err(e) => {
            let s = serde_json::to_string_pretty(&e).unwrap();
            HttpResponse::BadRequest().body(s)
        }
    }
}

#[post("/table_rename")]
pub async fn table_rename(table: web::Json<TableRename>, dao: web::Data<DaoPG>) -> HttpResponse {
    let res = dao.rename_table(table.0).await;

    match res {
        Ok(_) => HttpResponse::Ok().body("succeeded"),
        Err(e) => {
            let s = serde_json::to_string_pretty(&e).unwrap();
            HttpResponse::BadRequest().body(s)
        }
    }
}

#[post("/table_truncate")]
pub async fn table_truncate(
    table: web::Json<TableTruncate>,
    dao: web::Data<DaoPG>,
) -> HttpResponse {
    let res = dao.truncate_table(table.0).await;

    match res {
        Ok(_) => HttpResponse::Ok().body("succeeded"),
        Err(e) => {
            let s = serde_json::to_string_pretty(&e).unwrap();
            HttpResponse::BadRequest().body(s)
        }
    }
}

#[post("/index_create")]
pub async fn index_create(idx: web::Json<IndexCreate>, dao: web::Data<DaoPG>) -> HttpResponse {
    let res = dao.create_index(idx.0).await;

    match res {
        Ok(_) => HttpResponse::Ok().body("succeeded"),
        Err(e) => {
            let s = serde_json::to_string_pretty(&e).unwrap();
            HttpResponse::BadRequest().body(s)
        }
    }
}

#[post("/index_drop")]
pub async fn index_drop(idx: web::Json<IndexDrop>, dao: web::Data<DaoPG>) -> HttpResponse {
    let res = dao.drop_index(idx.0).await;

    match res {
        Ok(_) => HttpResponse::Ok().body("succeeded"),
        Err(e) => {
            let s = serde_json::to_string_pretty(&e).unwrap();
            HttpResponse::BadRequest().body(s)
        }
    }
}
