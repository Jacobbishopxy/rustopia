//!

use actix_web::{get, post, web, HttpResponse, Responder};
use serde::Deserialize;

use ua_dao::dao::DaoOptions;
use ua_dao::interface::UaSchema;
use ua_model::*;

// TODO: better http response

#[derive(Deserialize)]
pub struct CreateTableReq {
    create_if_not_exists: Option<bool>,
}

#[get("/")]
async fn index() -> impl Responder {
    format!("Welcome to Sea Server!")
}

#[get("/table_list")]
pub async fn table_list(dao: web::Data<DaoOptions>) -> HttpResponse {
    let res = dao.list_table().await;

    match res {
        Ok(r) => HttpResponse::Ok().body(r.json().to_string()),
        Err(e) => {
            let s = serde_json::to_string_pretty(&e).unwrap();
            HttpResponse::BadRequest().body(s)
        }
    }
}

#[post("/table_create")]
pub async fn table_create(
    table: web::Json<TableCreate>,
    req: web::Query<CreateTableReq>,
    dao: web::Data<DaoOptions>,
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
pub async fn table_alter(table: web::Json<TableAlter>, dao: web::Data<DaoOptions>) -> HttpResponse {
    let res = dao.alter_table(&table.0).await;

    match res {
        Ok(_) => HttpResponse::Ok().body("succeeded"),
        Err(e) => {
            let s = serde_json::to_string_pretty(&e).unwrap();
            HttpResponse::BadRequest().body(s)
        }
    }
}

#[post("/table_drop")]
pub async fn table_drop(table: web::Json<TableDrop>, dao: web::Data<DaoOptions>) -> HttpResponse {
    let res = dao.drop_table(&table.0).await;

    match res {
        Ok(_) => HttpResponse::Ok().body("succeeded"),
        Err(e) => {
            let s = serde_json::to_string_pretty(&e).unwrap();
            HttpResponse::BadRequest().body(s)
        }
    }
}

#[post("/table_rename")]
pub async fn table_rename(
    table: web::Json<TableRename>,
    dao: web::Data<DaoOptions>,
) -> HttpResponse {
    let res = dao.rename_table(&table.0).await;

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
    dao: web::Data<DaoOptions>,
) -> HttpResponse {
    let res = dao.truncate_table(&table.0).await;

    match res {
        Ok(_) => HttpResponse::Ok().body("succeeded"),
        Err(e) => {
            let s = serde_json::to_string_pretty(&e).unwrap();
            HttpResponse::BadRequest().body(s)
        }
    }
}

#[post("/index_create")]
pub async fn index_create(idx: web::Json<IndexCreate>, dao: web::Data<DaoOptions>) -> HttpResponse {
    let res = dao.create_index(&idx.0).await;

    match res {
        Ok(_) => HttpResponse::Ok().body("succeeded"),
        Err(e) => {
            let s = serde_json::to_string_pretty(&e).unwrap();
            HttpResponse::BadRequest().body(s)
        }
    }
}

#[post("/index_drop")]
pub async fn index_drop(idx: web::Json<IndexDrop>, dao: web::Data<DaoOptions>) -> HttpResponse {
    let res = dao.drop_index(&idx.0).await;

    match res {
        Ok(_) => HttpResponse::Ok().body("succeeded"),
        Err(e) => {
            let s = serde_json::to_string_pretty(&e).unwrap();
            HttpResponse::BadRequest().body(s)
        }
    }
}

#[post("/foreign_key_create")]
pub async fn foreign_key_create(
    key: web::Json<ForeignKeyCreate>,
    dao: web::Data<DaoOptions>,
) -> HttpResponse {
    let res = dao.create_foreign_key(&key.0).await;

    match res {
        Ok(_) => HttpResponse::Ok().body("succeeded"),
        Err(e) => {
            let s = serde_json::to_string_pretty(&e).unwrap();
            HttpResponse::BadRequest().body(s)
        }
    }
}

#[post("/foreign_key_drop")]
pub async fn foreign_key_drop(
    key: web::Json<ForeignKeyDrop>,
    dao: web::Data<DaoOptions>,
) -> HttpResponse {
    let res = dao.drop_foreign_key(&key.0).await;

    match res {
        Ok(_) => HttpResponse::Ok().body("succeeded"),
        Err(e) => {
            let s = serde_json::to_string_pretty(&e).unwrap();
            HttpResponse::BadRequest().body(s)
        }
    }
}
