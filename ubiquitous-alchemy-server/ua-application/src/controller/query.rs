//!

use actix_web::{get, post, web, HttpResponse, Responder, Scope};

use sqlz::model::*;

use super::DatabaseIdRequest;
use crate::error::ServiceError;
use crate::model::{MutexUaStore, UaUtil};
use crate::service::query;

#[get("/")]
async fn index() -> impl Responder {
    format!("API: query")
}

#[post("/select")]
pub async fn select(
    dyn_conn: web::Data<MutexUaStore>,
    req: web::Query<DatabaseIdRequest>,
    select: web::Json<Select>,
) -> Result<HttpResponse, ServiceError> {
    // shared pool's reference has been cloned
    let conn = dyn_conn.lock().unwrap();
    let key = UaUtil::str_to_uuid(&req.db_id)?;

    let dao = conn.get_conn(&key)?.biz_pool.dao();

    query::table_select(dao, &select.0)
        .await
        .map(|r| HttpResponse::Ok().body(r.to_string()))
}

pub fn scope(name: &str) -> Scope {
    web::scope(name).service(index).service(select)
}
