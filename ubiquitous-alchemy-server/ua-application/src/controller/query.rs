//!

use actix_web::{post, web, HttpResponse, Scope};

use ua_domain_model::*;

use super::DatabaseIdRequest;
use crate::error::ServiceError;
use crate::service::{query, MutexUaStore};

#[post("/table_select")]
pub async fn table_select(
    dyn_conn: web::Data<MutexUaStore>,
    req: web::Query<DatabaseIdRequest>,
    select: web::Json<Select>,
) -> Result<HttpResponse, ServiceError> {
    // shared pool's reference has been cloned
    let conn = dyn_conn.lock().unwrap();

    // TODO: get_conn unwrap
    let dao = conn.get_conn(&req.db_id).unwrap().biz_pool.dao();

    query::table_select(dao, &select.0)
        .await
        .map(|r| HttpResponse::Ok().body(r.to_string()))
}

pub fn scope(name: &str) -> Scope {
    web::scope(name).service(table_select)
}
