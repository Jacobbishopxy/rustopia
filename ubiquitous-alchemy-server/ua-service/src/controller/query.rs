//!

use actix_web::{post, web, HttpResponse};

use ua_model::*;

use super::DatabaseIdRequest;
use crate::error::ServiceError;
use crate::service::{query, MutexServiceDynConn};

#[post("/table_select")]
pub async fn table_select(
    dyn_conn: web::Data<MutexServiceDynConn>,
    req: web::Query<DatabaseIdRequest>,
    select: web::Json<Select>,
) -> Result<HttpResponse, ServiceError> {
    // shared pool's reference has been cloned
    let dao = dyn_conn.lock().unwrap().get_dao(&req.db_id)?.clone();

    query::table_select(&dao, &select.0)
        .await
        .map(|r| HttpResponse::Ok().body(r.to_string()))
}
