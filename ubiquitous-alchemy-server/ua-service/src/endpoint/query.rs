//!

use actix_web::{post, web, HttpResponse};

use ua_dao::dao::{DaoPG, UaQuery};
use ua_model::*;

#[post("/table_select")]
pub async fn table_select(select: web::Json<Select>, dao: web::Data<DaoPG>) -> HttpResponse {
    let res = dao.select(&select.0).await;

    match res {
        Ok(r) => HttpResponse::Ok().body(r.json().to_string()),
        Err(e) => {
            let s = serde_json::to_string_pretty(&e).unwrap();
            HttpResponse::BadRequest().body(s)
        }
    }
}
