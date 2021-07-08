pub mod configuration;
pub mod query;
pub mod schema;

use actix_web::{get, Responder};
use serde::Deserialize;

#[derive(Deserialize)]

pub struct DatabaseIdRequest {
    db_id: String,
}

#[get("/")]
pub async fn index() -> impl Responder {
    format!("Welcome to Ubiquitous Alchemy Server!")
}
