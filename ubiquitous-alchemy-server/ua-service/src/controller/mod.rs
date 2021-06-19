pub mod dynamic;
pub mod query;
pub mod schema;

use serde::Deserialize;

#[derive(Deserialize)]

pub struct DatabaseIdRequest {
    db_id: String,
}
