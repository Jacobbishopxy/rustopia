//！

use service::interface::UaQuery;
use service::{DaoOptions, JsonType};
use sqlz::model::*;

use crate::error::ServiceError;

pub async fn table_select(dao: &DaoOptions, select: &Select) -> Result<JsonType, ServiceError> {
    Ok(dao.select(select).await?.json())
}
