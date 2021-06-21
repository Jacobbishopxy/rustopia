//！

use ua_domain_model::*;
use ua_service::interface::UaQuery;
use ua_service::{DaoOptions, JsonType};

use crate::error::ServiceError;

pub async fn table_select(dao: &DaoOptions, select: &Select) -> Result<JsonType, ServiceError> {
    Ok(dao.select(select).await?.json())
}
