//！

use ua_dao::interface::UaQuery;
use ua_dao::{DaoOptions, JsonType};
use ua_model::*;

use crate::error::ServiceError;

pub async fn table_select(dao: &DaoOptions, select: &Select) -> Result<JsonType, ServiceError> {
    Ok(dao.select(select).await?.json())
}
