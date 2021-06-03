use async_trait::async_trait;

use crate::error::DaoError as Error;
use ua_model::*;

#[async_trait]
pub trait UaQuery {
    type Res;

    async fn select(&self, select: Select) -> Result<Self::Res, Error>;
}
