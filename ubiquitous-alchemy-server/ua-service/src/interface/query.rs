use async_trait::async_trait;

use crate::DaoError as Error;
use ua_domain_model::*;

#[async_trait]
pub trait UaQuery {
    type Out;

    async fn select(&self, select: &Select) -> Result<Self::Out, Error>;
}