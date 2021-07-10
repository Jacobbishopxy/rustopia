use async_trait::async_trait;
use sqlx::mysql::MySqlRow;
use sqlx::postgres::PgRow;

use crate::dao::DaoOptions;
use crate::interface::UaQuery;
use crate::provider::sea::{MY_BUILDER, PG_BUILDER};
use crate::util::type_conversion;
use crate::{DaoError, QueryResult};

// database row data conversion
#[async_trait]
impl UaQuery for DaoOptions {
    type Out = Box<dyn QueryResult>;

    async fn select(&self, select: &sqlz::model::Select) -> Result<Self::Out, DaoError> {
        let columns = select.columns.iter().map(|c| c.name()).collect();
        let res = match self {
            DaoOptions::PG(p) => {
                sqlx::query(&PG_BUILDER.select(select))
                    .try_map(|row: PgRow| type_conversion::row_to_map_postgres(row, &columns))
                    .fetch_all(&p.pool)
                    .await
            }
            DaoOptions::MY(p) => {
                sqlx::query(&MY_BUILDER.select(select))
                    .try_map(|row: MySqlRow| type_conversion::row_to_map_mysql(row, &columns))
                    .fetch_all(&p.pool)
                    .await
            }
        };

        match res {
            Ok(r) => Ok(Box::new(r)),
            Err(e) => Err(DaoError::from(e)),
        }
    }
}
