//!

use async_trait::async_trait;
use serde::Serialize;
use sqlx::mysql::{MySql, MySqlPoolOptions};
use sqlx::postgres::{PgPoolOptions, Postgres};
use sqlx::{Connection, MySqlConnection, PgConnection, Pool};

use dyn_conn::{
    BizPoolFunctionality, ConnGeneratorFunctionality, ConnInfo, ConnInfoFunctionality, ConnMember,
    Driver,
};

/// dynamic pool options
pub enum DynPoolOptions {
    Mysql(Pool<MySql>),
    Postgres(Pool<Postgres>),
}

#[async_trait]
impl BizPoolFunctionality for DynPoolOptions {
    async fn disconnect(&self) {
        match &self {
            DynPoolOptions::Mysql(c) => {
                c.close().await;
            }
            DynPoolOptions::Postgres(c) => {
                c.close().await;
            }
        }
    }
}

#[derive(Serialize, Clone)]
pub struct RConnInfo(ConnInfo);

impl RConnInfo {
    pub fn new(ci: ConnInfo) -> Self {
        RConnInfo(ci)
    }
}

impl ConnInfoFunctionality for RConnInfo {
    fn to_conn_info(&self) -> ConnInfo {
        self.0.clone()
    }
}

#[async_trait]
impl ConnGeneratorFunctionality<RConnInfo, DynPoolOptions> for DynPoolOptions {
    type ErrorType = sqlx::Error;

    async fn check_connection(conn_info: &ConnInfo) -> Result<bool, Self::ErrorType> {
        match conn_info.driver {
            Driver::Postgres => match PgConnection::connect(&conn_info.to_string()).await {
                Ok(_) => Ok(true),
                Err(e) => Err(e),
            },
            Driver::Mysql => match MySqlConnection::connect(&conn_info.to_string()).await {
                Ok(_) => Ok(true),
                Err(e) => Err(e),
            },
        }
    }

    async fn conn_establish(
        conn_info: &ConnInfo,
    ) -> Result<ConnMember<RConnInfo, DynPoolOptions>, Self::ErrorType> {
        let uri = &conn_info.to_string();

        match conn_info.driver {
            Driver::Postgres => {
                let pool = PgPoolOptions::new()
                    .max_connections(10)
                    .connect(uri)
                    .await?;
                let pool = DynPoolOptions::Postgres(pool);
                Ok(ConnMember {
                    info: RConnInfo::new(conn_info.clone()),
                    biz_pool: pool,
                })
            }
            Driver::Mysql => {
                let pool = MySqlPoolOptions::new()
                    .max_connections(10)
                    .connect(uri)
                    .await?;
                let pool = DynPoolOptions::Mysql(pool);
                Ok(ConnMember {
                    info: RConnInfo::new(conn_info.clone()),
                    biz_pool: pool,
                })
            }
        }
    }
}
