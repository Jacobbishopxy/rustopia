//!

use std::collections::HashMap;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use sqlx::mysql::{MySql, MySqlPoolOptions};
use sqlx::postgres::{PgPoolOptions, Postgres};
use sqlx::Pool;

use dyn_conn::{
    BizPoolFunctionality, ConnInfo, ConnInfoFunctionality, ConnMember, ConnStoreError, Driver,
    PersistenceFunctionality,
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

#[async_trait]
impl ConnInfoFunctionality<DynPoolOptions> for DynPoolOptions {
    type ErrorType = sqlx::Error;

    async fn conn_establish(
        conn_info: ConnInfo,
    ) -> Result<ConnMember<DynPoolOptions>, Self::ErrorType> {
        let uri = &conn_info.to_string();

        match conn_info.driver {
            Driver::Postgres => {
                let pool = PgPoolOptions::new()
                    .max_connections(10)
                    .connect(uri)
                    .await?;
                let pool = DynPoolOptions::Postgres(pool);
                Ok(ConnMember {
                    info: conn_info.clone(),
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
                    info: conn_info.clone(),
                    biz_pool: pool,
                })
            }
        }
    }
}

#[derive(Serialize, Deserialize, Debug, sqlx::FromRow)]
pub struct ConnectionInformation {
    pub id: String,
    pub driver: String,
    pub username: String,
    pub password: String,
    pub host: String,
    pub port: i32,
    pub database: String,
}

type ConnInfoKV = (String, ConnInfo);

impl From<ConnectionInformation> for ConnInfoKV {
    fn from(ci: ConnectionInformation) -> Self {
        let k = ci.id;
        let drv = if ci.driver == "postgres" {
            Driver::Postgres
        } else {
            Driver::Mysql
        };
        let v = ConnInfo {
            driver: drv,
            username: ci.username,
            password: ci.password,
            host: ci.host,
            port: ci.port,
            database: ci.database,
        };

        (k, v)
    }
}

#[async_trait]
impl PersistenceFunctionality for DynPoolOptions {
    async fn load(&self, key: &str) -> Result<ConnInfo, ConnStoreError> {
        let qu = "SELECT * FROM conn_info WHERE id = ? LIMIT 1";
        match self {
            DynPoolOptions::Mysql(p) => {
                match sqlx::query_as::<_, ConnectionInformation>(qu)
                    .bind(key)
                    .fetch_one(p)
                    .await
                {
                    Ok(res) => Ok(ConnInfoKV::from(res).1),
                    Err(_) => Err(ConnStoreError::ConnFailed(key.to_owned())),
                }
            }
            DynPoolOptions::Postgres(p) => {
                match sqlx::query_as::<_, ConnectionInformation>(qu)
                    .bind(key)
                    .fetch_one(p)
                    .await
                {
                    Ok(res) => Ok(ConnInfoKV::from(res).1),
                    Err(_) => Err(ConnStoreError::ConnFailed(key.to_owned())),
                }
            }
        }
    }

    async fn load_all(&self) -> Result<HashMap<String, ConnInfo>, ConnStoreError> {
        let qu = "SELECT * FROM conn_info";
        match self {
            DynPoolOptions::Mysql(p) => {
                match sqlx::query_as::<_, ConnectionInformation>(qu)
                    .fetch_all(p)
                    .await
                {
                    Ok(res) => Ok(res.into_iter().map(|r| ConnInfoKV::from(r)).collect()),
                    Err(_) => Err(ConnStoreError::ConnFailed("load all failed".to_owned())),
                }
            }
            DynPoolOptions::Postgres(p) => {
                match sqlx::query_as::<_, ConnectionInformation>(qu)
                    .fetch_all(p)
                    .await
                {
                    Ok(res) => Ok(res.into_iter().map(|r| ConnInfoKV::from(r)).collect()),
                    Err(_) => Err(ConnStoreError::ConnFailed("load all failed".to_owned())),
                }
            }
        }
    }

    async fn save(&self, key: &str, conn: &ConnInfo) -> Result<ConnInfo, ConnStoreError> {
        todo!()
    }

    async fn update(&self, key: &str, conn: &ConnInfo) -> Result<ConnInfo, ConnStoreError> {
        todo!()
    }

    async fn delete(&self, key: &str) -> Result<ConnInfo, ConnStoreError> {
        todo!()
    }
}
