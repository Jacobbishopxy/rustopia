use std::sync::Mutex;

use async_trait::async_trait;

use dyn_conn::{
    BizPoolFunctionality, ConnInfo, ConnInfoFunctionality, ConnMember, ConnStore, ConnStoreError,
    Driver, PersistenceFunctionality,
};
use ua_persistence::{ConnectionInformation, PersistenceDao};
use ua_service::{DaoMY, DaoOptions, DaoPG};

use crate::error::ServiceError;

pub struct UaConn(DaoOptions);

impl UaConn {
    pub fn dao(&self) -> &DaoOptions {
        &self.0
    }
}

#[async_trait]
impl BizPoolFunctionality for UaConn {
    async fn disconnect(&self) {
        match &self.0 {
            DaoOptions::PG(p) => {
                p.pool.close().await;
            }
            DaoOptions::MY(p) => {
                p.pool.close().await;
            }
        }
    }
}

#[async_trait]
impl ConnInfoFunctionality<UaConn> for UaConn {
    type ErrorType = ServiceError;

    async fn check_connection(conn_info: &ConnInfo) -> Result<bool, Self::ErrorType> {
        match conn_info.driver {
            Driver::Postgres => todo!(),
            Driver::Mysql => todo!(),
        }
    }

    async fn conn_establish(conn_info: &ConnInfo) -> Result<ConnMember<UaConn>, Self::ErrorType> {
        let uri = &conn_info.to_string();

        match conn_info.driver {
            Driver::Postgres => {
                let dao = DaoOptions::PG(DaoPG::new(uri, 10).await);
                Ok(ConnMember {
                    info: conn_info.clone(),
                    biz_pool: UaConn(dao),
                })
            }
            Driver::Mysql => {
                let dao = DaoOptions::MY(DaoMY::new(uri, 10).await);
                Ok(ConnMember {
                    info: conn_info.clone(),
                    biz_pool: UaConn(dao),
                })
            }
        }
    }
}

pub type UaStore = ConnStore<UaConn>;
pub type MutexUaStore = Mutex<UaStore>;
pub type UaConnInfo = ConnInfo;

pub struct UaPersistence(PersistenceDao);

impl UaPersistence {
    pub async fn new(conn: &str) -> Self {
        UaPersistence(PersistenceDao::new(conn).await)
    }
}

#[async_trait]
impl PersistenceFunctionality for UaPersistence {
    async fn load(&self, key: &str) -> Result<ConnInfo, ConnStoreError> {
        todo!()
    }

    async fn load_all(
        &self,
    ) -> Result<std::collections::HashMap<String, ConnInfo>, ConnStoreError> {
        todo!()
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
