use std::sync::Mutex;

use async_trait::async_trait;

use dyn_conn::{
    BizPoolFunctionality, ConnInfo, ConnInfoFunctionality, ConnMember, ConnStore, Driver,
};
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

    async fn conn_establish(conn_info: ConnInfo) -> Result<ConnMember<UaConn>, Self::ErrorType> {
        let uri = &conn_info.to_string();

        match conn_info.driver {
            Driver::Postgres => {
                let dao = DaoOptions::PG(DaoPG::new(uri, 10).await);
                Ok(ConnMember {
                    info: conn_info,
                    biz_pool: UaConn(dao),
                })
            }
            Driver::Mysql => {
                let dao = DaoOptions::MY(DaoMY::new(uri, 10).await);
                Ok(ConnMember {
                    info: conn_info,
                    biz_pool: UaConn(dao),
                })
            }
        }
    }
}

pub type UaStore = ConnStore<UaConn>;
pub type MutexUaStore = Mutex<UaStore>;
pub type UaConnInfo = ConnInfo;
