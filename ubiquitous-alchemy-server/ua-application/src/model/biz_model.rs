//! integration of concrete business logic and abstract interfaces

use std::sync::Mutex;

use async_trait::async_trait;
use serde::Serialize;

use dyn_conn::{
    BizPoolFunctionality, ConnGeneratorFunctionality, ConnInfo, ConnInfoFunctionality, ConnMember,
    ConnStore, ConnUtil, Driver,
};
use ua_persistence::ConnectionInformation;
use ua_service::{DaoMY, DaoOptions, DaoPG};

use crate::error::ServiceError;

#[derive(Serialize, Clone)]
pub struct CI(ConnectionInformation);

impl From<ConnectionInformation> for CI {
    fn from(ci: ConnectionInformation) -> Self {
        CI(ci)
    }
}

impl CI {
    pub fn new(conn_info: ConnInfo) -> Self {
        let drv = if conn_info.driver == Driver::Postgres {
            "postgres"
        } else {
            "mysql"
        };
        CI(ConnectionInformation {
            id: None,
            name: "".to_owned(),
            description: None,
            driver: drv.to_owned(),
            username: conn_info.username,
            password: conn_info.password,
            host: conn_info.host,
            port: conn_info.port,
            database: conn_info.database,
        })
    }

    pub fn ci(&self) -> ConnectionInformation {
        self.0.clone()
    }
}

impl ConnInfoFunctionality for CI {
    fn to_conn_info(&self) -> ConnInfo {
        let drv = if self.0.driver == "postgres" {
            Driver::Postgres
        } else {
            Driver::Mysql
        };
        ConnInfo {
            driver: drv,
            username: self.0.username.clone(),
            password: self.0.password.clone(),
            host: self.0.host.clone(),
            port: self.0.port.clone(),
            database: self.0.database.clone(),
        }
    }
}
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
impl ConnGeneratorFunctionality<CI, UaConn> for UaConn {
    type ErrorType = ServiceError;

    async fn check_connection(conn_info: &ConnInfo) -> Result<bool, Self::ErrorType> {
        match conn_info.driver {
            Driver::Postgres => Ok(DaoPG::connectable(&conn_info.to_string()).await),
            Driver::Mysql => Ok(DaoMY::connectable(&conn_info.to_string()).await),
        }
    }

    async fn conn_establish(
        conn_info: &ConnInfo,
    ) -> Result<ConnMember<CI, UaConn>, Self::ErrorType> {
        let uri = &conn_info.to_string();

        match conn_info.driver {
            Driver::Postgres => {
                let dao = DaoOptions::PG(DaoPG::new(uri, 10).await?);
                Ok(ConnMember {
                    info: CI::new(conn_info.clone()),
                    biz_pool: UaConn(dao),
                })
            }
            Driver::Mysql => {
                let dao = DaoOptions::MY(DaoMY::new(uri, 10).await?);
                Ok(ConnMember {
                    info: CI::new(conn_info.clone()),
                    biz_pool: UaConn(dao),
                })
            }
        }
    }
}

pub type UaStore = ConnStore<CI, UaConn>;
pub type MutexUaStore = Mutex<UaStore>;
pub type UaConnInfo = ConnectionInformation;
pub type UaUtil = ConnUtil;
