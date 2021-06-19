pub mod query;
pub mod schema;

use std::collections::HashMap;

use dyn_conn::{Conn, ConnInfo, DynConn, DynPoolOptions};
use std::sync::Mutex;
use ua_dao::{DaoError, DaoMY, DaoOptions, DaoPG};

use crate::error::ServiceError;

/// Conn's facade mode (todo: redundant, consider improving)
pub struct ServiceConn(Conn);

/// conversion from DynPoolOptions to DaoOptions
impl From<ServiceConn> for DaoOptions {
    fn from(conn: ServiceConn) -> Self {
        match conn.0.pool {
            DynPoolOptions::Mysql(pool) => DaoOptions::MY(DaoMY {
                info: conn.0.info.to_string(),
                pool,
            }),
            DynPoolOptions::Postgres(pool) => DaoOptions::PG(DaoPG {
                info: conn.0.info.to_string(),
                pool,
            }),
        }
    }
}

/// main struct using as shared data among cross threads
pub struct ServiceDynConn {
    pub store: HashMap<String, DaoOptions>,
}

/// conversion from DynConn to DaoOptions (todo: losing DynConn's methods, consider improving)
impl From<DynConn> for ServiceDynConn {
    fn from(dyn_conn: DynConn) -> Self {
        ServiceDynConn {
            store: dyn_conn
                .store
                .into_iter()
                .map(|(k, c)| (k, DaoOptions::from(ServiceConn(c))))
                .collect(),
        }
    }
}

impl ServiceDynConn {
    pub fn new(dyn_conn: DynConn) -> Self {
        ServiceDynConn::from(dyn_conn)
    }

    pub fn get_dao(&self, key: &str) -> Result<&DaoOptions, ServiceError> {
        if let Some(d) = self.store.get(key) {
            return Ok(d);
        }
        Err(ServiceError::DaoNotFoundError(key.to_owned()))
    }

    pub fn list_dao(&self) -> Result<HashMap<String, String>, ServiceError> {
        let res = self
            .store
            .iter()
            .map(|(k, c)| (k.to_owned(), c.info()))
            .collect();

        Ok(res)
    }

    pub async fn create_dao(
        &mut self,
        key: &str,
        conn_info: ConnInfo,
    ) -> Result<bool, ServiceError> {
        if !self.store.contains_key(key) {
            if let Ok(r) = conn_info.to_conn().await {
                self.store
                    .insert(key.to_owned(), DaoOptions::from(ServiceConn(r)));
                return Ok(true);
            }
            return Err(ServiceError::DaoError(DaoError::DatabaseConnectionError(
                conn_info.to_string(),
            )));
        }
        Err(ServiceError::DaoAlreadyExistError(key.to_owned()))
    }

    pub async fn delete_dao(&self, key: &str) -> Result<bool, ServiceError> {
        if self.store.contains_key(key) {
            self.store.get(key).unwrap().disconnect().await;
            return Ok(true);
        }
        Err(ServiceError::DaoNotFoundError(key.to_owned()))
    }

    pub async fn update_dao(
        &mut self,
        key: &str,
        conn_info: ConnInfo,
    ) -> Result<bool, ServiceError> {
        if self.store.contains_key(key) {
            if let Ok(r) = conn_info.to_conn().await {
                self.store
                    .insert(key.to_owned(), DaoOptions::from(ServiceConn(r)));
                return Ok(true);
            }
            return Err(ServiceError::DaoError(DaoError::DatabaseConnectionError(
                conn_info.to_string(),
            )));
        }
        Err(ServiceError::DaoNotFoundError(key.to_owned()))
    }
}

pub type MutexServiceDynConn = Mutex<ServiceDynConn>;
