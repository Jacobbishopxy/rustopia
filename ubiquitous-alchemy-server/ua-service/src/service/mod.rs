pub mod query;
pub mod schema;

use std::{collections::HashMap, sync::Mutex};

use async_trait::async_trait;
use serde::Serialize;

use dyn_conn::{models::DynPoolFunctionality, Conn, ConnInfo, DynConn, DynConnFunctionality};
use ua_dao::{DaoError, DaoMY, DaoOptions, DaoPG};

use crate::error::ServiceError;

pub struct UaConn(DaoOptions);

impl UaConn {
    pub fn dao(&self) -> &DaoOptions {
        &self.0
    }
}

#[async_trait]
impl DynPoolFunctionality for UaConn {
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

pub type MutexUaDynConn = Mutex<DynConn<UaConn>>;
pub type UaConnInfo = ConnInfo;

#[derive(Serialize)]
#[serde(untagged)]
pub enum DynConnUaOut {
    Simple(String),
    SimpleMap(HashMap<String, String>),
}

impl DynConnUaOut {
    pub fn json(&self) -> serde_json::Value {
        serde_json::json!(self)
    }

    pub fn json_string(&self) -> String {
        self.json().to_string()
    }
}

async fn conn_establish(conn_info: ConnInfo) -> Result<Conn<UaConn>, ServiceError> {
    let uri = &conn_info.to_string();

    match conn_info.driver {
        dyn_conn::Driver::Postgres => {
            let dao = DaoOptions::PG(DaoPG::new(uri, 10).await);
            Ok(Conn {
                info: conn_info,
                pool: UaConn(dao),
            })
        }
        dyn_conn::Driver::Mysql => {
            let dao = DaoOptions::MY(DaoMY::new(uri, 10).await);
            Ok(Conn {
                info: conn_info,
                pool: UaConn(dao),
            })
        }
    }
}

#[async_trait]
impl DynConnFunctionality<UaConn> for DynConn<UaConn> {
    type Out = Result<DynConnUaOut, ServiceError>;

    fn show_info(&self) -> Self::Out {
        let res = self
            .store
            .iter()
            .map(|(k, v)| (k.clone(), v.info.to_string()))
            .collect();

        Ok(DynConnUaOut::SimpleMap(res))
    }

    async fn delete_conn(&mut self, key: &str) -> Self::Out {
        match self.store.contains_key(key) {
            true => {
                self.store.get(key).unwrap().pool.disconnect().await;
                Ok(DynConnUaOut::Simple(format!("Disconnected from {:?}", key)))
            }
            false => Err(ServiceError::DaoNotFoundError(key.to_owned())),
        }
    }

    async fn create_conn(&mut self, key: &str, conn_info: ConnInfo) -> Self::Out {
        match self.store.contains_key(key) {
            true => Err(ServiceError::DaoAlreadyExistError(key.to_owned())),
            false => {
                if let Ok(r) = conn_establish(conn_info.clone()).await {
                    self.store.insert(key.to_owned(), r);
                    return Ok(DynConnUaOut::Simple(format!(
                        "New conn {:?} succeeded",
                        &key
                    )));
                }
                Err(ServiceError::DaoError(DaoError::DatabaseConnectionError(
                    conn_info.to_string(),
                )))
            }
        }
    }

    async fn update_conn(&mut self, key: &str, conn_info: ConnInfo) -> Self::Out {
        match self.store.contains_key(key) {
            true => {
                if let Ok(r) = conn_establish(conn_info.clone()).await {
                    self.store.get(key).unwrap().pool.disconnect().await;
                    self.store.insert(key.to_owned(), r);
                    return Ok(DynConnUaOut::Simple(format!(
                        "New conn {:?} succeeded",
                        &key
                    )));
                }
                Err(ServiceError::DaoError(DaoError::DatabaseConnectionError(
                    conn_info.to_string(),
                )))
            }
            false => Err(ServiceError::DaoAlreadyExistError(key.to_owned())),
        }
    }
}
